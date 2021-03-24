const dotenv = require("dotenv");
dotenv.config();
const { generateKey } = require("../data/dataDefaulter");
const faunadb = require("faunadb"),
    fq = faunadb.query;

DEBUG = true; // set to false to disable debugging
function debug_log() {
    if (DEBUG) {
        console.log.apply(this, arguments);
    }
}

/* 
    General utility methods, used just within db-utils and test file.
*/
async function faunaQuery(query) {
    const client = process.env.DEVELOPMENT
        ? new faunadb.Client({ secret: process.env.FAUNA_DB_DEV })
        : new faunadb.Client({ secret: process.env.FAUNA_DB_PROD });

    try {
        debug_log(`trying faunaQuery`, JSON.stringify(query));
        const res = await client.query(query);
        debug_log(
            `successfully executed query ${JSON.stringify(
                query
            )}, got res ${JSON.stringify(res)}`
        );
        return res;
    } catch (error) {
        debug_log(`for query ${JSON.stringify(query)}, got error ${error}`);
        debug_log(error.description);
        throw error;
    }
}
function hashCode(s) {
    // Taken from: https://gist.github.com/hyamamoto/fd435505d29ebfa3d9716fd2be8d42f0
    var h = 0,
        l = s.length,
        i = 0;
    if (l > 0) while (i < l) h = ((h << 5) - h + s.charCodeAt(i++)) | 0;
    return h;
}

async function generateId() {
    return faunaQuery(fq.NewId());
}

function generateLocationId({ name, street, city, zip }) {
    // This deterministically generates a key from the location info.
    const keyString = generateKey({ name, street, city, zip });
    // FaunaDB only accepts positive ref ids, so we make it positive.
    const hash = Math.abs(hashCode(keyString));
    return hash;
}

function generateLocationIds(locations) {
    // This deterministically generates a key from the location info.
    const keyStrings = locations.map((loc) =>
        generateKey({
            name: loc.name,
            street: loc.street,
            city: loc.city,
            zip: loc.zip,
        })
    );
    // FaunaDB only accepts positive ref ids, so we make it positive.
    return keyStrings.map((ks) => Math.abs(hashCode(ks)));
}

function addGeneratedIdsToLocations(locations) {
    return locations.reduce((acc, curr) => {
        return [
            {
                ...curr,
                // FaunaDB only accepts positive ref ids, so we make it positive.
                refId: Math.abs(
                    // hash it
                    hashCode(
                        // This deterministically generates a key from the location info.
                        generateKey({
                            name: curr.name,
                            street: curr.street,
                            city: curr.city,
                            zip: curr.zip,
                        })
                    )
                ),
            },
            ...acc,
        ];
    }, []);
}

/*
    Basic CRUD operations.
*/
async function retrieveItemByRefId(collectionName, refId) {
    const result = await faunaQuery(
        fq.Get(fq.Ref(fq.Collection(collectionName), refId))
    );
    debug_log(
        `querying ${collectionName} collection with refId ${refId} and got result ${JSON.stringify(
            result
        )}`
    );
    return result;
}

async function retrieveItemsByRefIds(collectionName, refIds) {
    const queries = refIds.map((refId) =>
        fq.Get(fq.Ref(fq.Collection(collectionName), refId))
    );
    const result = await faunaQuery(queries);
    debug_log(
        `querying ${collectionName} collection with refIds ${refIds} and got result ${JSON.stringify(
            result
        )}`
    );
    return result;
}

async function checkItemExistsByRefId(collectionName, refId) {
    const result = await faunaQuery(
        fq.Exists(fq.Ref(fq.Collection(collectionName), refId))
    );
    return result;
}

async function checkItemsExistByRefIds(collectionName, refIds) {
    const queries = refIds.map((refId) =>
        fq.Exists(fq.Ref(fq.Collection(collectionName), refId))
    );
    const result = await faunaQuery(queries);
    return result;
}

async function deleteItemByRefId(collectionName, refId) {
    await faunaQuery(fq.Delete(fq.Ref(fq.Collection(collectionName), refId)));
}

async function deleteItemsByRefIds(collectionName, refIds) {
    const queries = refIds.map((refId) =>
        fq.Delete(fq.Ref(fq.Collection(collectionName), refId))
    );
    await faunaQuery(queries);
}

async function writeLocationByRefId({
    refId,
    name,
    address: { street, city, zip },
    signUpLink,
    latitude,
    longitude,
}) {
    await faunaQuery(
        fq.Create(fq.Ref(fq.Collection("locations"), refId), {
            data: {
                name,
                address: {
                    street,
                    city,
                    zip,
                },
                signUpLink,
                latitude,
                longitude,
            },
        })
    );
}

async function writeLocationsByRefIds(locationsWithRefIds) {
    const queries = locationsWithRefIds.map((loc) =>
        fq.Create(fq.Ref(fq.Collection("locations"), loc.refId), {
            data: {
                name: loc.name,
                address: {
                    street: loc.street,
                    city: loc.city,
                    zip: loc.zip,
                },
                signUpLink: loc.signUpLink,
                latitude: loc.latitude,
                longitude: loc.longitude,
            },
        })
    );
    await faunaQuery(queries);
}

async function writeScraperRunByRefId({ refId, locationRefId, timestamp }) {
    await faunaQuery(
        fq.Create(fq.Ref(fq.Collection("scraperRuns"), refId), {
            data: {
                locationRef: fq.Ref(fq.Collection("locations"), locationRefId),
                timestamp,
            },
        })
    );
}

async function writeAppointmentsByRefId({
    refId,
    scraperRunRefId,
    date,
    numberAvailable,
    signUpLink,
    extraData,
}) {
    await faunaQuery(
        fq.Create(fq.Ref(fq.Collection("appointments"), refId), {
            data: {
                scraperRunRef: fq.Ref(
                    fq.Collection("scraperRuns"),
                    scraperRunRefId
                ),
                date,
                numberAvailable,
                signUpLink,
                extraData,
            },
        })
    );
}

/*
    Index-based queries. This allows us to search our indexes (scraperRunsByLocation, appointmentsByScraperRun).
*/
async function getScaperRunsByLocation(locationRefId) {
    const scraperRuns = await faunaQuery(
        fq.Map(
            fq.Paginate(
                fq.Match(
                    fq.Index("scraperRunsByLocation"),
                    fq.Ref(fq.Collection("locations"), locationRefId)
                )
            ),
            fq.Lambda((x) => fq.Get(x))
        )
    );
    debug_log(
        `for locationRefId ${locationRefId} got response ${JSON.stringify(
            scraperRuns
        )}`
    );
    return scraperRuns;
}

async function getAppointmentsByScraperRun(scraperRunRefId) {
    const appointments = await faunaQuery(
        fq.Map(
            fq.Paginate(
                fq.Match(
                    fq.Index("appointmentsByScraperRun"),
                    fq.Ref(fq.Collection("scraperRuns"), scraperRunRefId)
                )
            ),
            fq.Lambda((x) => fq.Get(x))
        )
    );
    debug_log(
        `for scraperRunRefId ${scraperRunRefId} got response ${JSON.stringify(
            appointments
        )}`
    );
    return appointments;
}

/* 
    Utility that for one scraper output, writes to all the tables (locations, scraperRuns, and appointments).
    We will call this many times from main.js.
*/
async function writeScrapedData({
    name,
    street,
    city,
    zip,
    availability,
    hasAvailability,
    extraData,
    timestamp,
    latitude,
    longitude,
    signUpLink,
}) {
    const locationRefId = generateLocationId({ name, street, city, zip });
    const itemExists = await checkItemExistsByRefId("locations", locationRefId);
    debug_log(`item ${locationRefId} exists: ${itemExists}`);
    if (!itemExists) {
        await writeLocationByRefId({
            refId: locationRefId,
            name,
            address: {
                street,
                city,
                zip,
            },
            signUpLink,
            latitude,
            longitude,
        });
    }

    const scraperRunRefId = await generateId();
    await writeScraperRunByRefId({
        refId: scraperRunRefId,
        locationRefId,
        timestamp,
    });

    if (hasAvailability && availability) {
        Object.entries(availability).map(async ([date, dateAvailability]) => {
            if (
                dateAvailability.hasAvailability &&
                dateAvailability.numberAvailableAppointments > 0
            ) {
                const appointmentsRefId = await generateId();
                await writeAppointmentsByRefId({
                    refId: appointmentsRefId,
                    scraperRunRefId,
                    date,
                    numberAvailable:
                        dateAvailability.numberAvailableAppointments,
                    signUpLink: dateAvailability.signUpLink || signUpLink,
                    extraData,
                });
            }
        });
    }
}

async function writeScrapedDataBatch(locations) {

    // todo - finish this function
    // add locationRefId to the location object!!!!
    // for each location where it doesn't exist, write the item
    const locationRefIds = generateLocationIds(locations);
    // const locationRefId = generateLocationId({ name, street, city, zip });
    // const itemExists = await checkItemExistsByRefId("locations", locationRefId);
    const itemsExistBools = await checkItemsExistByRefIds(
        "locations",
        locations
    );

    debug_log(`item ${itemsExistBools} exists: ${itemsExistBools}`);

    const nonExistentRefIds = locationRefIds.filter((_refId, idx) => {
        return !itemExistsBools[idx];
    });
    const nonExistentLocations = locations.filter((_loc, idx) => {
        return !itemExistsBools[idx];
    });
    if (nonExistentRefIds.length) {
        await writeLocationsByRefIds(nonExistentRefIds, nonExistentLocations);
    }
    // for indexes where exists = false, write them
    // await Promise.all(
    //     itemsExistBools.map((itemExists, idx) => {
    //         if(!itemExists) {
    //             await write
    //         }
    //     })
    // )

    // if (!itemExists) {
    //     await writeLocationByRefId({
    //         refId: locationRefId,
    //         name,
    //         address: {
    //             street,
    //             city,
    //             zip,
    //         },
    //         signUpLink,
    //         latitude,
    //         longitude,
    //     });
    // }

    const scraperRunRefId = await generateId();
    await writeScraperRunByRefId({
        refId: scraperRunRefId,
        locationRefId,
        timestamp,
    });

    if (hasAvailability && availability) {
        Object.entries(availability).map(async ([date, dateAvailability]) => {
            if (
                dateAvailability.hasAvailability &&
                dateAvailability.numberAvailableAppointments > 0
            ) {
                const appointmentsRefId = await generateId();
                await writeAppointmentsByRefId({
                    refId: appointmentsRefId,
                    scraperRunRefId,
                    date,
                    numberAvailable:
                        dateAvailability.numberAvailableAppointments,
                    signUpLink: dateAvailability.signUpLink || signUpLink,
                    extraData,
                });
            }
        });
    }
}

/* 
    Utility that will return all availability for each location's most recent scraper run.
    This will go into a lambda.
*/
async function getAppointmentsForAllLocations() {}

module.exports = {
    addGeneratedIdsToLocations,
    checkItemExistsByRefId,
    checkItemsExistByRefIds,
    getAppointmentsForAllLocations,
    getAppointmentsByScraperRun,
    deleteItemByRefId,
    deleteItemsByRefIds,
    generateLocationId,
    generateLocationIds,
    getScaperRunsByLocation,
    retrieveItemByRefId,
    retrieveItemsByRefIds,
    writeAppointmentsByRefId,
    writeLocationByRefId,
    writeLocationsByRefIds,
    writeScrapedData,
    writeScraperRunByRefId,
};
