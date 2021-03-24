const chai = require("chai");
chai.use(require("chai-as-promised"));
chai.use(require("chai-shallow-deep-equal"));
const expect = chai.expect;
const lodash = require("lodash");

describe("FaunaDB Utils", function () {
    const dbUtils = require("../lib/db-utils");
    it("can create, retrieve, and delete docs from Locations collection (once doc at a time)", async () => {
        const randomName = Math.random().toString(36).substring(7);
        const collectionName = "locations";
        const location = {
            name: `RandomName-${randomName}`,
            address: { street: "1 Main St", city: "Newton", zip: "02458" },
            signUpLink: "www.google.com",
        };
        const generatedId = dbUtils.generateLocationId({
            name: location.name,
            steet: location.address.street,
            city: location.address.city,
            zip: location.address.zip,
        });

        await expect(
            dbUtils.retrieveItemByRefId(collectionName, generatedId)
        ).to.eventually.be.rejectedWith("instance not found");

        await expect(
            dbUtils.checkItemExistsByRefId(collectionName, generatedId)
        ).to.eventually.be.false;

        await dbUtils.writeLocationByRefId({
            refId: generatedId,
            ...location,
        });

        await expect(
            dbUtils.checkItemExistsByRefId(collectionName, generatedId)
        ).to.eventually.be.true;

        const retrieveResult = await dbUtils.retrieveItemByRefId(
            collectionName,
            generatedId
        );
        expect(retrieveResult).to.be.shallowDeepEqual({
            ref: {
                value: {
                    collection: {
                        value: {
                            collection: {
                                value: {
                                    id: "collections",
                                },
                            },
                            id: "locations",
                        },
                    },
                    id: generatedId,
                },
            },
            data: location,
        });

        await dbUtils.deleteItemByRefId(collectionName, generatedId);
        await expect(
            dbUtils.retrieveItemByRefId(collectionName, generatedId)
        ).to.eventually.be.rejectedWith("instance not found");
    }).timeout(3000);

    it(
        "can create, retrieve, and delete docs from Locations collection (in batches)",
        async () => {
            const collectionName = "locations";
            const locations = [
                {
                    name: `RandomName-${Math.random()
                        .toString(36)
                        .substring(7)}`,
                    street: "1 Main St",
                    city: "Newton",
                    zip: "02458",
                    signUpLink: "www.google.com",
                },
                {
                    name: `RandomName-${Math.random()
                        .toString(36)
                        .substring(7)}`,
                    street: "2 Main St",
                    city: "Newton",
                    zip: "02458",
                    signUpLink: "www.google.com",
                },
            ];
            // so that we can keep refids tied to locations, add them
            const locationsWithRefIds = dbUtils.addGeneratedIdsToLocations(
                locations
            );
            const generatedIds = locationsWithRefIds.map((loc) => loc.refId);

            await expect(
                dbUtils.retrieveItemsByRefIds(collectionName, generatedIds)
            ).to.eventually.be.rejectedWith("instance not found");

            await expect(
                dbUtils.checkItemsExistByRefIds(collectionName, generatedIds)
            ).to.eventually.deep.equal([false, false]);

            await dbUtils.writeLocationsByRefIds(locationsWithRefIds);

            await expect(
                dbUtils.checkItemsExistByRefIds(collectionName, generatedIds)
            ).to.eventually.deep.equal([true, true]);

            const retrieveResult = await dbUtils.retrieveItemsByRefIds(
                collectionName,
                generatedIds
            );
            const filteredResults = retrieveResult.map(
                (entry) => lodash.omit(entry, ["ts", "ref"]) // remove the timestamp and reference, too complicated to check against
            );
            expect(filteredResults).to.have.deep.members([
                {
                    data: {
                        name: locations[0].name,
                        address: {
                            street: locations[0].street,
                            city: locations[0].city,
                            zip: locations[0].zip,
                        },
                        signUpLink: locations[0].signUpLink,
                    },
                },
                {
                    data: {
                        name: locations[1].name,
                        address: {
                            street: locations[1].street,
                            city: locations[1].city,
                            zip: locations[1].zip,
                        },
                        signUpLink: locations[1].signUpLink,
                    },
                },
            ]);

            await dbUtils.deleteItemsByRefIds(collectionName, generatedIds);
            await expect(
                dbUtils.checkItemsExistByRefIds(collectionName, generatedIds)
            ).to.eventually.deep.equal([false, false]);
        }
    ).timeout(3000);

    it("given one scraper's output, can create, retrieve, and delete docs from Locations, ScraperRuns, and Appointments collections", async () => {
        const randomName = Math.random().toString(36).substring(7);
        const scraperOutput = {
            name: `RandomName-${randomName}`,
            street: "2240 Iyannough Road",
            city: "West Barnstable",
            zip: "02668",
            availability: {
                "03/16/2021": {
                    hasAvailability: true,
                    numberAvailableAppointments: 2,
                    signUpLink: "fake-signup-link-2",
                },
                "03/17/2021": {
                    hasAvailability: true,
                    numberAvailableAppointments: 1,
                    signUpLink: null,
                },
            },
            hasAvailability: true,
            extraData: {
                "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
                "Age groups served": "Adults",
                "Services offered": "Vaccination",
                "Additional Information": "Pfizer vaccine",
                "Clinic Hours": "10:00 am - 03:00 pm",
            },
            timestamp: "2021-03-16T13:15:27.318Z",
            latitude: 41.6909399,
            longitude: -70.3373802,
            signUpLink: "fake-signup-link",
        };

        // Write the appopriate location (if it's not already there), scaperRun, and appointment(s)
        await dbUtils.writeScrapedData(scraperOutput);

        // sleep while the DB writing happens...
        await new Promise((r) => setTimeout(r, 1000));

        const locationId = dbUtils.generateLocationId({
            name: scraperOutput.name,
            street: scraperOutput.street,
            city: scraperOutput.city,
            zip: scraperOutput.zip,
        });
        const retrieveLocationResult = await dbUtils.retrieveItemByRefId(
            "locations",
            locationId
        );
        expect(retrieveLocationResult).to.be.shallowDeepEqual({
            ref: {
                value: {
                    collection: {
                        value: {
                            collection: {
                                value: {
                                    id: "collections",
                                },
                            },
                            id: "locations",
                        },
                    },
                    id: locationId,
                },
            },
            data: {
                name: scraperOutput.name,
                address: {
                    street: scraperOutput.street,
                    city: scraperOutput.city,
                    zip: scraperOutput.zip,
                },
                latitude: scraperOutput.latitude,
                longitude: scraperOutput.longitude,
                signUpLink: scraperOutput.signUpLink,
            },
        });
        // assert that the scraper run is there (check index)
        const retrieveScraperRunResult = await dbUtils.getScaperRunsByLocation(
            locationId
        );
        expect(retrieveScraperRunResult).to.be.shallowDeepEqual({
            data: [
                {
                    ref: {
                        value: {
                            collection: {
                                value: {
                                    id: "scraperRuns",
                                    collection: {
                                        value: { id: "collections" },
                                    },
                                },
                            },
                        },
                    },
                    data: {
                        locationRef: {
                            value: {
                                id: locationId,
                                collection: {
                                    value: {
                                        id: "locations",
                                        collection: {
                                            value: { id: "collections" },
                                        },
                                    },
                                },
                            },
                        },
                        timestamp: "2021-03-16T13:15:27.318Z",
                    },
                },
            ],
        });

        const scraperRunRef = retrieveScraperRunResult.data[0].ref.value.id;

        // assert that the appointmentAvailability is there
        const retrieveAppointmentsResult = await dbUtils.getAppointmentsByScraperRun(
            scraperRunRef
        );

        const filteredResults = retrieveAppointmentsResult.data.map(
            (entry) => lodash.omit(entry.data, ["scraperRunRef"]) // this was too complicated to check against.
        );

        expect(filteredResults).to.have.deep.members([
            {
                date: "03/16/2021",
                numberAvailable: 2,
                signUpLink: "fake-signup-link-2",
                extraData: {
                    "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
                    "Age groups served": "Adults",
                    "Services offered": "Vaccination",
                    "Additional Information": "Pfizer vaccine",
                    "Clinic Hours": "10:00 am - 03:00 pm",
                },
            },
            {
                date: "03/17/2021",
                numberAvailable: 1,
                signUpLink: "fake-signup-link",
                extraData: {
                    "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
                    "Age groups served": "Adults",
                    "Services offered": "Vaccination",
                    "Additional Information": "Pfizer vaccine",
                    "Clinic Hours": "10:00 am - 03:00 pm",
                },
            },
        ]);
        const appointmentRefIds = retrieveAppointmentsResult.data.map(
            (entry) => entry.ref.value.id
        );

        // clean up - delete it all
        await dbUtils.deleteItemByRefId("locations", locationId);
        await dbUtils.deleteItemByRefId("scraperRuns", scraperRunRef);
        appointmentRefIds.map(async (id) => {
            await dbUtils.deleteItemByRefId("appointments", id);
        });
    }).timeout(4000);

    it("given a batch of scrapers outputs, can create, retrieve, and delete docs from Locations, ScraperRuns, and Appointments collections", async () => {
        // todo - finish this test

        //     const scraperOutputs = [
        //         {
        //             name: `RandomName-${Math.random().toString(36).substring(7)}`,
        //             street: "2240 Iyannough Road",
        //             city: "West Barnstable",
        //             zip: "02668",
        //             availability: {
        //                 "03/16/2021": {
        //                     hasAvailability: true,
        //                     numberAvailableAppointments: 2,
        //                     signUpLink: "fake-signup-link-2",
        //                 },
        //                 "03/17/2021": {
        //                     hasAvailability: true,
        //                     numberAvailableAppointments: 1,
        //                     signUpLink: null,
        //                 },
        //             },
        //             hasAvailability: true,
        //             extraData: {
        //                 "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
        //                 "Age groups served": "Adults",
        //                 "Services offered": "Vaccination",
        //                 "Additional Information": "Pfizer vaccine",
        //                 "Clinic Hours": "10:00 am - 03:00 pm",
        //             },
        //             timestamp: "2021-03-16T13:15:27.318Z",
        //             latitude: 41.6909399,
        //             longitude: -70.3373802,
        //             signUpLink: "fake-signup-link",
        //         },
        //         {
        //             name: `RandomName-${Math.random().toString(36).substring(7)}`,
        //             street: "409 W Broadway",
        //             city: "South Boston",
        //             zip: "02127",
        //             availability: {
        //                 "03/30/2021": {
        //                     hasAvailability: true,
        //                     numberAvailableAppointments: 30,
        //                     signUpLink: "fake-signup-link-3",
        //                 },
        //             },
        //             hasAvailability: true,
        //             timestamp: "2021-03-16T16:16:16.318Z",
        //             latitude: 100,
        //             longitude: -100,
        //             signUpLink: "fake-signup-link-3",
        //         },
        //     ];
        //     // Write the appopriate location (if it's not already there), scaperRun, and appointment(s)
        //     // await dbUtils.writeScrapedData(scraperOutput);
        //     // // sleep while the DB writing happens...
        //     // await new Promise((r) => setTimeout(r, 1000));
        //     // const locationId = dbUtils.generateLocationId({
        //     //     name: scraperOutput.name,
        //     //     street: scraperOutput.street,
        //     //     city: scraperOutput.city,
        //     //     zip: scraperOutput.zip,
        //     // });
        //     // const retrieveLocationResult = await dbUtils.retrieveItemByRefId(
        //     //     "locations",
        //     //     locationId
        //     // );
        //     // expect(retrieveLocationResult).to.be.shallowDeepEqual({
        //     //     ref: {
        //     //         value: {
        //     //             collection: {
        //     //                 value: {
        //     //                     collection: {
        //     //                         value: {
        //     //                             id: "collections",
        //     //                         },
        //     //                     },
        //     //                     id: "locations",
        //     //                 },
        //     //             },
        //     //             id: locationId,
        //     //         },
        //     //     },
        //     //     data: {
        //     //         name: scraperOutput.name,
        //     //         address: {
        //     //             street: scraperOutput.street,
        //     //             city: scraperOutput.city,
        //     //             zip: scraperOutput.zip,
        //     //         },
        //     //         latitude: scraperOutput.latitude,
        //     //         longitude: scraperOutput.longitude,
        //     //         signUpLink: scraperOutput.signUpLink,
        //     //     },
        //     // });
        //     // // assert that the scraper run is there (check index)
        //     // const retrieveScraperRunResult = await dbUtils.getScaperRunsByLocation(
        //     //     locationId
        //     // );
        //     // expect(retrieveScraperRunResult).to.be.shallowDeepEqual({
        //     //     data: [
        //     //         {
        //     //             ref: {
        //     //                 value: {
        //     //                     collection: {
        //     //                         value: {
        //     //                             id: "scraperRuns",
        //     //                             collection: {
        //     //                                 value: { id: "collections" },
        //     //                             },
        //     //                         },
        //     //                     },
        //     //                 },
        //     //             },
        //     //             data: {
        //     //                 locationRef: {
        //     //                     value: {
        //     //                         id: locationId,
        //     //                         collection: {
        //     //                             value: {
        //     //                                 id: "locations",
        //     //                                 collection: {
        //     //                                     value: { id: "collections" },
        //     //                                 },
        //     //                             },
        //     //                         },
        //     //                     },
        //     //                 },
        //     //                 timestamp: "2021-03-16T13:15:27.318Z",
        //     //             },
        //     //         },
        //     //     ],
        //     // });
        //     // const scraperRunRef = retrieveScraperRunResult.data[0].ref.value.id;
        //     // // assert that the appointmentAvailability is there
        //     // const retrieveAppointmentsResult = await dbUtils.getAppointmentsByScraperRun(
        //     //     scraperRunRef
        //     // );
        //     // const filteredResults = retrieveAppointmentsResult.data.map(
        //     //     (entry) => lodash.omit(entry.data, ["scraperRunRef"]) // this was too complicated to check against.
        //     // );
        //     // expect(filteredResults).to.have.deep.members([
        //     //     {
        //     //         date: "03/16/2021",
        //     //         numberAvailable: 2,
        //     //         signUpLink: "fake-signup-link-2",
        //     //         extraData: {
        //     //             "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
        //     //             "Age groups served": "Adults",
        //     //             "Services offered": "Vaccination",
        //     //             "Additional Information": "Pfizer vaccine",
        //     //             "Clinic Hours": "10:00 am - 03:00 pm",
        //     //         },
        //     //     },
        //     //     {
        //     //         date: "03/17/2021",
        //     //         numberAvailable: 1,
        //     //         signUpLink: "fake-signup-link",
        //     //         extraData: {
        //     //             "Vaccinations offered": "Pfizer-BioNTech COVID-19 Vaccine",
        //     //             "Age groups served": "Adults",
        //     //             "Services offered": "Vaccination",
        //     //             "Additional Information": "Pfizer vaccine",
        //     //             "Clinic Hours": "10:00 am - 03:00 pm",
        //     //         },
        //     //     },
        //     // ]);
        //     // const appointmentRefIds = retrieveAppointmentsResult.data.map(
        //     //     (entry) => entry.ref.value.id
        //     // );
        //     // // clean up - delete it all
        //     // await dbUtils.deleteItemByRefId("locations", locationId);
        //     // await dbUtils.deleteItemByRefId("scraperRuns", scraperRunRef);
        //     // appointmentRefIds.map(async (id) => {
        //     //     await dbUtils.deleteItemByRefId("appointments", id);
        //     // });
    }).timeout(4000);

    it("can get the availability for all locations' most recent scraper runs", async () => {
        await dbUtils.getAppointmentsForAllLocations();
        // the logic isn't here yet.
    });
});
