import { MongoClient } from "mongodb";
import winston from "winston";

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "app.log" }),
  ],
});

const url = "mongodb://127.0.0.1:27017";
const dbName = "geospatial_database";
const collectionName_one = "circle1";
const collectionName_two = "circle2";
const collectionName_three = "circle3";
const options = {
  serverSelectionTimeoutMS: 3000,
  connectTimeoutMS: 3000,
  socketTimeoutMS: 3000,
};
const COLLECTION_ONE_SIZE = 10_000;
const COLLECTION_TWO_SIZE = 50_000;
const COLLECTION_THREE_SIZE = 100_000;

async function startQuery() {
  let client;
  logger.info("Starting connection to MongoDB");
  console.time("Connect time ");
  try {
    client = await MongoClient.connect(url, options);
  } catch (error) {
    logger.error(`Timeout:\n${error.message}`);
    process.exit();
  } finally {
    console.timeEnd("Connect time ");
  }
  console.time("Find time: ");

  const result1 = await populateCollection(
    client,
    dbName,
    collectionName_one,
    COLLECTION_ONE_SIZE
  );
  const result2 = await populateCollection(
    client,
    dbName,
    collectionName_two,
    COLLECTION_TWO_SIZE
  );
  const result3 = await populateCollection(
    client,
    dbName,
    collectionName_three,
    COLLECTION_THREE_SIZE
  );

  logger.info(
    `Response time for ${collectionName_one}: ${result1.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_two}: ${result2.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_three}: ${result3.toFixed(3)} ms`
  );

  const totalResponseTime = result1 + result2 + result3;
  const averageResponseTime = totalResponseTime / 3;

  logger.info(
    `Average response time for all collections: ${averageResponseTime.toFixed(
      3
    )} ms`
  );

  console.timeEnd("Find time: ");
  await client.close();
}

async function populateCollection(
  client,
  dbName,
  collectionName,
  collectionSize
) {
  const collection = client.db(dbName).collection(collectionName);
  let totalResponseTime = 0;
  const batchSize = 1000; // Adjust batch size as needed

  for (let i = 0; i < collectionSize; i += batchSize) {
    const batch = [];
    for (let j = 0; j < batchSize && i + j < collectionSize; j++) {
      const minLatitude = -90;
      const maxLatitude = 90;
      const minLongitude = -180;
      const maxLongitude = 180;
      const minRadius = 0;
      const maxRadius = 100;
      const decimals = 7;
      const radiusDecimals = 2;

      const randomLatitude = (
        Math.random() * (maxLatitude - minLatitude) +
        minLatitude
      ).toFixed(decimals);
      const randomLongitude = (
        Math.random() * (maxLongitude - minLongitude) +
        minLongitude
      ).toFixed(decimals);
      const randomRadius = (
        Math.random() * (maxRadius - minRadius) +
        minRadius
      ).toFixed(radiusDecimals);

      const box = [
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) + 0.01],
      ];

      const polygon = [
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) + 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) + 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
      ];

      const document = {
        coordinates: [parseFloat(randomLatitude), parseFloat(randomLongitude)],
        radius: parseFloat(randomRadius),
        box: box,
        polygon: polygon,
      };

      batch.push(document);
    }

    const startTime = process.hrtime();
    await collection.insertMany(batch);
    const endTime = process.hrtime(startTime);

    const responseTime = endTime[0] * 1000 + endTime[1] / 1e6;
    totalResponseTime += responseTime;

    logger.info(
      `Inserted batch ${
        i / batchSize + 1
      }, response time: ${responseTime.toFixed(3)} ms`
    );
  }

  const averageResponseTime = totalResponseTime / (collectionSize / batchSize);
  return averageResponseTime;
}

await startQuery();