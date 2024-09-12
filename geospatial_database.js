import { MongoClient } from "mongodb";
import winston from "winston";
import dotenv from "dotenv";

dotenv.config();

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

const url = process.env.MONGODB_URL;
const dbName = process.env.DB_NAME;
const collectionName_one = "circle1";
const collectionName_two = "circle2";
const collectionName_three = "circle3";
const collectionName_four = "box1";
const collectionName_five = "box2";
const collectionName_six = "box3";
const collectionName_seven = "polygon1";
const collectionName_eight = "polygon2";
const collectionName_nine = "polygon3";
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

  const result1 = await populateCollectionCircle(
    client,
    dbName,
    collectionName_one,
    COLLECTION_ONE_SIZE
  );
  const result2 = await populateCollectionCircle(
    client,
    dbName,
    collectionName_two,
    COLLECTION_TWO_SIZE
  );
  const result3 = await populateCollectionCircle(
    client,
    dbName,
    collectionName_three,
    COLLECTION_THREE_SIZE
  );
  const result4 = await populateCollectionBox(
    client,
    dbName,
    collectionName_four,
    COLLECTION_ONE_SIZE
  );
  const result5 = await populateCollectionBox(
    client,
    dbName,
    collectionName_five,
    COLLECTION_TWO_SIZE
  );
  const result6 = await populateCollectionBox(
    client,
    dbName,
    collectionName_six,
    COLLECTION_THREE_SIZE
  );
  const result7 = await populateCollectionPolygon(
    client,
    dbName,
    collectionName_seven,
    COLLECTION_ONE_SIZE
  );
  const result8 = await populateCollectionPolygon(
    client,
    dbName,
    collectionName_eight,
    COLLECTION_TWO_SIZE
  );
  const result9 = await populateCollectionPolygon(
    client,
    dbName,
    collectionName_nine,
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
  logger.info(
    `Response time for ${collectionName_four}: ${result4.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_five}: ${result5.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_six}: ${result6.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_seven}: ${result7.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_eight}: ${result8.toFixed(3)} ms`
  );
  logger.info(
    `Response time for ${collectionName_nine}: ${result9.toFixed(3)} ms`
  );

  const totalResponseTime =
    result1 +
    result2 +
    result3 +
    result4 +
    result5 +
    result6 +
    result7 +
    result8 +
    result9;
  const averageResponseTime = totalResponseTime / 9;

  logger.info(
    `Average response time for all collections: ${averageResponseTime.toFixed(
      3
    )} ms`
  );

  console.timeEnd("Find time: ");
  await client.close();
}

async function populateCollectionCircle(
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

      const document = {
        location: {
          coordinates: [
            parseFloat(randomLatitude),
            parseFloat(randomLongitude),
          ],
          radius: parseFloat(randomRadius),
          type: "Circle",
        },
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

async function populateCollectionBox(
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
      const decimals = 7;

      const randomLatitude = (
        Math.random() * (maxLatitude - minLatitude) +
        minLatitude
      ).toFixed(decimals);
      const randomLongitude = (
        Math.random() * (maxLongitude - minLongitude) +
        minLongitude
      ).toFixed(decimals);

      const coordinates = [
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) + 0.01],
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) + 0.01],
      ];

      const document = {
        location: {
          coordinates: coordinates,
          type: "box",
        },
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

async function populateCollectionPolygon(
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
      const decimals = 7;

      const randomLatitude = (
        Math.random() * (maxLatitude - minLatitude) +
        minLatitude
      ).toFixed(decimals);
      const randomLongitude = (
        Math.random() * (maxLongitude - minLongitude) +
        minLongitude
      ).toFixed(decimals);

      const coordinates = [
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) + 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) + 0.01],
        [parseFloat(randomLatitude) + 0.01, parseFloat(randomLongitude) - 0.01],
        [parseFloat(randomLatitude) - 0.01, parseFloat(randomLongitude) - 0.01],
      ];

      const document = {
        location: {
          coordinates: coordinates,
          type: "polygon",
        },
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
