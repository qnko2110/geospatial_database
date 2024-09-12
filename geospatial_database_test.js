import { MongoClient } from "mongodb";
import dotenv from "dotenv";
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

dotenv.config();

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

 
  const collection4 = client.db(dbName).collection(collectionName_four);
  await collection4.createIndex({ location: "2dsphere" });
  

 

  const query2 = collection4
    .find({
      location: {
        $geometry: {
          $box: [
            [coordinates[0], coordinates[1]],
            [coordinates[0], coordinates[1]],
            [coordinates[0], coordinates[1]],
            [coordinates[0], coordinates[1]],
          ],
        },
      },
    })
    .toArray();
  console.log(query2);

  
}

await startQuery();
