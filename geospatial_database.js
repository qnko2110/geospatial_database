const MongoClient = require("mongodb").MongoClient;

const url = "mongodb://127.0.0.1:27017";
const dbName = "geospatial_database";
const collectionName_one = "circle1";
const collectionName_two = "circle2";
const collectionName_three = "circle3";
const options = {
  serverSelectionTimeoutMS: 3000,
  connectTimeoutMS: 3000,
  socketTimeoutMS: 3000,
  useUnifiedTopology: true,
};
const COLLECTION_ONE_SIZE = 10_000;
const COLLECTION_TWO_SIZE = 50_000;
const COLLECTION_THREE_SIZE = 100_000;

async function startQuery() {
  let client;
  console.time("Connect time ");
  try {
    client = await MongoClient.connect(url, options);
  } catch (error) {
    console.log(`Timeout:\n${error.message}`);
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
  console.log(result1);
  console.log(result2);
  console.log(result3);
}
async function populateCollection(
  client,
  dbName,
  collectionName,
  collectionSize
) {
  const collection = client.db(dbName).collection(collectionName);
  for (let i = 0; i < collectionSize; i++) {
    const minLatitude = -90;
    const maxLatitude = 90;
    const minLongitude = -180;
    const maxLongitude = 180;
    const minRadius = 0;
    const maxRadius = 100;
    const decimals = 7;
    const radiusDecimals = 2;
    
    const randomLatitude = (Math.random() * (maxLatitude - minLatitude) + minLatitude).toFixed(decimals);
    const randomLongitude = (Math.random() * (maxLongitude - minLongitude) + minLongitude).toFixed(decimals);
    const randomRadius = (Math.random() * (maxRadius - minRadius) + minRadius).toFixed(radiusDecimals);
    const document = {
      coordinates: [randomLatitude, randomLongitude],
      radius: randomRadius,
    };
    await collection.insertOne(document);
  }
  return collection;
}
startQuery();
