import express from "express";
import bodyParser from "body-parser";
import { Pool } from "pg";
import fs from "fs";
import { pipeline } from "stream";
require("dotenv").config();

import util from "util";
import { Readable } from "stream";
const readFileAsync = util.promisify(fs.readFile);

const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || "",
  user: process.env.DB_USER || "",
  password: process.env.DB_PASSWORD || "",
};

const SERVER_EVENTS_FILE_PATH =
  process.env.SERVER_EVENTS_FILE_PATH || "server_events.jsonl";

const app = express();
app.use(bodyParser.json());

const pool = new Pool(dbConfig);

app.post("/liveEvent", (req, res, next) => {
  const secret = "secret";
  const auth = req.headers.authorization;

  if (!auth || auth !== `Basic ${secret}`) {
    console.error("Invalid authorization header:", auth);
    res.status(401).send("Unauthorized");
    return;
  }

  if (!req.body || !validateEvent(req.body)) {
    console.error("Invalid event:", req.body);
    res.status(400).send("Bad Request");
    return;
  }

  // Here, we're adding a timestamp to the event data before writing it to the file.
  // This is so that we can use the timestamp to find the unprocessed events in the file later.
  const jsonData = `${JSON.stringify({
    ...req.body,
    timestamp: Date.now(),
  })}\n`;

  const writableStream = fs.createWriteStream(SERVER_EVENTS_FILE_PATH, {
    flags: "a",
  });

  pipeline(Readable.from([jsonData]), writableStream, (err: any) => {
    if (err) {
      next(err);
    } else {
      console.log("Data appended to file successfully.");
    }
  });

  res.status(200).send("OK");
});

app.get("/userEvents/:userId", async (req, res) => {
  const userId = req.params.userId;
  if (!userId || typeof userId !== "string") {
    console.error("Invalid userId parameter");
    res.status(400).send("Bad Request");
    return;
  }

  try {
    const data = await pool.query(
      "SELECT * FROM users_revenue WHERE user_id = $1",
      [userId]
    );

    res.status(200).send(data.rows);
  } catch (error: any) {
    console.error("Error getting user events:", error.message);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/createTable", (req, res) => {
  console.log("Starting to create table...");
  createUsersRevenueTable();
  res.status(200).send("OK");
});

app.listen(8000, () => {
  console.log("server is running on port 8000");
});

async function createUsersRevenueTable() {
  try {
    const sql = await readFileAsync("db.sql", "utf8");
    await pool.query(sql);
    console.log("Table created successfully.");
  } catch (error: any) {
    throw new Error("Error creating table: " + error.message);
  }
}

function validateEvent(event: any) {
  let valid = true;
  const nameValueOptions = ["add_revenue", "subtract_revenue"];

  if (!event.hasOwnProperty("userId") || typeof event.userId !== "string") {
    valid = false;
  }
  if (!event.hasOwnProperty("name") || !nameValueOptions.includes(event.name)) {
    valid = false;
  }
  if (!event.hasOwnProperty("value") || typeof event.value !== "number") {
    valid = false;
  }

  return valid;
}
