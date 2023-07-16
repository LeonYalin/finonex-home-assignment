import express from "express";
import bodyParser from "body-parser";
import fs from "fs";
import readline from "readline";
import { pipeline } from "stream";
import axios from "axios";
require("dotenv").config();

const EVENT_SERVER_URL = "http://localhost:8000";
const EVENTS_FILE_PATH = "events.jsonl";

const app = express();
app.use(bodyParser.json());

app.listen(3000, () => {
  console.log("client is running on port 3000");
});

app.get("/generateEvents", (req, res) => {
  console.log("Starting to generate events...");
  generateEvents(EVENTS_FILE_PATH, postEvent);
  res.status(200).send("OK");
});

async function generateEvents(
  filePath: string,
  callback: (event: any) => void
) {
  try {
    const fileStream = fs.createReadStream(filePath, "utf8");
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    pipeline(
      rl,
      async (source) => {
        for await (const line of source) {
          try {
            const event = JSON.parse(line);
            const isValid = validateEvent(event);
            if (!isValid) {
              console.error("Invalid event, skipping:", event);
              continue;
            }
            callback(event);
          } catch (error: any) {
            throw new Error("Error parsing JSON: " + error.message);
          }
        }
      },
      async function (err) {
        if (err) {
          console.error("Pipeline error:", err);
        } else {
          console.log("Pipeline completed");
        }
      }
    );
  } catch (error: any) {
    throw new Error("Error reading file: " + error.message);
  }
}

async function postEvent(event: any) {
  const secret = "secret";
  try {
    const response = await axios.post(`${EVENT_SERVER_URL}/liveEvent`, event, {
      headers: { Authorization: `Basic ${secret}` },
    });
    console.log("postEvent response:", response.data);
  } catch (error) {
    console.error("Error posting event:", error);
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
