import express from "express";
import bodyParser from "body-parser";
import fs from "fs";
import readline from "readline";
import axios from "axios";

const EVENT_SERVER_URL = "http://localhost:8000";

const app = express();
app.use(bodyParser.json());

app.listen(3000, () => {
  console.log("client is running on port 3000");
});

processFile("events.jsonl");

async function* lineGenerator(filePath: string) {
  const fileStream = fs
    .createReadStream(filePath, "utf8")
    .on("error", (error) => {
      console.error("Error reading file:", error);
    });

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    yield line;
  }
}

async function postEvent(event: any) {
  const secret = "secret";
  try {
    const response = await axios.post(`${EVENT_SERVER_URL}/liveEvent`, event, {
      headers: { Authorization: `Basic ${secret}` },
    });
    console.log("Response:", response.data);
  } catch (error) {
    console.error("Error posting event:", error);
  }
}

async function processFile(filePath: string) {
  const generator = lineGenerator(filePath);

  for await (const line of generator) {
    try {
      const event = JSON.parse(line);
      const isValid = validateEvent(event);
      if (!isValid) {
        console.error("Invalid event, skipping:", event);
        continue;
      }
      console.log("Event:", event);
      postEvent(event);
    } catch (error) {
      console.error("Error parsing JSON:", error);
    }
  }

  console.log("File reading completed.");
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
