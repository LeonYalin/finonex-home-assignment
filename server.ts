import express from "express";
import bodyParser from "body-parser";

const app = express();
app.use(bodyParser.json());

import { Pool } from "pg";
const pool = new Pool({
  host: "localhost",
  port: 5432,
  database: "leonyalin",
  user: "leonyalin",
  password: "",
});

pool.query("SELECT NOW()", (err, res) => {
  if (err) {
    console.error("Error executing query:", err);
  } else {
    console.log("Query result:", res.rows);
  }
});

app.post("/liveEvent", (req, res) => {
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

  console.log("Received event:", req.body);
  res.status(200).send("OK");
});

app.get("/userEvents/:userId", (req, res) => {
  const userId = req.params.userId;
  if (!userId) {
    console.error("Missing userId parameter");
    res.status(400).send("Bad Request");
    return;
  }

  console.log("Received request for user:", userId);
});

app.listen(8000, () => {
  console.log("server is running on port 8000");
});

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
