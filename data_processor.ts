import fs from "fs";
import readline from "readline";
import { pipeline } from "stream";
import { Pool } from "pg";
import pgplib from "pg-promise";

const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || "",
  user: process.env.DB_USER || "",
  password: process.env.DB_PASSWORD || "",
};

const pool = new Pool(dbConfig);
const pgp = pgplib();
const db = pgp(dbConfig);

const TABLE_NAME = "users_revenue";
const SERVER_EVENTS_FILE_PATH =
  process.env.SERVER_EVENTS_FILE_PATH || "server_events.jsonl";
const CHECKPOINT_FILE_PATH = "checkpoint.json";

async function processData() {
  // first, get last_updated timestamp from the checkpoint.json file, to serve as the starting point for processing events
  const checkpoint = await getCheckpointValueFromFile();

  // then, read events from file, and process each event that has a timestamp greater than last_updated
  const newEvents = await extractEventsFromFile(
    SERVER_EVENTS_FILE_PATH,
    checkpoint
  );

  // then, group events by user_id, and calculate revenue for each user
  const usersRevenueFromEvents = calculateUsersRevenueFromEvents(newEvents);

  // then, get current revenue values from users_revenue table
  const usersRevenueFromDB = await getUsersRevenueFromDB(
    usersRevenueFromEvents
  );

  // then, merge the users revenue from events with the revenue from the database
  const mergedUsersRevenue = mergeUsersRevenue(
    usersRevenueFromEvents,
    usersRevenueFromDB
  );

  // then, insert/update users_revenue table with the new revenue values
  await writeUpdatedUsersRevenueToDB(mergedUsersRevenue);

  // finally, update checkpoint timestamp in checkpoint.json file
  await updateCheckpointValue(Date.now());
}
processData();

async function extractEventsFromFile(
  filePath: string,
  checkpoint: number
): Promise<any[]> {
  const events: any[] = [];
  return new Promise((resolve, reject) => {
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
              if (validateEvent(event) && event.timestamp > checkpoint) {
                delete event.timestamp;
                events.push(event);
              }
            } catch (error: any) {
              throw new Error("Error parsing JSON: " + error.message);
            }
          }
        },
        async function (err) {
          if (err) {
            throw new Error(
              "Error extracting events from file: " + err.message
            );
            reject(err);
          } else {
            resolve(events);
          }
        }
      );
    } catch (error: any) {
      throw new Error("Error reading file: " + error.message);
    }
  });
}

function calculateUsersRevenueFromEvents(events: any[]) {
  const usersRevenue: any = {};
  events.forEach((event) => {
    const { userId, name, value } = event;
    if (!usersRevenue.hasOwnProperty(userId)) {
      usersRevenue[userId] = 0;
    }
    if (name === "add_revenue") {
      usersRevenue[userId] += value;
    } else if (name === "subtract_revenue") {
      usersRevenue[userId] -= value;
    }
  });
  return usersRevenue;
}

async function getUsersRevenueFromDB(
  usersRevenueFromEvents: Record<string, number>
) {
  const userIdsArr = Object.keys(usersRevenueFromEvents);
  try {
    let query = `SELECT user_id, revenue FROM ${TABLE_NAME}`;
    if (userIdsArr.length > 0) {
      query += ` WHERE user_id IN (${userIdsArr
        .map((id) => `'${id}'`)
        .join(",")})`;
      const res = await pool.query(query);
      return res.rows;
    } else {
      return Promise.resolve([]);
    }
  } catch (error: any) {
    throw new Error("Error retrieving users revenue: " + error.message);
  }
}
/**
 * Here, we do 2 things:
 * 1. Calculate the final revenue for each user
 * 2. Create two groups of users with their revenues: one for insert (new records), and one for update (existing records)
 */
function mergeUsersRevenue(
  usersRevenueFromEvents: Record<string, number>,
  usersRevenueFromDB: any[]
) {
  const usersRevenueFromDBMap = usersRevenueFromDB.reduce(
    (map, { user_id, revenue }) => {
      map[user_id] = revenue;
      return map;
    },
    {} as Record<string, number>
  );

  const mergedUsersRevenueUpdate: Record<string, number> = {
    ...usersRevenueFromDBMap,
  };
  const mergedUsersRevenueInsert: Record<string, number> = {};

  Object.entries(usersRevenueFromEvents).forEach(([userId, revenue]) => {
    if (mergedUsersRevenueUpdate.hasOwnProperty(userId)) {
      mergedUsersRevenueUpdate[userId] += revenue;
    } else {
      mergedUsersRevenueInsert[userId] = revenue;
    }
  });
  return { update: mergedUsersRevenueUpdate, insert: mergedUsersRevenueInsert };
}

async function writeUpdatedUsersRevenueToDB(mergedUsersRevenue: any) {
  const { update, insert } = mergedUsersRevenue;

  try {
    await db.tx(async (t) => {
      const tableName = "users_revenue";

      const insertQueries: any[] = [];
      Object.entries(insert).forEach(([user_id, revenue]) => {
        insertQueries.push(
          t.none("INSERT INTO $1:name (user_id, revenue) VALUES ($2, $3)", [
            tableName,
            user_id,
            revenue,
          ])
        );
      });

      const updateQueries: any[] = [];
      Object.entries(update).forEach(([user_id, revenue]) => {
        updateQueries.push(
          t.none("UPDATE $1:name SET revenue = $3 WHERE user_id = $2", [
            tableName,
            user_id,
            revenue,
          ])
        );
      });

      await t.batch([...insertQueries, ...updateQueries]);
    });

    console.log("Bulk insert and update successful.");
  } catch (error: any) {
    throw new Error("Error performing bulk operations: " + error.message);
  } finally {
    pgp.end();
  }
}

async function updateCheckpointValue(newCheckpoint: number) {
  try {
    const content = { checkpoint: newCheckpoint };

    await fs.promises.writeFile(
      CHECKPOINT_FILE_PATH,
      JSON.stringify(content),
      "utf8"
    );
  } catch (error: any) {
    throw new Error("Error writing checkpoint file: " + error.message);
  }
}

async function getCheckpointValueFromFile() {
  try {
    // Check if the file exists
    await fs.promises.access(CHECKPOINT_FILE_PATH, fs.constants.F_OK);

    // File exists, read its contents
    const data = await fs.promises.readFile(CHECKPOINT_FILE_PATH, "utf8");
    const jsonObject = JSON.parse(data);
    return jsonObject.checkpoint as number;
  } catch (error: any) {
    if (error.code === "ENOENT") {
      const initialContent = { checkpoint: 0 };
      await fs.promises.writeFile(
        CHECKPOINT_FILE_PATH,
        JSON.stringify(initialContent),
        "utf8"
      );
      return initialContent.checkpoint as number;
    } else {
      throw new Error("Error getting checkpoint file: " + error.message);
    }
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
  if (
    !event.hasOwnProperty("timestamp") ||
    typeof event.timestamp !== "number"
  ) {
    valid = false;
  }

  return valid;
}
