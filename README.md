# Finonex Home Assignment: ETL Process with Node.js

This is the README file for the Home Assignment Project that involves building an ETL (Extract, Transform, Load) process using Node.js. The project consists of three main services: **Client**, **Server**, and **Data Processor**. Each service has its specific responsibilities in the ETL pipeline.

## Project Overview

The main objective of this project is to create an ETL process that handles live events generated by the **Client** service. These events are sent to the **Server** service, which receives them via a REST endpoint and appends them to a local file for further processing. The **Data Processor** service is responsible for reading the file, identifying new records, and writing them to a PostgreSQL database.

## Services

### Client

The **Client** service is responsible for generating live events and sending them to the **Server** service. It serves as the data source for the ETL pipeline.

**Utilities**

- In order to generate events, run the `npm run generate_events` command. Make sure you have `curl` installed. Alternatively, you can send a `get` request to `localhost:3000/generateEvents` endpoint.
- This command will extact events from the local `events.jsonl` file, will validate them, and will send them to the **Server** service.

### Server

The **Server** service acts as an intermediate layer in the ETL process. Its primary role is to receive live events from the **Client** service via a RESTful API. After receiving an event, server adds a `timestamp` value to it and appends it to a local file, which acts as a staging area for data processing.

**Utilities**

- In order to generate a table to save the results of ETL processing, run the `npm run create_table` command. Make sure you have `curl` installed. Alternatively, you can send a `get` request to `localhost:8000/createTable` endpoint.
- This command will create a `users_revenue` table in a locally installed `PostgreSQL` database.

### Data Processor

The **Data Processor** service is the final step in the ETL pipeline. Its responsibility is to read the data from the local file, identify new records that have not been processed before, and then insert or update these new records in a PostgreSQL database. The data processing should be efficient and handle large datasets.

**Process Overview**

- First, get the checkpoint value stored in the local file. A `Checkpoint` is a timestamp value of the last processed event in the events file. This is espetially useful in the incremental data load flow and allows separating between the new events and already processed ones.
- Open the events file, iterate over the events and separate the new events from the already processed ones by comparing their timestamp with the checkpoint from the previous step.
- Take the new events, group them by user id and calculate user revenue for each user.
- For these users, load their current balance from DB, then calculate the final revenue by adding/substracting with the previous calculations.
- After all the calculations are done, write the final result to the database. For each user, the action can be either `insert` or `update`, depending on if the user is new or not. Use transaction to insert everything in bulk.
- Update the checkpoint value and write it back to the file.

## Getting Started

To run the project, follow the steps below:

1. Clone the repository.
2. Install Node.js and PostgreSQL on your system if not already installed.
3. Install project dependencies using `npm install`.
4. Configure the PostgreSQL database credentials in the project's `.env` configuration file.
5. Run the project using `npm start`. This will start client and server services together.
