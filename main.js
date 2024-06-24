import { wasmConnector, coordinator, MosaicClient, Selection, interval } from "@uwdata/mosaic-core";
import { Query, count } from "@uwdata/mosaic-sql";
import testData from "./data.js";

const connector = wasmConnector();
const db = await connector.getDuckDB();
const con = await connector.getConnection();

await db.registerFileText("testData.json", JSON.stringify(testData));
await con.insertJSONFromPath("testData.json", { name: "testData" });

await connector.query({
  sql: `
    BEGIN TRANSACTION;

    -- explicitly load the ICU extension since autoloading is not working properly
    LOAD icu;

    -- returns timestamp with time zone type based on user's time zone preference
    ALTER TABLE testData ADD COLUMN TripCreatedTZ TIMESTAMPTZ;
    UPDATE testData SET TripCreatedTZ = CAST(TripCreated AS TIMESTAMP) AT TIME ZONE 'UTC';

    -- returns integers in range [0, 23]
    ALTER TABLE testData ADD COLUMN HourOfDay INTEGER;
    UPDATE testData SET HourOfDay = EXTRACT(HOUR FROM CAST(TripCreatedTZ AS TIMESTAMP));

    -- returns integers in range [0, 6] where 0 = monday
    ALTER TABLE testData ADD COLUMN DayOfWeek INTEGER;
    UPDATE testData SET DayOfWeek = (EXTRACT(WEEKDAY FROM CAST(TripCreatedTZ AS TIMESTAMP)) + 6) % 7;

    -- returns DATE of monday in UTC
    ALTER TABLE testData ADD COLUMN WeekStartMonday DATE;
    UPDATE testData SET WeekStartMonday = DATE_TRUNC('week', CAST(TripCreatedTZ AS TIMESTAMP));

    COMMIT TRANSACTION;
  `,
  type: "exec",
});

const coord = coordinator();
coord.databaseConnector(connector);

class CustomClient extends MosaicClient {
  constructor(tableName, columnName, filterBy) {
    super(filterBy);
    this.tableName = tableName;
    this.columnName = columnName;
  }

  query(filter = []) {
    const { tableName, columnName } = this;
    return Query.from(tableName)
      .select({ key: columnName, value: count() })
      .where(filter)
      .groupby(columnName);
  }

  queryResult(data) {
    this.data = data.toArray().map((row) => row.toJSON());
    return this;
  }
}

const selection = Selection.crossfilter();
const client1 = new CustomClient("testData", "HourOfDay", selection);
const client2 = new CustomClient("testData", "DayOfWeek", selection);
coord.connect(client1);
coord.connect(client2);

console.log(selection.predicate(client1)); // => []
console.log(selection.predicate(client2)); // => []

// it seems like below is being attached to class itself not an instance of class
selection.update(interval("HourOfDay", [0, 24], { source: client1 }));
console.log(selection.predicate(client1) + ""); // => ("HourOfDay" BETWEEN 0 AND 24)
console.log(selection.predicate(client2) + ""); // => ("HourOfDay" BETWEEN 0 AND 24)

selection.update(interval("DayOfWeek", [0, 7], { source: client2 }));
console.log(selection.predicate(client1) + ""); // => ("HourOfDay" BETWEEN 0 AND 24)
console.log(selection.predicate(client2) + ""); // => ("HourOfDay" BETWEEN 0 AND 24)
