import { wasmConnector, coordinator, MosaicClient, Selection, clauseInterval, Param } from "@uwdata/mosaic-core";
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
  constructor(tableName, columnName, filterBy, onUpdate) {
    super(filterBy);
    this.tableName = tableName;
    this.columnName = columnName;
    this.onUpdate = onUpdate;
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

  update() {
    this.onUpdate(this.data)
    return this;
  }
}

const selection = Selection.crossfilter();
const client1 = new CustomClient("testData", "HourOfDay", selection, (data) => console.log(data) /* (data) => comp1.data(data) */ );
const client2 = new CustomClient("testData", "DayOfWeek", selection, (data) => console.log(data) /* (data) => comp2.data(data) */ );
await coord.connect(client1);
await coord.connect(client2);

const c1 = Param.value(0);
const c2 = Param.value(24);
const c3 = Param.value(0);
const c4 = Param.value(7);

selection.update(clauseInterval("HourOfDay", [c1, c2], { source: client1 }));
selection.update(clauseInterval("DayOfWeek", [c3, c4], { source: client2 }));

c1.update(2);
c2.update(3);
c3.update(0);
c4.update(1);
