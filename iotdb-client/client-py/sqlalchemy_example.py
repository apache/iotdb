# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
IoTDB SQLAlchemy Dialect Example

This example demonstrates how to use the IoTDB SQLAlchemy dialect with IoTDB's
table model (IoTDB 2.0+). It covers:
  1. Creating an engine and connecting to IoTDB
  2. DDL: Creating tables with IoTDB-specific column categories and TTL
  3. DML: Inserting, querying, and deleting data
  4. Schema reflection via inspect()
  5. Raw SQL execution via text()

Prerequisites:
  - IoTDB 2.0+ server running on localhost:6667
  - pip install apache-iotdb sqlalchemy
"""

from sqlalchemy import (
    create_engine,
    inspect,
    Column,
    Float,
    Integer,
    BigInteger,
    String,
    Boolean,
    Table,
    MetaData,
)
from sqlalchemy.sql import text

DATABASE = "sqlalchemy_example_db"

# ---------------------------------------------------------------
# 1. Create an Engine
# ---------------------------------------------------------------
# Connection URL format: iotdb://username:password@host:port/database
# The database part is optional; you can also set it later with USE.
engine = create_engine("iotdb://root:root@127.0.0.1:6667")
print("Engine created successfully.")

# ---------------------------------------------------------------
# 2. Create a Database
# ---------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % DATABASE))
    print("Database '%s' created." % DATABASE)

# ---------------------------------------------------------------
# 3. DDL — Define and Create a Table
# ---------------------------------------------------------------
# IoTDB table model columns have categories:
#   TIME      — the timestamp column (auto-generated if not specified)
#   TAG       — identifier/indexing columns (e.g., region, device id)
#   ATTRIBUTE — descriptive columns (e.g., model, firmware version)
#   FIELD     — measurement/metric columns (e.g., temperature, humidity)
#
# Use iotdb_category on each Column and iotdb_ttl on the Table.
metadata = MetaData()

sensors = Table(
    "sensors",
    metadata,
    Column("region", String, iotdb_category="TAG"),
    Column("device_id", String, iotdb_category="TAG"),
    Column("model", String, iotdb_category="ATTRIBUTE"),
    Column("temperature", Float, iotdb_category="FIELD"),
    Column("humidity", Float, iotdb_category="FIELD"),
    Column("status", Boolean, iotdb_category="FIELD"),
    schema=DATABASE,
    iotdb_ttl=86400000,  # TTL in milliseconds (1 day)
)

metadata.create_all(engine)
print("Table 'sensors' created with TTL=86400000ms.")

# ---------------------------------------------------------------
# 4. DML — Insert Data
# ---------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(
        sensors.insert().values(
            region="asia",
            device_id="d001",
            model="ModelA",
            temperature=25.5,
            humidity=60.0,
            status=True,
        )
    )
    conn.execute(
        sensors.insert().values(
            region="asia",
            device_id="d002",
            model="ModelB",
            temperature=27.3,
            humidity=55.0,
            status=True,
        )
    )
    conn.execute(
        sensors.insert().values(
            region="europe",
            device_id="d003",
            model="ModelA",
            temperature=18.1,
            humidity=75.0,
            status=False,
        )
    )
    print("Inserted 3 rows into 'sensors'.")

# ---------------------------------------------------------------
# 5. DML — Query Data
# ---------------------------------------------------------------
with engine.connect() as conn:
    # SELECT all rows
    print("\n--- All rows ---")
    result = conn.execute(sensors.select())
    for row in result:
        print(row)

    # SELECT with WHERE
    print("\n--- Rows where region='asia' ---")
    result = conn.execute(sensors.select().where(sensors.c.region == "asia"))
    for row in result:
        print(row)

    # SELECT with ORDER BY and LIMIT
    print("\n--- Top 2 by temperature (ascending) ---")
    result = conn.execute(sensors.select().order_by(sensors.c.temperature).limit(2))
    for row in result:
        print(row)

# ---------------------------------------------------------------
# 6. DML — Delete Data
# ---------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(sensors.delete().where(sensors.c.device_id == "d003"))
    print("\nDeleted rows where device_id='d003'.")

    remaining = conn.execute(sensors.select()).fetchall()
    print("Remaining rows: %d" % len(remaining))

# ---------------------------------------------------------------
# 7. Schema Reflection via inspect()
# ---------------------------------------------------------------
insp = inspect(engine)

schemas = insp.get_schema_names()
print("\n--- Databases (schemas) ---")
print(schemas)

tables = insp.get_table_names(schema=DATABASE)
print("\n--- Tables in '%s' ---" % DATABASE)
print(tables)

columns = insp.get_columns(table_name="sensors", schema=DATABASE)
print("\n--- Columns of 'sensors' ---")
for col in columns:
    print(
        "  %s: type=%s, category=%s"
        % (
            col["name"],
            col["type"],
            col.get("iotdb_category", "N/A"),
        )
    )

# ---------------------------------------------------------------
# 8. Raw SQL via text()
# ---------------------------------------------------------------
with engine.connect() as conn:
    print("\n--- Raw SQL query ---")
    result = conn.execute(text("SELECT * FROM %s.sensors" % DATABASE))
    for row in result:
        print(row)

# ---------------------------------------------------------------
# 9. Explicit TIME Column
# ---------------------------------------------------------------
# By default, IoTDB auto-generates the TIME column. You can also
# define it explicitly to control the timestamp values.
metadata2 = MetaData()

events = Table(
    "events",
    metadata2,
    Column("ts", BigInteger, iotdb_category="TIME"),
    Column("device_id", String, iotdb_category="TAG"),
    Column("event_type", String, iotdb_category="ATTRIBUTE"),
    Column("value", Float, iotdb_category="FIELD"),
    schema=DATABASE,
)

metadata2.create_all(engine)
print("\nTable 'events' created with explicit TIME column.")

with engine.connect() as conn:
    conn.execute(
        events.insert().values(
            ts=1000000, device_id="d001", event_type="alert", value=99.9
        )
    )
    conn.execute(
        events.insert().values(
            ts=2000000, device_id="d002", event_type="info", value=42.0
        )
    )

    print("\n--- Events ---")
    result = conn.execute(events.select())
    for row in result:
        print(row)

# ---------------------------------------------------------------
# 10. Cleanup
# ---------------------------------------------------------------
sensors.drop(engine)
events.drop(engine)
with engine.connect() as conn:
    conn.execute(text("DROP DATABASE IF EXISTS %s" % DATABASE))
print("\nCleanup done. Database '%s' dropped." % DATABASE)

engine.dispose()
print("Example finished.")
