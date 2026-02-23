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
Flight SQL Example: Query IoTDB via ADBC and convert results to pandas DataFrame.

This example demonstrates how to use the ADBC Flight SQL driver to:
1. Connect to IoTDB's Arrow Flight SQL service
2. Execute SQL queries
3. Convert query results directly to pandas DataFrame

Requirements:
    pip install adbc_driver_flightsql pyarrow pandas

Before running, ensure IoTDB is running with Arrow Flight SQL enabled:
    - Set enable_arrow_flight_sql_service=true in iotdb-system.properties
    - Default Flight SQL port is 8904
"""

import adbc_driver_flightsql.dbapi as flight_sql
import pandas as pd

# IoTDB Flight SQL connection parameters
FLIGHT_SQL_URI = "grpc://localhost:8904"
USERNAME = "root"
PASSWORD = "root"


def get_connection():
    """Create an ADBC Flight SQL connection to IoTDB."""
    return flight_sql.connect(
        uri=FLIGHT_SQL_URI,
        db_kwargs={"username": USERNAME, "password": PASSWORD},
    )


def setup_data(conn):
    """Create database, table, and insert sample data."""
    cursor = conn.cursor()

    # Create database
    cursor.execute("CREATE DATABASE IF NOT EXISTS flight_example_db")

    # Create table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS flight_example_db.sensor_data ("
        "region STRING TAG, "
        "device_id STRING TAG, "
        "temperature FLOAT FIELD, "
        "humidity DOUBLE FIELD, "
        "status BOOLEAN FIELD, "
        "description TEXT FIELD)"
    )

    # Insert sample data
    insert_sqls = [
        "INSERT INTO flight_example_db.sensor_data"
        "(time, region, device_id, temperature, humidity, status, description) "
        "VALUES(1, 'north', 'dev001', 25.5, 60.2, true, 'normal')",
        "INSERT INTO flight_example_db.sensor_data"
        "(time, region, device_id, temperature, humidity, status, description) "
        "VALUES(2, 'north', 'dev001', 26.1, 58.5, true, 'normal')",
        "INSERT INTO flight_example_db.sensor_data"
        "(time, region, device_id, temperature, humidity, status, description) "
        "VALUES(3, 'south', 'dev002', 30.2, 45.0, false, 'high temp')",
        "INSERT INTO flight_example_db.sensor_data"
        "(time, region, device_id, temperature, humidity, status, description) "
        "VALUES(4, 'south', 'dev002', 31.5, 42.3, false, 'high temp')",
        "INSERT INTO flight_example_db.sensor_data"
        "(time, region, device_id, temperature, humidity, status, description) "
        "VALUES(5, 'north', 'dev003', 22.0, 70.1, true, 'cool')",
    ]
    for sql in insert_sqls:
        cursor.execute(sql)

    cursor.close()
    print("Sample data inserted successfully.")


def query_to_dataframe(conn, sql):
    """Execute a SQL query and return results as a pandas DataFrame."""
    cursor = conn.cursor()
    cursor.execute(sql)
    # Fetch result as Arrow Table, then convert to pandas DataFrame
    arrow_table = cursor.fetch_arrow_table()
    df = arrow_table.to_pandas()
    cursor.close()
    return df


def main():
    print("=" * 60)
    print("IoTDB Arrow Flight SQL Example (Python ADBC)")
    print("=" * 60)

    # Connect to IoTDB Flight SQL service
    conn = get_connection()
    print(f"\nConnected to IoTDB Flight SQL at {FLIGHT_SQL_URI}")

    # Setup sample data
    print("\n--- Setting up test data ---")
    setup_data(conn)

    # Example 1: Full table scan → DataFrame
    print("\n" + "=" * 60)
    print("Example 1: Full table scan → DataFrame")
    print("=" * 60)
    df = query_to_dataframe(
        conn,
        "SELECT time, region, device_id, temperature, humidity, status, description "
        "FROM flight_example_db.sensor_data ORDER BY time",
    )
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Column types:\n{df.dtypes}\n")
    print(df.to_string(index=False))

    # Example 2: Filtered query → DataFrame
    print("\n" + "=" * 60)
    print("Example 2: Filtered query (region = 'north') → DataFrame")
    print("=" * 60)
    df_north = query_to_dataframe(
        conn,
        "SELECT time, device_id, temperature, humidity "
        "FROM flight_example_db.sensor_data WHERE region = 'north' ORDER BY time",
    )
    print(f"\nDataFrame shape: {df_north.shape}")
    print(df_north.to_string(index=False))

    # Example 3: Aggregation query → DataFrame
    print("\n" + "=" * 60)
    print("Example 3: Aggregation → DataFrame")
    print("=" * 60)
    df_agg = query_to_dataframe(
        conn,
        "SELECT region, COUNT(*) as cnt, AVG(temperature) as avg_temp, "
        "MAX(humidity) as max_humidity "
        "FROM flight_example_db.sensor_data GROUP BY region ORDER BY region",
    )
    print(f"\nAggregation DataFrame shape: {df_agg.shape}")
    print(df_agg.to_string(index=False))

    # Example 4: Using pandas operations on the DataFrame
    print("\n" + "=" * 60)
    print("Example 4: Pandas operations on Flight SQL results")
    print("=" * 60)
    df_all = query_to_dataframe(
        conn,
        "SELECT time, region, device_id, temperature, humidity "
        "FROM flight_example_db.sensor_data ORDER BY time",
    )
    # Compute statistics
    print(f"\nTemperature statistics:\n{df_all['temperature'].describe()}")
    print(f"\nGroup by region mean:\n{df_all.groupby('region')[['temperature', 'humidity']].mean()}")

    # Cleanup
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS flight_example_db")
    cursor.close()

    conn.close()
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
