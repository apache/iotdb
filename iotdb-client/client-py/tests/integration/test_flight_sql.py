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
Integration tests for Arrow Flight SQL service in IoTDB.

Requirements:
    pip install adbc_driver_flightsql pyarrow pandas
"""

import pytest

try:
    import adbc_driver_flightsql.dbapi as flight_sql
    import pyarrow

    HAS_FLIGHT_SQL = True
except ImportError:
    HAS_FLIGHT_SQL = False

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import ColumnType, Tablet
from .iotdb_container import IoTDBContainer

# Default Arrow Flight SQL port
FLIGHT_SQL_PORT = 8904


@pytest.fixture(scope="module")
def iotdb_flight_sql():
    """Start an IoTDB container with Flight SQL port exposed and prepare test data."""
    with IoTDBContainer("iotdb:dev") as db:
        db.with_exposed_ports(FLIGHT_SQL_PORT)

        # Insert test data via standard session
        session = Session(
            db.get_container_host_ip(),
            db.get_exposed_port(6667),
            "root",
            "root",
        )
        session.open(False)

        # Create database and table
        session.execute_non_query_statement("CREATE DATABASE IF NOT EXISTS flight_test_db")
        session.execute_statement(
            "CREATE TABLE IF NOT EXISTS flight_test_db.test_table ("
            "device_id STRING TAG, "
            "temperature FLOAT FIELD, "
            "humidity DOUBLE FIELD, "
            "status BOOLEAN FIELD, "
            "info TEXT FIELD)"
        )

        # Insert sample data
        column_names = ["device_id", "temperature", "humidity", "status", "info"]
        data_types = [
            TSDataType.STRING,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.BOOLEAN,
            TSDataType.STRING,
        ]
        column_types = [
            ColumnType.TAG,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
            ColumnType.FIELD,
        ]
        timestamps = list(range(1, 6))
        values = [
            ["dev001", 25.5, 60.2, True, "normal"],
            ["dev001", 26.1, 58.5, True, "normal"],
            ["dev002", 30.2, 45.0, False, "high temp"],
            ["dev002", 31.5, 42.3, False, "high temp"],
            ["dev001", 24.8, 62.1, True, "normal"],
        ]
        tablet = Tablet(
            "test_table", column_names, data_types, values, timestamps, column_types
        )
        session.insert_relational_tablet(tablet)
        session.execute_non_query_statement("FLUSH")
        session.close()

        yield db


@pytest.mark.skipif(not HAS_FLIGHT_SQL, reason="adbc_driver_flightsql not installed")
def test_flight_sql_query(iotdb_flight_sql):
    """Test querying IoTDB via Arrow Flight SQL and verify Arrow Table result."""
    db = iotdb_flight_sql
    host = db.get_container_host_ip()
    port = db.get_exposed_port(FLIGHT_SQL_PORT)

    uri = f"grpc://{host}:{port}"
    conn = flight_sql.connect(
        uri=uri,
        db_kwargs={"username": "root", "password": "root"},
    )
    cursor = conn.cursor()

    cursor.execute(
        "SELECT time, device_id, temperature, humidity, status, info "
        "FROM flight_test_db.test_table ORDER BY time"
    )
    table = cursor.fetch_arrow_table()

    # Verify we got all 5 rows
    assert table.num_rows == 5, f"Expected 5 rows, got {table.num_rows}"
    # Verify columns
    assert table.num_columns == 6, f"Expected 6 columns, got {table.num_columns}"

    cursor.close()
    conn.close()


@pytest.mark.skipif(not HAS_FLIGHT_SQL, reason="adbc_driver_flightsql not installed")
def test_flight_sql_to_dataframe(iotdb_flight_sql):
    """Test converting Flight SQL results directly to pandas DataFrame."""
    db = iotdb_flight_sql
    host = db.get_container_host_ip()
    port = db.get_exposed_port(FLIGHT_SQL_PORT)

    uri = f"grpc://{host}:{port}"
    conn = flight_sql.connect(
        uri=uri,
        db_kwargs={"username": "root", "password": "root"},
    )
    cursor = conn.cursor()

    cursor.execute(
        "SELECT time, device_id, temperature, humidity "
        "FROM flight_test_db.test_table ORDER BY time"
    )
    table = cursor.fetch_arrow_table()
    df = table.to_pandas()

    # Verify DataFrame shape
    assert df.shape == (5, 4), f"Expected shape (5, 4), got {df.shape}"

    # Verify column names
    expected_cols = {"time", "device_id", "temperature", "humidity"}
    assert set(df.columns) == expected_cols, f"Unexpected columns: {df.columns.tolist()}"

    cursor.close()
    conn.close()


@pytest.mark.skipif(not HAS_FLIGHT_SQL, reason="adbc_driver_flightsql not installed")
def test_flight_sql_aggregation(iotdb_flight_sql):
    """Test aggregation query via Flight SQL."""
    db = iotdb_flight_sql
    host = db.get_container_host_ip()
    port = db.get_exposed_port(FLIGHT_SQL_PORT)

    uri = f"grpc://{host}:{port}"
    conn = flight_sql.connect(
        uri=uri,
        db_kwargs={"username": "root", "password": "root"},
    )
    cursor = conn.cursor()

    cursor.execute(
        "SELECT device_id, COUNT(*) as cnt, AVG(temperature) as avg_temp "
        "FROM flight_test_db.test_table GROUP BY device_id ORDER BY device_id"
    )
    table = cursor.fetch_arrow_table()
    df = table.to_pandas()

    # Should have 2 groups (dev001 and dev002)
    assert df.shape[0] == 2, f"Expected 2 groups, got {df.shape[0]}"

    cursor.close()
    conn.close()


@pytest.mark.skipif(not HAS_FLIGHT_SQL, reason="adbc_driver_flightsql not installed")
def test_flight_sql_empty_result(iotdb_flight_sql):
    """Test query that returns empty result via Flight SQL."""
    db = iotdb_flight_sql
    host = db.get_container_host_ip()
    port = db.get_exposed_port(FLIGHT_SQL_PORT)

    uri = f"grpc://{host}:{port}"
    conn = flight_sql.connect(
        uri=uri,
        db_kwargs={"username": "root", "password": "root"},
    )
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM flight_test_db.test_table WHERE device_id = 'nonexistent'"
    )
    table = cursor.fetch_arrow_table()

    assert table.num_rows == 0, f"Expected 0 rows, got {table.num_rows}"

    cursor.close()
    conn.close()
