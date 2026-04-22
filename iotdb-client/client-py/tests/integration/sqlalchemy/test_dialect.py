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
from sqlalchemy.dialects import registry
from sqlalchemy.sql import text
from tests.integration.iotdb_container import IoTDBContainer
from urllib.parse import quote_plus as urlquote


TEST_DB = "test_sqlalchemy"


def test_table_model_dialect():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        url = f"iotdb://root:{password}@{host}:{port}/{TEST_DB}"
        registry.register("iotdb", "iotdb.sqlalchemy.IoTDBDialect", "IoTDBDialect")
        engine = create_engine(url)

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE %s" % TEST_DB))

        _test_ddl(engine)
        _test_dml(engine)
        _test_reflection(engine)
        _test_time_column(engine)

        with engine.connect() as conn:
            conn.execute(text("DROP DATABASE %s" % TEST_DB))

        engine.dispose()
        print("All table model dialect tests passed!")


def _test_ddl(engine):
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
        schema=TEST_DB,
        iotdb_ttl=3600000,
    )

    metadata.create_all(engine)

    insp = inspect(engine)
    table_names = insp.get_table_names(schema=TEST_DB)
    assert "sensors" in table_names, (
        "CREATE TABLE failed: 'sensors' not in %s" % table_names
    )

    sensors.drop(engine)
    table_names = insp.get_table_names(schema=TEST_DB)
    assert "sensors" not in table_names, (
        "DROP TABLE failed: 'sensors' still in %s" % table_names
    )

    print("  DDL tests passed")


def _test_dml(engine):
    metadata = MetaData()
    sensors = Table(
        "sensors_dml",
        metadata,
        Column("region", String, iotdb_category="TAG"),
        Column("device_id", String, iotdb_category="TAG"),
        Column("temperature", Float, iotdb_category="FIELD"),
        Column("humidity", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            sensors.insert().values(
                region="asia",
                device_id="d001",
                temperature=25.5,
                humidity=60.0,
            )
        )
        conn.execute(
            sensors.insert().values(
                region="europe",
                device_id="d002",
                temperature=18.3,
                humidity=75.0,
            )
        )

        result = conn.execute(sensors.select()).fetchall()
        assert len(result) == 2, "INSERT/SELECT failed: expected 2 rows, got %d" % len(
            result
        )

        result = conn.execute(
            sensors.select().where(sensors.c.region == "asia")
        ).fetchall()
        assert len(result) == 1, (
            "SELECT WHERE failed: expected 1 row, got %d" % len(result)
        )

        result = conn.execute(
            sensors.select().order_by(sensors.c.temperature).limit(1)
        ).fetchall()
        assert len(result) == 1, "LIMIT failed: expected 1 row, got %d" % len(result)

        conn.execute(
            sensors.delete().where(sensors.c.device_id == "d002")
        )
        result = conn.execute(sensors.select()).fetchall()
        assert len(result) == 1, (
            "DELETE failed: expected 1 row after delete, got %d" % len(result)
        )

    sensors.drop(engine)
    print("  DML tests passed")


def _test_reflection(engine):
    with engine.connect() as conn:
        conn.execute(text("USE %s" % TEST_DB))
        conn.execute(
            text(
                "CREATE TABLE reflect_test ("
                "region STRING TAG, "
                "device STRING TAG, "
                "model STRING ATTRIBUTE, "
                "temperature FLOAT FIELD, "
                "humidity DOUBLE FIELD"
                ")"
            )
        )

    insp = inspect(engine)

    schemas = insp.get_schema_names()
    assert TEST_DB in schemas, "%s not in schemas: %s" % (TEST_DB, schemas)

    tables = insp.get_table_names(schema=TEST_DB)
    assert "reflect_test" in tables, "reflect_test not in tables: %s" % tables

    columns = insp.get_columns(table_name="reflect_test", schema=TEST_DB)
    col_names = [c["name"] for c in columns]
    assert "region" in col_names, "region not in columns: %s" % col_names
    assert "temperature" in col_names, "temperature not in columns: %s" % col_names

    pk = insp.get_pk_constraint(table_name="reflect_test", schema=TEST_DB)
    assert pk["constrained_columns"] == [], "Expected empty PK constraint"

    fks = insp.get_foreign_keys(table_name="reflect_test", schema=TEST_DB)
    assert fks == [], "Expected empty FK list"

    indexes = insp.get_indexes(table_name="reflect_test", schema=TEST_DB)
    assert indexes == [], "Expected empty index list"

    with engine.connect() as conn:
        conn.execute(text("USE %s" % TEST_DB))
        conn.execute(text("DROP TABLE reflect_test"))

    print("  Reflection tests passed")


def _test_time_column(engine):
    metadata = MetaData()

    table_implicit = Table(
        "time_implicit",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            table_implicit.insert().values(device="d001", value=42.0)
        )
        result = conn.execute(table_implicit.select()).fetchall()
        assert len(result) == 1, (
            "Implicit TIME insert failed: expected 1 row, got %d" % len(result)
        )

    table_implicit.drop(engine)

    metadata2 = MetaData()
    table_explicit = Table(
        "time_explicit",
        metadata2,
        Column("ts", BigInteger, iotdb_category="TIME"),
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata2.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            table_explicit.insert().values(ts=1000000, device="d001", value=42.0)
        )
        result = conn.execute(table_explicit.select()).fetchall()
        assert len(result) == 1, (
            "Explicit TIME insert failed: expected 1 row, got %d" % len(result)
        )

    table_explicit.drop(engine)
    print("  TIME column tests passed")


if __name__ == "__main__":
    test_table_model_dialect()
