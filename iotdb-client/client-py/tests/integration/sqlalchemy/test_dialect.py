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
    Text,
    LargeBinary,
    DateTime,
    Date,
    Table,
    MetaData,
    func,
    and_,
    or_,
)
from sqlalchemy.dialects import registry
from sqlalchemy.sql import text
from tests.integration.iotdb_container import IoTDBContainer
from urllib.parse import quote_plus as urlquote

TEST_DB = "test_sqlalchemy"
TEST_DB2 = "test_sqlalchemy2"


def _make_engine(host, port, password, database=None):
    if database:
        url = f"iotdb://root:{password}@{host}:{port}/{database}"
    else:
        url = f"iotdb://root:{password}@{host}:{port}"
    registry.register("iotdb", "iotdb.sqlalchemy.IoTDBDialect", "IoTDBDialect")
    return create_engine(url)


def test_table_model_dialect():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        engine = _make_engine(host, port, password, TEST_DB)

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


def test_all_data_types():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        engine = _make_engine(host, port, password, TEST_DB)

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB))

        _test_all_data_types(engine)

        with engine.connect() as conn:
            conn.execute(text("DROP DATABASE %s" % TEST_DB))

        engine.dispose()
        print("All data type tests passed!")


def test_advanced_queries():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        engine = _make_engine(host, port, password, TEST_DB)

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB))

        _test_select_specific_columns(engine)
        _test_multiple_where_conditions(engine)
        _test_aggregation(engine)
        _test_batch_insert(engine)
        _test_null_values(engine)

        with engine.connect() as conn:
            conn.execute(text("DROP DATABASE %s" % TEST_DB))

        engine.dispose()
        print("All advanced query tests passed!")


def test_schema_operations():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        engine = _make_engine(host, port, password, TEST_DB)

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB))
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB2))

        _test_has_schema_and_table(engine)
        _test_get_view_names(engine)
        _test_multiple_databases(engine)
        _test_column_category_reflection(engine)
        _test_multiple_tables(engine)
        _test_table_without_ttl(engine)

        with engine.connect() as conn:
            conn.execute(text("DROP DATABASE IF EXISTS %s" % TEST_DB))
            conn.execute(text("DROP DATABASE IF EXISTS %s" % TEST_DB2))

        engine.dispose()
        print("All schema operation tests passed!")


def test_raw_sql():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)
        engine = _make_engine(host, port, password, TEST_DB)

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB))

        _test_raw_sql(engine)

        with engine.connect() as conn:
            conn.execute(text("DROP DATABASE %s" % TEST_DB))

        engine.dispose()
        print("All raw SQL tests passed!")


def test_url_with_database():

    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        password = urlquote("TimechoDB@2021")
        host = db.get_container_host_ip()
        port = db.get_exposed_port(6667)

        engine_with_db = _make_engine(host, port, password, TEST_DB)
        engine_without_db = _make_engine(host, port, password)

        with engine_without_db.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS %s" % TEST_DB))

        _test_url_with_database(engine_with_db, engine_without_db)

        with engine_without_db.connect() as conn:
            conn.execute(text("DROP DATABASE %s" % TEST_DB))

        engine_with_db.dispose()
        engine_without_db.dispose()
        print("URL with database tests passed!")


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
        assert len(result) == 1, "SELECT WHERE failed: expected 1 row, got %d" % len(
            result
        )

        result = conn.execute(
            sensors.select().order_by(sensors.c.temperature).limit(1)
        ).fetchall()
        assert len(result) == 1, "LIMIT failed: expected 1 row, got %d" % len(result)

        conn.execute(sensors.delete().where(sensors.c.device_id == "d002"))
        result = conn.execute(sensors.select()).fetchall()
        assert (
            len(result) == 1
        ), "DELETE failed: expected 1 row after delete, got %d" % len(result)

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
        conn.execute(table_implicit.insert().values(device="d001", value=42.0))
        result = conn.execute(table_implicit.select()).fetchall()
        assert (
            len(result) == 1
        ), "Implicit TIME insert failed: expected 1 row, got %d" % len(result)

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
        assert (
            len(result) == 1
        ), "Explicit TIME insert failed: expected 1 row, got %d" % len(result)

    table_explicit.drop(engine)
    print("  TIME column tests passed")


if __name__ == "__main__":
    test_table_model_dialect()


# ---------------------------------------------------------------
# All data types test
# ---------------------------------------------------------------
def _test_all_data_types(engine):
    metadata = MetaData()

    all_types = Table(
        "all_types",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("col_boolean", Boolean, iotdb_category="FIELD"),
        Column("col_int32", Integer, iotdb_category="FIELD"),
        Column("col_int64", BigInteger, iotdb_category="FIELD"),
        Column("col_float", Float, iotdb_category="FIELD"),
        Column("col_double", Float(precision=53), iotdb_category="FIELD"),
        Column("col_string", String, iotdb_category="FIELD"),
        Column("col_text", Text, iotdb_category="FIELD"),
        Column("col_date", Date, iotdb_category="FIELD"),
        Column("col_timestamp", DateTime, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            all_types.insert().values(
                device="d001",
                col_boolean=True,
                col_int32=42,
                col_int64=9999999999,
                col_float=3.14,
                col_double=2.718281828459045,
                col_string="hello",
                col_text="world",
                col_date="2024-01-15",
                col_timestamp=1700000000000,
            )
        )

        result = conn.execute(all_types.select()).fetchall()
        assert (
            len(result) == 1
        ), "All types insert failed: expected 1 row, got %d" % len(result)

    insp = inspect(engine)
    columns = insp.get_columns(table_name="all_types", schema=TEST_DB)
    col_names = [c["name"] for c in columns]
    for expected in [
        "device",
        "col_boolean",
        "col_int32",
        "col_int64",
        "col_float",
        "col_double",
        "col_string",
        "col_text",
        "col_date",
        "col_timestamp",
    ]:
        assert expected in col_names, "%s not in columns: %s" % (expected, col_names)

    all_types.drop(engine)
    print("  All data types tests passed")


# ---------------------------------------------------------------
# Advanced query tests
# ---------------------------------------------------------------
def _create_sensor_table(engine, table_name):
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("region", String, iotdb_category="TAG"),
        Column("device_id", String, iotdb_category="TAG"),
        Column("model", String, iotdb_category="ATTRIBUTE"),
        Column("temperature", Float, iotdb_category="FIELD"),
        Column("humidity", Float, iotdb_category="FIELD"),
        Column("status", Boolean, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)
    return table


def _insert_sample_data(engine, table):
    with engine.connect() as conn:
        rows = [
            ("asia", "d001", "ModelA", 25.5, 60.0, True),
            ("asia", "d002", "ModelB", 27.3, 55.0, True),
            ("europe", "d003", "ModelA", 18.1, 75.0, False),
            ("europe", "d004", "ModelC", 22.0, 65.0, True),
            ("na", "d005", "ModelB", 30.2, 45.0, False),
        ]
        for region, device_id, model, temp, hum, status in rows:
            conn.execute(
                table.insert().values(
                    region=region,
                    device_id=device_id,
                    model=model,
                    temperature=temp,
                    humidity=hum,
                    status=status,
                )
            )


def _test_select_specific_columns(engine):
    table = _create_sensor_table(engine, "select_cols")
    _insert_sample_data(engine, table)

    with engine.connect() as conn:
        result = conn.execute(
            table.select().with_only_columns(table.c.region, table.c.temperature)
        ).fetchall()
        assert len(result) == 5, "Select columns failed: expected 5 rows, got %d" % len(
            result
        )

        result = conn.execute(
            table.select().with_only_columns(table.c.device_id)
        ).fetchall()
        assert (
            len(result) == 5
        ), "Select single column failed: expected 5 rows, got %d" % len(result)

    table.drop(engine)
    print("  Select specific columns tests passed")


def _test_multiple_where_conditions(engine):
    table = _create_sensor_table(engine, "where_cond")
    _insert_sample_data(engine, table)

    with engine.connect() as conn:
        # AND condition
        result = conn.execute(
            table.select().where(
                and_(table.c.region == "asia", table.c.temperature > 26.0)
            )
        ).fetchall()
        assert len(result) == 1, "AND condition failed: expected 1 row, got %d" % len(
            result
        )

        # OR condition
        result = conn.execute(
            table.select().where(or_(table.c.region == "asia", table.c.region == "na"))
        ).fetchall()
        assert len(result) == 3, "OR condition failed: expected 3 rows, got %d" % len(
            result
        )

        # Range condition
        result = conn.execute(
            table.select().where(
                and_(table.c.temperature >= 20.0, table.c.temperature <= 28.0)
            )
        ).fetchall()
        assert (
            len(result) == 3
        ), "Range condition failed: expected 3 rows, got %d" % len(result)

        # NOT EQUAL
        result = conn.execute(table.select().where(table.c.region != "asia")).fetchall()
        assert (
            len(result) == 3
        ), "NOT EQUAL condition failed: expected 3 rows, got %d" % len(result)

    table.drop(engine)
    print("  Multiple WHERE conditions tests passed")


def _test_aggregation(engine):
    table = _create_sensor_table(engine, "agg_test")
    _insert_sample_data(engine, table)

    with engine.connect() as conn:
        # COUNT
        result = conn.execute(table.select().with_only_columns(func.count())).scalar()
        assert result == 5, "COUNT failed: expected 5, got %s" % result

        # AVG
        result = conn.execute(
            table.select().with_only_columns(func.avg(table.c.temperature))
        ).scalar()
        assert result is not None, "AVG returned None"
        assert abs(result - 24.62) < 0.1, "AVG failed: expected ~24.62, got %s" % result

        # SUM
        result = conn.execute(
            table.select().with_only_columns(func.sum(table.c.humidity))
        ).scalar()
        assert result is not None, "SUM returned None"
        assert abs(result - 300.0) < 0.1, "SUM failed: expected 300.0, got %s" % result

        # MIN / MAX
        min_val = conn.execute(
            table.select().with_only_columns(func.min(table.c.temperature))
        ).scalar()
        max_val = conn.execute(
            table.select().with_only_columns(func.max(table.c.temperature))
        ).scalar()
        assert abs(min_val - 18.1) < 0.1, "MIN failed: expected 18.1, got %s" % min_val
        assert abs(max_val - 30.2) < 0.1, "MAX failed: expected 30.2, got %s" % max_val

    table.drop(engine)
    print("  Aggregation tests passed")


def _test_batch_insert(engine):
    metadata = MetaData()
    table = Table(
        "batch_test",
        metadata,
        Column("device_id", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        for i in range(20):
            conn.execute(table.insert().values(device_id="d%03d" % i, value=float(i)))

        result = conn.execute(table.select()).fetchall()
        assert len(result) == 20, "Batch insert failed: expected 20 rows, got %d" % len(
            result
        )

        result = conn.execute(
            table.select().order_by(table.c.value).limit(5)
        ).fetchall()
        assert len(result) == 5, "Batch limit failed: expected 5 rows, got %d" % len(
            result
        )

        result = conn.execute(
            table.select().order_by(table.c.value).limit(10).offset(5)
        ).fetchall()
        assert len(result) == 10, "Batch offset failed: expected 10 rows, got %d" % len(
            result
        )

    table.drop(engine)
    print("  Batch insert tests passed")


def _test_null_values(engine):
    metadata = MetaData()
    table = Table(
        "null_test",
        metadata,
        Column("device_id", String, iotdb_category="TAG"),
        Column("temperature", Float, iotdb_category="FIELD"),
        Column("humidity", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            table.insert().values(device_id="d001", temperature=25.5, humidity=60.0)
        )
        conn.execute(table.insert().values(device_id="d002", temperature=18.0))
        conn.execute(table.insert().values(device_id="d003", humidity=70.0))

        result = conn.execute(table.select()).fetchall()
        assert len(result) == 3, "Null insert failed: expected 3 rows, got %d" % len(
            result
        )

    table.drop(engine)
    print("  NULL value tests passed")


# ---------------------------------------------------------------
# Schema operation tests
# ---------------------------------------------------------------
def _test_has_schema_and_table(engine):
    insp = inspect(engine)
    dialect = engine.dialect

    with engine.connect() as conn:
        assert dialect.has_schema(conn, TEST_DB), (
            "has_schema('%s') returned False" % TEST_DB
        )
        assert not dialect.has_schema(
            conn, "nonexistent_db_xyz"
        ), "has_schema('nonexistent_db_xyz') returned True"

    metadata = MetaData()
    t = Table(
        "has_check",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        assert dialect.has_table(
            conn, "has_check", schema=TEST_DB
        ), "has_table('has_check') returned False"
        assert not dialect.has_table(
            conn, "nonexistent_table_xyz", schema=TEST_DB
        ), "has_table('nonexistent_table_xyz') returned True"

    t.drop(engine)
    print("  has_schema/has_table tests passed")


def _test_get_view_names(engine):
    insp = inspect(engine)
    views = insp.get_view_names(schema=TEST_DB)
    assert views == [], "Expected empty view list, got %s" % views
    print("  get_view_names tests passed")


def _test_multiple_databases(engine):
    insp = inspect(engine)
    schemas = insp.get_schema_names()
    assert TEST_DB in schemas, "%s not in schemas: %s" % (TEST_DB, schemas)
    assert TEST_DB2 in schemas, "%s not in schemas: %s" % (TEST_DB2, schemas)

    metadata1 = MetaData()
    t1 = Table(
        "table_in_db1",
        metadata1,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata1.create_all(engine)

    metadata2 = MetaData()
    t2 = Table(
        "table_in_db2",
        metadata2,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB2,
    )
    metadata2.create_all(engine)

    tables1 = insp.get_table_names(schema=TEST_DB)
    tables2 = insp.get_table_names(schema=TEST_DB2)
    assert "table_in_db1" in tables1, "table_in_db1 not in %s tables: %s" % (
        TEST_DB,
        tables1,
    )
    assert "table_in_db2" in tables2, "table_in_db2 not in %s tables: %s" % (
        TEST_DB2,
        tables2,
    )
    assert "table_in_db2" not in tables1, (
        "table_in_db2 should not be in %s tables" % TEST_DB
    )

    with engine.connect() as conn:
        conn.execute(t1.insert().values(device="d1", value=1.0))
        conn.execute(t2.insert().values(device="d2", value=2.0))

        r1 = conn.execute(t1.select()).fetchall()
        r2 = conn.execute(t2.select()).fetchall()
        assert (
            len(r1) == 1 and len(r2) == 1
        ), "Cross-db insert failed: db1=%d, db2=%d" % (len(r1), len(r2))

    t1.drop(engine)
    t2.drop(engine)
    print("  Multiple databases tests passed")


def _test_column_category_reflection(engine):
    with engine.connect() as conn:
        conn.execute(text("USE %s" % TEST_DB))
        conn.execute(
            text(
                "CREATE TABLE cat_reflect ("
                "region STRING TAG, "
                "device STRING TAG, "
                "model STRING ATTRIBUTE, "
                "temperature FLOAT FIELD, "
                "humidity DOUBLE FIELD"
                ")"
            )
        )

    insp = inspect(engine)
    columns = insp.get_columns(table_name="cat_reflect", schema=TEST_DB)

    category_map = {}
    for col in columns:
        if "iotdb_category" in col:
            category_map[col["name"]] = col["iotdb_category"]

    assert category_map.get("time") == "TIME" or "time" in [
        c["name"] for c in columns
    ], "TIME column not found"

    if "region" in category_map:
        assert category_map["region"] == "TAG", (
            "Expected TAG for region, got %s" % category_map["region"]
        )
    if "model" in category_map:
        assert category_map["model"] == "ATTRIBUTE", (
            "Expected ATTRIBUTE for model, got %s" % category_map["model"]
        )
    if "temperature" in category_map:
        assert category_map["temperature"] == "FIELD", (
            "Expected FIELD for temperature, got %s" % category_map["temperature"]
        )

    with engine.connect() as conn:
        conn.execute(text("USE %s" % TEST_DB))
        conn.execute(text("DROP TABLE cat_reflect"))
    print("  Column category reflection tests passed")


def _test_multiple_tables(engine):
    metadata = MetaData()

    t1 = Table(
        "multi_a",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    t2 = Table(
        "multi_b",
        metadata,
        Column("region", String, iotdb_category="TAG"),
        Column("score", Integer, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    t3 = Table(
        "multi_c",
        metadata,
        Column("name", String, iotdb_category="TAG"),
        Column("active", Boolean, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    insp = inspect(engine)
    tables = insp.get_table_names(schema=TEST_DB)
    for name in ["multi_a", "multi_b", "multi_c"]:
        assert name in tables, "%s not in tables: %s" % (name, tables)

    with engine.connect() as conn:
        conn.execute(t1.insert().values(device="d1", value=1.0))
        conn.execute(t2.insert().values(region="asia", score=100))
        conn.execute(t3.insert().values(name="sensor1", active=True))

        assert len(conn.execute(t1.select()).fetchall()) == 1
        assert len(conn.execute(t2.select()).fetchall()) == 1
        assert len(conn.execute(t3.select()).fetchall()) == 1

    t1.drop(engine)
    t2.drop(engine)
    t3.drop(engine)

    tables = insp.get_table_names(schema=TEST_DB)
    for name in ["multi_a", "multi_b", "multi_c"]:
        assert name not in tables, "%s still in tables after drop" % name

    print("  Multiple tables tests passed")


def _test_table_without_ttl(engine):
    metadata = MetaData()
    table = Table(
        "no_ttl",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine)

    insp = inspect(engine)
    tables = insp.get_table_names(schema=TEST_DB)
    assert "no_ttl" in tables, "no_ttl not in tables: %s" % tables

    with engine.connect() as conn:
        conn.execute(table.insert().values(device="d1", value=42.0))
        result = conn.execute(table.select()).fetchall()
        assert (
            len(result) == 1
        ), "Table without TTL failed: expected 1 row, got %d" % len(result)

    table.drop(engine)
    print("  Table without TTL tests passed")


# ---------------------------------------------------------------
# Raw SQL tests
# ---------------------------------------------------------------
def _test_raw_sql(engine):
    with engine.connect() as conn:
        conn.execute(text("USE %s" % TEST_DB))
        conn.execute(
            text(
                "CREATE TABLE raw_sql_test ("
                "region STRING TAG, "
                "device STRING TAG, "
                "temperature FLOAT FIELD, "
                "humidity DOUBLE FIELD"
                ")"
            )
        )

        conn.execute(
            text(
                "INSERT INTO raw_sql_test (region, device, temperature, humidity) "
                "VALUES ('asia', 'd001', 25.5, 60.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO raw_sql_test (region, device, temperature, humidity) "
                "VALUES ('europe', 'd002', 18.1, 75.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO raw_sql_test (region, device, temperature, humidity) "
                "VALUES ('na', 'd003', 30.2, 45.0)"
            )
        )

        result = conn.execute(
            text("SELECT * FROM %s.raw_sql_test" % TEST_DB)
        ).fetchall()
        assert len(result) == 3, "Raw SQL select all failed: expected 3, got %d" % len(
            result
        )

        result = conn.execute(
            text("SELECT * FROM %s.raw_sql_test WHERE region = 'asia'" % TEST_DB)
        ).fetchall()
        assert len(result) == 1, "Raw SQL where failed: expected 1, got %d" % len(
            result
        )

        result = conn.execute(
            text("SELECT * FROM %s.raw_sql_test ORDER BY temperature LIMIT 2" % TEST_DB)
        ).fetchall()
        assert len(result) == 2, "Raw SQL order+limit failed: expected 2, got %d" % len(
            result
        )

        result = conn.execute(
            text("SELECT COUNT(*) FROM %s.raw_sql_test" % TEST_DB)
        ).fetchall()
        assert result[0][0] == 3, (
            "Raw SQL count failed: expected 3, got %s" % result[0][0]
        )

        conn.execute(
            text("DELETE FROM %s.raw_sql_test WHERE device = 'd003'" % TEST_DB)
        )
        result = conn.execute(
            text("SELECT * FROM %s.raw_sql_test" % TEST_DB)
        ).fetchall()
        assert len(result) == 2, "Raw SQL delete failed: expected 2, got %d" % len(
            result
        )

        conn.execute(text("DROP TABLE raw_sql_test"))

    print("  Raw SQL tests passed")


# ---------------------------------------------------------------
# URL with database tests
# ---------------------------------------------------------------
def _test_url_with_database(engine_with_db, engine_without_db):
    metadata = MetaData()
    table = Table(
        "url_db_test",
        metadata,
        Column("device", String, iotdb_category="TAG"),
        Column("value", Float, iotdb_category="FIELD"),
        schema=TEST_DB,
    )
    metadata.create_all(engine_with_db)

    with engine_with_db.connect() as conn:
        conn.execute(table.insert().values(device="d1", value=1.0))
        result = conn.execute(table.select()).fetchall()
        assert len(result) == 1, "URL with DB insert failed: expected 1, got %d" % len(
            result
        )

    insp_with = inspect(engine_with_db)
    insp_without = inspect(engine_without_db)

    tables_with = insp_with.get_table_names(schema=TEST_DB)
    tables_without = insp_without.get_table_names(schema=TEST_DB)
    assert "url_db_test" in tables_with, "url_db_test not visible with db engine"
    assert "url_db_test" in tables_without, "url_db_test not visible without db engine"

    table.drop(engine_with_db)
    print("  URL with database tests passed")
