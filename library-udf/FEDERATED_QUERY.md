<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Federated query table functions

[中文使用说明（含打包、注册与完整 SQL）](FEDERATED_QUERY_zh.md)

This plugin exposes remote JDBC query results as table-model relations in IoTDB.
It was migrated from `ty/mysql-connector` at `b7dc14dd490289add5fa2a92f09082a8e5339a99`
onto master at `87a6d198a901f46630a3455f59b264fc9368ca08`.
The plugin includes its own external-database exception classes and requires no
changes to the server or UDF API. The current master already provides the table-function lifecycle callbacks and UDF
error handling required by the original branch. Those existing implementations are
used without restoring the obsolete operators or duplicate status codes. The old
ConfigNode Ratis buffer-size adjustment is unrelated to this plugin and is not included.

## Connectors and drivers

All class names below have the prefix
`org.apache.iotdb.library.relational.tablefunction.connector.`.

| Class | JDBC driver | URL prefix |
| --- | --- | --- |
| `MySqlConnectorTableFunction` | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql:` |
| `PostgreSqlConnectorTableFunction` | `org.postgresql.Driver` | `jdbc:postgresql:` |
| `ClickhouseConnectorTableFunction` | `com.clickhouse.jdbc.ClickHouseDriver` | `jdbc:ch:` |
| `OpenGaussConnectorTableFunction` | `org.opengauss.Driver` | `jdbc:opengauss:` |
| `GaussDBConnectorTableFunction` | `com.huawei.gaussdb.jdbc.Driver` | `jdbc:gaussdb:` |

Cassandra, Doris, MongoDB, Redis, and Snowflake classes inherited from the old
branch are empty placeholders, not usable connectors.

GaussDB and openGauss have separate adapters. Huawei explicitly advises against
using the PostgreSQL JDBC driver for GaussDB, even when a particular version can
connect. See the [GaussDB driver documentation](https://support.huaweicloud.com/distributed-devg-v2-gaussdb/gaussdb-12-0056.html).

openGauss supports PostgreSQL-compatible authentication when the server and user
are configured for MD5. Its normal SHA-256 authentication requires an openGauss
compatible driver. The native adapter uses `org.opengauss:opengauss-jdbc:6.0.3-og`
(the `-og` variant has the `org.opengauss` namespace), avoiding collisions with
PostgreSQL. See the [openGauss Java guide](https://docs.opengauss.org/en/docs/latest/getting_started/java.html).
An openGauss test does not establish compatibility with every commercial GaussDB
release. Use the GaussDB driver version supplied for the target deployment.

## Build and installation

For a single plugin JAR containing all five JDBC drivers and runtime dependencies:

```sh
mvn clean package -pl library-udf -am -P federated-query-jar -DskipTests
```

Deploy `library-udf/target/library-udf-2.0.11-SNAPSHOT-federated-jar-with-dependencies.jar`
on its own in each node's `ext/udf/`. This opt-in profile excludes the server's UDF,
TsFile and SLF4J APIs, preserves reflective driver loading, and merges service metadata.
Do not combine it with `get-jar-with-dependencies` or duplicate thin/driver JARs.

For the thin JAR with separately installed drivers, build from the repository root:

```sh
mvn clean package -pl library-udf -am -DskipTests
```

Copy the resulting `library-udf-<version>.jar` and the driver JARs needed by your
connectors into `ext/udf/` on every ConfigNode and DataNode, before registering
functions. In the default thin build, JDBC dependencies are `provided` and `optional`: they are not bundled
into the plugin or propagated into IoTDB distributions.

Driver versions declared for compilation/testing are MySQL 9.3.0, PostgreSQL
42.7.7, ClickHouse 0.8.2 (`shaded-all`), openGauss 6.0.3-og, and GaussDB
v2.0-8.218.0 (`com.huaweicloud:gaussdbjdbc`), selected for GaussDB 25.1.32 /
V2.0-8.218.0. Install the appropriate vendor driver and its runtime dependencies
for the database being queried. Do not install GaussDB's `gsjdbc4.jar` or the
PostgreSQL-namespaced openGauss driver alongside PostgreSQL JDBC; these share class
names. The adapters in this plugin deliberately use distinct vendor namespaces.

In a table-model CLI session:

```sql
CREATE FUNCTION query_opengauss AS
  'org.apache.iotdb.library.relational.tablefunction.connector.OpenGaussConnectorTableFunction';
CREATE FUNCTION query_gaussdb AS
  'org.apache.iotdb.library.relational.tablefunction.connector.GaussDBConnectorTableFunction';
CREATE FUNCTION query_pg AS
  'org.apache.iotdb.library.relational.tablefunction.connector.PostgreSqlConnectorTableFunction';

SELECT * FROM query_opengauss(
  SQL => 'SELECT id, name FROM public.devices ORDER BY id',
  URL => 'jdbc:opengauss://127.0.0.1:5432/postgres',
  USERNAME => 'reader', "PASSWORD" => '<password>');

-- The remote result can be joined with a local IoTDB table.
SELECT l.time, l.device_id, r.name, l.temperature
FROM measurements l
JOIN query_opengauss(
  SQL => 'SELECT id, name FROM public.devices',
  URL => 'jdbc:opengauss://127.0.0.1:5432/postgres',
  USERNAME => 'reader', "PASSWORD" => '<password>') r
ON l.device_id = r.id;
```

`PASSWORD` is a reserved SQL keyword, so quote its name as `"PASSWORD"` when
using named arguments (or pass all four arguments positionally).

The `SQL` argument is executed in the external database's dialect. Use a dedicated
read-only database account. Column aliases are preserved. SQL, URL, USERNAME, and
PASSWORD are scalar arguments; URL/user/password defaults depend on the connector.
Passwords default to the empty string. Credentials are passed in the query and
serialized execution handle, so apply the deployment's existing query-log and
transport protections.

## Type mapping and current limits

On baseline `87a6d198a9`, an outer `ORDER BY device_id` on a standalone JDBC table
function can hit a planner null pointer (`deviceTableScanNode`). The tested SQL
examples put this ordering in the remote `SQL` argument. The local/remote JOIN
example with `ORDER BY l.device_id` passes. Do not rely on remote input ordering
after adding IoTDB operations that can reorder rows.

Integer types map to INT32/INT64, floating-point and DECIMAL/NUMERIC to FLOAT/DOUBLE,
character types to STRING, BOOLEAN/BIT to BOOLEAN, DATE to DATE, TIME/TIMESTAMP
(including JDBC TIMESTAMP_WITH_TIMEZONE) to TIMESTAMP, and binary/BLOB to BLOB.
NULL values remain NULL. Unsupported JDBC types produce a UDF type error; cast
arrays, JSON, UUID, or vendor-specific objects in the remote SQL when necessary.
DECIMAL/NUMERIC conversion to DOUBLE can lose precision. Temporal conversion uses
JDBC epoch milliseconds; the deployment must use IoTDB's default millisecond
precision. The default JDBC timezone follows the DataNode JVM, so configure the
same timezone on all nodes when reading timestamps without a timezone.

The analyzer opens a connection for result metadata; execution opens another
connection. The remote query runs as one table-function source, without automatic
predicate/join pushdown or parallel split discovery. Put remote filters in `SQL`.
Rows are emitted in bounded TsBlocks, but a JDBC driver may buffer the complete
remote result unless its URL/configuration enables streaming. ClickHouse's
metadata fallback executes the original SQL with `setMaxRows(1)` rather than
rewriting its LIMIT clause.

## Unit verification

```sh
mvn clean test -pl library-udf -am \
  -Dtest=JDBCConnectorTableFunctionTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false
```

These tests cover alias metadata and serialized handles, batching, resource
cleanup after query/close failures, argument validation, and vendor driver
namespace selection. End-to-end verification additionally requires real IoTDB
and external database instances.

## Verified configuration

The end-to-end results below used the initial GaussDB driver
`com.huaweicloud.gaussdb:gaussdbjdbc:506.0.0.b058-jdk7`. The current default is
`com.huaweicloud:gaussdbjdbc:v2.0-8.218.0`; changing the default does not establish
verification against a commercial GaussDB server.

On 2026-09-18, IoTDB 2.0.11-SNAPSHOT based on master `87a6d198a9` was tested
against a real openGauss 5.0.0 ARM64 instance, using a UTF-8 database in PG
compatibility mode. All 14 end-to-end assertions passed: native SHA-256 login,
expected rejection of that login by stock PostgreSQL JDBC, native and MD5
compatible queries, aliases, NULL values, numeric/date/timestamp/binary values,
5001 rows across batches, local/remote JOIN, and recovery after SQL/authentication
failures. The GaussDB adapter with its Huawei driver also queried this openGauss
instance successfully. Twenty additional `LIMIT 1` queries left no sessions for
either external test user in `pg_stat_activity`.

Both full-reactor locale compiles and the six connector unit tests passed.
A commercial GaussDB server, and live MySQL/ClickHouse servers, were not part of
this verification. For commercial GaussDB, select the official driver matching
the deployment and verify against that actual server before rollout.
