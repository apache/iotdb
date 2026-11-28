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

# IoTDB: PostgreSQL wire protocol

PostgreSQL clients query the **table model**, read-only: Microsoft Power BI (built-in PostgreSQL
connector, Import and DirectQuery, Desktop and on-premises data gateway), Excel, psql, JDBC
(pgJDBC, DBeaver), Python (psycopg), .NET (Npgsql) and others. Every database appears as a
PostgreSQL schema; queries are translated into table-model SQL and run by the database's own
engine, so time filters and aggregations stay in the engine.

The service is a built-in external service like REST and MQTT (module
`external-service-impl/postgresql`), off by default.

## Enabling

The jar ships in `lib/` of the distribution; it must stay there (not in `ext/external_service`),
because the database also loads its catalog tables from it. Set the keys in
`conf/iotdb-system.properties` (all keys and defaults are in the "PostgreSQL Service
Configuration" section of the template):

```properties
enable_postgresql_service=true
postgresql_service_address=127.0.0.1   # loopback by default
postgresql_service_port=5432
```

Other keys: `postgresql_service_max_connections` (32; more are refused with 53300),
`postgresql_service_idle_timeout_in_ms` (30 minutes), `postgresql_service_startup_timeout_in_ms`
(10 seconds), `postgresql_service_write_timeout_in_ms` (5 minutes: a client that stops reading
results loses its query and its connection).

## Security

- **TLS off loopback, no exceptions.** On any address other than loopback the service accepts only
  TLS 1.2/1.3 connections with AEAD cipher suites (`sslmode=require` or stricter); a plain
  connection gets `FATAL 28000 encryption required`. The certificate comes from `key_store_path` /
  `key_store_pwd`. Without a key store the service does not start off loopback and logs why
  (naming `postgresql_service_address`). On loopback, plain connections are allowed.
- **Logins** go through the database's normal login (`SessionManager.login`), with its lockout and
  error handling; refused logins get `28P01`. The client sends its password with PostgreSQL's
  cleartext password authentication, which is only ever offered inside TLS or on loopback.
  SCRAM-SHA-256 is not offered: it needs SCRAM verifiers stored for each user, and IoTDB stores a
  different password hash.
- A session that the database ends (for example a dropped user) is closed with `FATAL 57P01` at
  its next message.

## Power BI

### Certificates

Power BI Desktop and the on-premises data gateway (Npgsql) verify the server certificate: the
certificate must chain to a trusted root, and its subject alternative names must contain the name
used as "Server" in Power BI.

- Issue the certificate in `key_store_path` for the name clients use (DNS name or IP address),
  and import the issuing CA (or the self-signed certificate itself) on the client machines.

Import on Windows, for Desktop as the user and for the gateway on the gateway machine as the
gateway's service account (or the local machine): `certmgr.msc` (user) or `certlm.msc` (machine)
→ *Trusted Root Certification Authorities* → *Certificates* → *All Tasks* → *Import…*, select the
`.cer`/`.crt` file. Then use exactly one of the certificate's names as server, for example
`plc-17.factory.local:5432`; an IP address works only if the certificate contains it (and with
libpq-based clients only from libpq 15 on, which matches IP-address names).

### Connecting

1. Import the certificate as described above, on the Desktop machine and on the gateway machine.
2. *Get Data → PostgreSQL database*: server `<host>` (port 5432 by default), any database name,
   Import or DirectQuery, then user and password.
3. The navigator lists the databases you may read as schemas, with their tables and columns.
4. DirectQuery: aggregations, date hierarchy (`date_trunc`), relative date filters and Top N are
   translated and run in the database; anything without an exact translation is refused with
   SQLSTATE 0A000 naming the construct, never answered approximately.
5. Gateway: add a PostgreSQL data source with the same server name as in Desktop (one of the
   certificate's names) and keep certificate validation on.

## Names and types

- **Namespaces.** Schema = database: `mydb.sensors` is table `sensors` of database `mydb`. The
  `search_path` is empty by default, so unqualified user tables are refused with 42P01 and a hint;
  `SET search_path TO mydb` makes them resolvable. `pg_catalog` and `information_schema` are the
  emulated catalogs; IoTDB's own `information_schema` stays hidden from PostgreSQL clients.
- **Types.**

  | Table model | PostgreSQL |
  |---|---|
  | BOOLEAN | `bool` |
  | INT32 | `int4` |
  | INT64 | `int8` |
  | FLOAT | `float4` |
  | DOUBLE | `float8` |
  | STRING, TEXT | `text` |
  | BLOB | `bytea` |
  | DATE | `date` |
  | TIMESTAMP | `timestamptz` (session time zone; nanosecond precision is truncated to microseconds) |
  | OBJECT and others | not shown |

- **Column categories** (TIME, TAG, ATTRIBUTE, FIELD) are the column comments
  (`pg_description`, JDBC `REMARKS`).
- **Catalog tables.** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_proc`,
  `pg_description`, `pg_database` (one database, `iotdb`), `pg_roles`, `pg_settings` and others
  exist as `information_schema.pg_*` tables, and PostgreSQL's `information_schema` views as
  `information_schema.pg_is_*`. IoTDB clients see them in `SHOW TABLES FROM information_schema`.
  They list only what the user may read.

## Tree-model data

The service reads the table model only. Tree-model data becomes visible through a **table view**
over a path pattern: the path levels below the prefix become tags, the measurements become fields.
For devices `root.factory.<line>.<machine>` with the measurements `temperature` and `running`:

```sql
-- in the table model (CLI: -sql_dialect table)
CREATE DATABASE factory;
CREATE VIEW factory.machines (
  line STRING TAG,
  machine STRING TAG,
  temperature DOUBLE FIELD,
  running BOOLEAN FIELD
) AS root.factory.**;
```

PostgreSQL clients then see the view like a table (`relkind` `v`), for example in psql:

```
iotdb=> \dv factory.*
         List of relations
 Schema  |   Name   | Type | Owner
---------+----------+------+-------
 factory | machines | view | root

iotdb=> SELECT line, avg(temperature) FROM factory.machines GROUP BY line ORDER BY line;
 line  |  avg
-------+-------
 line1 | 21.25
 line2 |  19.5
```

Clients that list views show it next to the tables. One view is needed per device pattern;
the service does not create views by itself.

## Supported SQL

The rules are documented in `PgToTableSql`, `DateTimeRules` and `ConstructCheck`. In short:

- `SELECT` with joins, sub-queries, CTEs, `UNION`, `CASE`, aggregates (also `count(DISTINCT x)`),
  window functions (`row_number`, `rank`, `lead`, `lag`, ... `OVER (PARTITION BY ... ORDER BY ...)`
  without explicit frames), `LIMIT`/`OFFSET`/`FETCH FIRST`, `ILIKE`, casts (`::`), parameters
  (`$1`), `E''` strings.
- Dates in the session time zone (`SET TimeZone`): `date_trunc` (day across DST changes,
  month/quarter/year calendar-aware; hour/minute/second where the zone's offsets are whole; week
  only in zones without DST), `extract`/`date_part` (not `week`: the engine's week is not ISO),
  `timestamp ± interval` with fixed-length units (days only in zones without DST, never months or
  years), `now()`, `CURRENT_DATE`, `timestamp '...'` literals as PostgreSQL reads them.
- Strings: `lower`, `upper`, `length`, `substring`, `trim`, `replace`, `strpos`, `starts_with`,
  `concat`, `||`, `coalesce`, `nullif`, `LIKE`, anchored regular expressions (`~ '^(name)$'`).
- Session commands: `SET`/`RESET`/`SHOW` (`TimeZone`, `search_path`, `statement_timeout`,
  `application_name`, ...), `BEGIN`/`COMMIT`/`ROLLBACK` (no effect besides the transaction status),
  `DISCARD`, `DEALLOCATE`; SQL cursors (`DECLARE`, `FETCH`/`MOVE` forward, `CLOSE`), as psycopg's
  server-side cursors use them.
- Protocol: simple and extended query protocol, binary results, `setFetchSize` (portals), cancel
  requests (57014), `statement_timeout`.

## Limitations

- Read-only: `INSERT`, `UPDATE`, `DELETE`, DDL and `COPY` are refused with 25006.
- Table model only; tree-model data is visible through table views (see "Tree-model data").
- Constructs without an exact equivalent are refused with 0A000 naming them, among them arrays,
  `numeric(p,s)`, `INTERVAL` months/years, explicit window frames, `lpad`/`rpad`.
- Timestamps with nanosecond precision are sent with microsecond precision.
- `length` counts UTF-16 code units (differs from PostgreSQL only for characters outside the
  Basic Multilingual Plane).
- psql: `\l`, `\dn`, `\dt`, `\d table` work; `\d+` does not.
- The `iotdb` database name of `pg_database` does not follow the database name a client sends
  when connecting (`current_database()` does).

## Tests

- Unit and golden tests: `./mvnw -pl external-service-impl/postgresql test`. The client corpus
  (statements recorded from psql, pgJDBC and psycopg against PostgreSQL 14) is in
  `src/test/resources/corpus`, its expected translations in `corpus/expected`.
- Integration tests: `org.apache.iotdb.postgresql.it.*` in `integration-test`.
