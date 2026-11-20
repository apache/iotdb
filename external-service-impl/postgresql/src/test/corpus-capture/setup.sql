--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- Reference schema for recording client SQL (spec 012, T001). Run against PostgreSQL 14 as
-- postgres: creates database iotdb with schemas refdb and other. The time range covers the
-- end of summer time in Europe (2026-10-25).
CREATE DATABASE iotdb;
\c iotdb
CREATE SCHEMA refdb;
CREATE TABLE refdb.sensors (
  "time" timestamptz NOT NULL,
  plant text, line text, machine text, model text,
  temperature double precision, pressure real, count bigint, level integer,
  running boolean, state text, note text, blob bytea, day date, ts timestamptz);
COMMENT ON COLUMN refdb.sensors.plant IS 'TAG';
COMMENT ON COLUMN refdb.sensors.model IS 'ATTRIBUTE';
INSERT INTO refdb.sensors
SELECT t, 'plant' || p, 'line' || l, 'm' || m, 'model' || (m % 3),
       20 + 5 * sin(extract(epoch FROM t) / 3600) + random(), (1 + random())::real,
       (extract(epoch FROM t))::bigint, (random() * 100)::int, random() > 0.2,
       (ARRAY['RUN','IDLE','STOP'])[1 + (random() * 2)::int], NULL, NULL, t::date, t
FROM generate_series(1, 3) p, generate_series(1, 4) l, generate_series(1, 5) m,
     generate_series(timestamptz '2026-10-20 00:00+00', timestamptz '2026-11-03 00:00+00', interval '5 minutes') t;
CREATE TABLE refdb.events ("time" timestamptz NOT NULL, machine text, code integer, message text);
INSERT INTO refdb.events
SELECT timestamptz '2026-10-20 00:00+00' + (i * interval '37 seconds'), 'm' || (i % 5 + 1), i % 17, 'event ' || i
FROM generate_series(1, 100000) i;
CREATE SCHEMA other;
CREATE TABLE other.weather ("time" timestamptz NOT NULL, station text, temperature double precision);
ANALYZE;
