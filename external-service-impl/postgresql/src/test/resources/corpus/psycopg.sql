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
-- Client: psycopg 3.3.9
-- Server: PostgreSQL 14.25 (docker postgres:14-alpine), log_statement=all
-- Captured: 2026-11-19 (T001, spec 012); schema refdb/other from corpus/README.md
-- next
BEGIN
-- next
SELECT version()
-- next
SELECT * FROM refdb.sensors LIMIT 5
-- next
-- parameters: $1 = 'm1', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 'm2', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 'm3', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 'm4', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 'm5', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 'm1', $2 = '2026-10-25 00:00:00+00', $3 = '10'
SELECT "time", temperature FROM refdb.sensors WHERE machine = $1 AND "time" >= $2 LIMIT $3
-- next
-- parameters: $1 = 't', $2 = '50'
SELECT * FROM refdb.sensors WHERE running = $1 AND level > $2 LIMIT 3
-- next
DECLARE "big" CURSOR FOR SELECT * FROM refdb.events
-- next
-- executed 21 times in a row (portal fetches or repeated executions)
FETCH FORWARD 5000 FROM "big"
-- next
CLOSE "big"
-- next
COMMIT
