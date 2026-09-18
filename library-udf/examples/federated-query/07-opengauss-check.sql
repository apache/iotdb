-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- 在 openGauss 中用数据库管理员执行。
\connect federated_demo
SHOW password_encryption_type;
SELECT current_database(), pg_encoding_to_char(encoding)
FROM pg_database WHERE datname=current_database();
SELECT rolname,
       CASE WHEN rolpassword LIKE 'md5%' THEN 'md5'
            WHEN rolpassword LIKE 'sha256%' THEN 'sha256'
            ELSE 'other' END AS password_type
FROM pg_authid
WHERE rolname IN ('federated_reader', 'federated_compat') ORDER BY rolname;

-- 等 IoTDB 查询结束后执行，预期 0 行。
SELECT usename, state, count(*) FROM pg_stat_activity
WHERE usename IN ('federated_reader', 'federated_compat')
GROUP BY usename, state ORDER BY usename, state;
