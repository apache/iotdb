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

-- 在 IoTDB 表模型会话中执行；已执行 02-iotdb-register.sql。
-- URL 中的地址是 ConfigNode/DataNode 能访问的 openGauss 地址。
CREATE DATABASE IF NOT EXISTS federated_demo;
USE federated_demo;

-- 1. 确认真实外部数据库版本。
SELECT * FROM query_opengauss(
  SQL => 'SELECT version() AS db_version',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 2. 全类型与 NULL：应返回 d1、d2、d3 三行。
SELECT * FROM query_opengauss(
  SQL => 'SELECT * FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 3. 别名与 SQL 字符串转义：应返回 renamed_id=d1。
SELECT renamed_id FROM query_opengauss(
  SQL => 'SELECT device_id AS renamed_id FROM public.fq_devices WHERE device_id = ''d1''',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 4. NULL 过滤：应只返回 d3。
SELECT device_id FROM query_opengauss(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') WHERE name IS NULL;

-- 5. 空结果：应返回 0 行。
SELECT * FROM query_opengauss(
  SQL => 'SELECT device_id FROM public.fq_devices WHERE false',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 6. 跨多个 TsBlock：row_count=5001，id_sum=12507501。
SELECT count(*) AS row_count, sum(id) AS id_sum FROM query_opengauss(
  SQL => 'SELECT generate_series(1,5001) AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 7. IoTDB 本地数据与 openGauss 表 JOIN。
-- measurements 为本测试新建的 IoTDB 表；已有同名表时请改名。
CREATE TABLE measurements (device_id STRING TAG, temperature DOUBLE FIELD);
INSERT INTO measurements(time, device_id, temperature) VALUES
  (1, 'd1', 20.0), (2, 'd2', 30.0), (3, 'd4', 40.0);
SELECT l.device_id, r.name, l.temperature
FROM measurements l JOIN query_opengauss(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') r
ON l.device_id = r.device_id ORDER BY l.device_id;
-- 预期：d1 / 温度计 / 20.0；d2 / 压力计 / 30.0。d4 不匹配。

-- 8. GaussDB 连接器使用 Huawei 驱动读取同一 openGauss 实例，应返回 3 行。
-- 此结果仅验证 openGauss 后端，不能替代商用 GaussDB 实例验证。
SELECT device_id, name FROM query_gaussdb(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:gaussdb://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 9. 提前结束：应只返回 id=1；可重复执行后检查连接释放。
SELECT * FROM query_opengauss(
  SQL => 'SELECT generate_series(1,5001) AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') LIMIT 1;
