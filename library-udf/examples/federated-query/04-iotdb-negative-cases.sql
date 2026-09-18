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

-- 在 IoTDB 表模型会话中逐条执行；前三条报错属于预期结果。
USE federated_demo;

-- 1. 不存在的列：应报外部 SQL/元数据错误。
SELECT * FROM query_opengauss(
  SQL => 'SELECT missing_column FROM public.fq_devices',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 2. 错误密码：应报认证失败。
SELECT * FROM query_opengauss(
  SQL => 'SELECT 1 AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'wrong-password');

-- 3. PostgreSQL 驱动尝试 SHA-256 账号：本次 openGauss 5.0.0 下应认证失败。
SELECT * FROM query_pg(
  SQL => 'SELECT 1 AS id',
  URL => 'jdbc:postgresql://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 4. 错误之后恢复查询：应返回 row_count=3。
SELECT count(*) AS row_count FROM query_opengauss(
  SQL => 'SELECT device_id FROM public.fq_devices',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');
