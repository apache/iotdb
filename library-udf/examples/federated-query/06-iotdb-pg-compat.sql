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

-- 在 IoTDB 表模型会话中执行；先完成 MD5 账号和 pg_hba.conf 配置。
USE federated_demo;
SELECT device_id, name FROM query_pg(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:postgresql://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_compat', "PASSWORD" => 'Fq_Pg_2026!');
-- 预期 3 行：d1/温度计，d2/压力计，d3/NULL。
