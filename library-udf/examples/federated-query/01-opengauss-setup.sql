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

-- 在 openGauss 的 gsql 中执行；运行前确认 password_encryption_type=2。
-- 以下数据库、账号为独立测试对象；首次执行时应不存在。
CREATE DATABASE federated_demo WITH ENCODING='UTF8' TEMPLATE=template0 DBCOMPATIBILITY='PG';
\connect federated_demo
CREATE USER federated_reader PASSWORD 'Fq_Native_2026!';

CREATE TABLE public.fq_devices (
  device_id varchar(20),
  name varchar(100),
  temperature double precision,
  enabled boolean,
  measured_at timestamp,
  installed_on date,
  payload bytea,
  amount numeric(10,2),
  seq bigint
);

INSERT INTO public.fq_devices VALUES
  ('d1', '温度计', 21.5, true, '2025-06-27 12:34:56.123', '2025-06-27',
   decode('00ff10', 'hex'), 123.45, 9000000001),
  ('d2', '压力计', -5.25, false, '2025-06-28 01:02:03', '2025-06-28',
   decode('abcd', 'hex'), -7.50, 9000000002),
  ('d3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

GRANT SELECT ON public.fq_devices TO federated_reader;
SELECT version();
SELECT device_id, name, temperature, enabled, amount, seq
FROM public.fq_devices ORDER BY device_id;
