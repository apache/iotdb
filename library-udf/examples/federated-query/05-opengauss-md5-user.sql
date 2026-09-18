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

-- 在 openGauss 的 gsql 中执行。
-- 执行前用 gs_guc reload 设置 password_encryption_type=0；执行后恢复为 2。
\connect federated_demo
SHOW password_encryption_type;
CREATE USER federated_compat PASSWORD 'Fq_Pg_2026!';
GRANT SELECT ON public.fq_devices TO federated_compat;
