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

-- 在 IoTDB 表模型会话中执行；同名函数已注册时跳过对应 CREATE。
CREATE FUNCTION query_opengauss AS
  'org.apache.iotdb.library.relational.tablefunction.connector.OpenGaussConnectorTableFunction';
CREATE FUNCTION query_gaussdb AS
  'org.apache.iotdb.library.relational.tablefunction.connector.GaussDBConnectorTableFunction';
CREATE FUNCTION query_pg AS
  'org.apache.iotdb.library.relational.tablefunction.connector.PostgreSqlConnectorTableFunction';
CREATE FUNCTION query_mysql AS
  'org.apache.iotdb.library.relational.tablefunction.connector.MySqlConnectorTableFunction';
CREATE FUNCTION query_clickhouse AS
  'org.apache.iotdb.library.relational.tablefunction.connector.ClickhouseConnectorTableFunction';
SHOW FUNCTIONS;
