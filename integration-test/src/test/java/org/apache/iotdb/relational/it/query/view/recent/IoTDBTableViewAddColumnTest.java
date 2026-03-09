/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBTableViewAddColumnTest {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAddColumn() throws IoTDBConnectionException, StatementExecutionException {
    String[] sqls = {
      "set sql_dialect=tree",
      "create database root.db",
      "insert into root.db.风机组.风机号1(time,电压,current) aligned values(10000,99.09,88.07)",
      "insert into root.db.风机组.风机号2(time,电压,电流) values(20000,99.09,88.07)",
      "set sql_dialect=table",
      "create database db",
      "use db",
      "CREATE OR REPLACE VIEW \"风机表\""
          + "(\"风机组\" TAG, "
          + "\"风机号\" TAG, "
          + "\"电压\" DOUBLE FIELD, "
          + "\"电流\" DOUBLE FIELD) "
          + "with (ttl='INF') "
          + "AS root.db.**",
      "ALTER VIEW IF EXISTS \"风机表\" ADD COLUMN IF NOT EXISTS \"风机组\" TAG",
      "ALTER VIEW IF EXISTS \"风机表\" ADD COLUMN IF NOT EXISTS \"风机组\" string",
      "ALTER VIEW IF EXISTS \"风机表\" ADD COLUMN if not exists \"风机组\" field",
      "ALTER VIEW IF EXISTS \"风机表\" ADD COLUMN if not exists \"电流\" DOUBLE FIELD from current",
      "show create view \"风机表\"",
    };
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (String sql : sqls) {
        session.executeNonQueryStatement(sql);
      }

      session.executeNonQueryStatement(
          "ALTER VIEW IF EXISTS \"风机表\" ADD COLUMN if not exists current field");
      SessionDataSet sessionDataSet = session.executeQueryStatement("desc \"风机表\"");
      int rowNum = 0;
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        rowNum++;
      }
      Assert.assertEquals(6, rowNum);
    }
  }
}
