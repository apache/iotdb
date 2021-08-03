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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBQuotedPathIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] exp =
          new String[] {
            "1509465600000,true", "1509465600001,true", "1509465600002,false", "1509465600003,false"
          };
      statement.execute("SET STORAGE GROUP TO root.ln");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf.01\".wt01.\"status.2.3\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.ln.\"wf.01\".wt01(timestamp,\"status.2.3\") values(1509465600000,true)");
      statement.execute(
          "INSERT INTO root.ln.\"wf.01\".wt01(timestamp,\"status.2.3\") values(1509465600001,true)");
      statement.execute(
          "INSERT INTO root.ln.\"wf.01\".wt01(timestamp,\"status.2.3\") values(1509465600002,false)");
      statement.execute(
          "INSERT INTO root.ln.\"wf.01\".wt01(timestamp,\"status.2.3\") values(1509465600003,false)");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf.01\".wt02.\"abd\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf.01\".wt02.\"asf.asd.sdf\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf.01\".wt02.\"asd12\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");

      boolean hasResultSet = statement.execute("SELECT * FROM root.ln.\"wf.01\".wt01");
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      try {
        int cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }

        hasResultSet =
            statement.execute("SELECT * FROM root.ln.\"wf.01\".wt01 WHERE \"status.2.3\" = false");
        assertTrue(hasResultSet);
        exp = new String[] {"1509465600002,false", "1509465600003,false"};
        cnt = 0;
        resultSet = statement.getResultSet();
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }

        hasResultSet =
            statement.execute(
                "select \"status.2.3\", 'status.2.3' from root.ln.\"wf.01\".wt01 align by device");
        assertTrue(hasResultSet);
        exp =
            new String[] {
              "1509465600000,root.ln.\"wf.01\".wt01,true,'status.2.3',",
              "1509465600001,root.ln.\"wf.01\".wt01,true,'status.2.3',",
              "1509465600002,root.ln.\"wf.01\".wt01,false,'status.2.3',",
              "1509465600003,root.ln.\"wf.01\".wt01,false,'status.2.3',"
            };
        cnt = 0;
        resultSet = statement.getResultSet();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(exp[cnt++], builder.toString());
        }

        statement.execute(
            "DELETE FROM root.ln.\"wf.01\".wt01.\"status.2.3\" WHERE time < 1509465600001");
        statement.execute("DELETE TIMESERIES root.ln.\"wf.01\".wt01.\"status.2.3\"");
      } finally {
        resultSet.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testIllegalStorageGroup() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.\"ln\"");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals(
          "315: The storage group name can only be characters, numbers and underscores. root.\"ln\" is not a legal path",
          e.getMessage());
    }
  }
}
