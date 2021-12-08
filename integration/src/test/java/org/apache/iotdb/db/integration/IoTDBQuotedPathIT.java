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

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBQuotedPathIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void test() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] exp =
          new String[] {
            "1509465600000,true", "1509465600001,true", "1509465600002,false", "1509465600003,false"
          };
      statement.execute("SET STORAGE GROUP TO root.ln");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf+01\".wt01.\"status+2+3\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.ln.\"wf+01\".wt01(timestamp,\"status+2+3\") values(1509465600000,true)");
      statement.execute(
          "INSERT INTO root.ln.\"wf+01\".wt01(timestamp,\"status+2+3\") values(1509465600001,true)");
      statement.execute(
          "INSERT INTO root.ln.\"wf+01\".wt01(timestamp,\"status+2+3\") values(1509465600002,false)");
      statement.execute(
          "INSERT INTO root.ln.\"wf+01\".wt01(timestamp,\"status+2+3\") values(1509465600003,false)");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf+01\".wt02.\"abd\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.\"wf+01\".wt02.\"asd12\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");

      boolean hasResultSet = statement.execute("SELECT * FROM root.ln.\"wf+01\".wt01");
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      try {
        int cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }

        hasResultSet =
            statement.execute("SELECT * FROM root.ln.\"wf+01\".wt01 WHERE \"status+2+3\" = false");
        assertTrue(hasResultSet);
        exp = new String[] {"1509465600002,false", "1509465600003,false"};
        cnt = 0;
        resultSet = statement.getResultSet();
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }

        statement.execute(
            "DELETE FROM root.ln.\"wf+01\".wt01.\"status+2+3\" WHERE time < 1509465600001");
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.`\"ln\"`");
    } catch (IoTDBSQLException e) {
      Assert.assertEquals(
          "315: The storage group name can only be characters, numbers and underscores. root.\"ln\" is not a legal path",
          e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
