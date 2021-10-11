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
import org.apache.iotdb.jdbc.AbstractIoTDBJDBCResultSet;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBTracingIT {

  @Before
  public void setUp() throws ClassNotFoundException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("SET STORAGE GROUP TO root.tracing");
      statement.execute("CREATE TIMESERIES root.tracing.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");

      String insertTemplate = "INSERT INTO root.tracing.d1(timestamp, s1) VALUES(%d,%d)";

      // prepare seq-file
      for (int i = 200; i < 300; i++) {
        statement.execute(String.format(insertTemplate, i, i));
      }
      statement.execute("flush");

      for (int i = 400; i < 500; i++) {
        statement.execute(String.format(insertTemplate, i, i));
      }
      statement.execute("flush");

      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(insertTemplate, i, i));
      }
      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void tracingTest() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");

    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing on");
    } catch (Exception e) {
      Assert.assertEquals("411: TRACING ON/OFF hasn't been supported yet", e.getMessage());
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing off");
    } catch (Exception e) {
      Assert.assertEquals("411: TRACING ON/OFF hasn't been supported yet", e.getMessage());
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing select s1 from root.tracing.d1");
      AbstractIoTDBJDBCResultSet resultSet = (AbstractIoTDBJDBCResultSet) statement.getResultSet();
      Assert.assertTrue(resultSet.isSetTracingInfo());
      Assert.assertEquals(1, resultSet.getStatisticsByName("seriesPathNum"));
      Assert.assertEquals(2, resultSet.getStatisticsByName("seqFileNum"));
      Assert.assertEquals(1, resultSet.getStatisticsByName("unSeqFileNum"));
      Assert.assertEquals(2, resultSet.getStatisticsByName("seqChunkNum"));
      Assert.assertEquals(200, resultSet.getStatisticsByName("seqChunkPointNum"));
      Assert.assertEquals(1, resultSet.getStatisticsByName("unSeqChunkNum"));
      Assert.assertEquals(100, resultSet.getStatisticsByName("unSeqChunkPointNum"));
      Assert.assertEquals(3, resultSet.getStatisticsByName("totalPageNum"));
      Assert.assertEquals(0, resultSet.getStatisticsByName("overlappedPageNum"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
