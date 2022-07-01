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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBResultSetIT {

  private static final String[] SQLs =
      new String[] {
        "SET STORAGE GROUP TO root.t1",
        "CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt01.type WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt01.grade WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.dev.status WITH DATATYPE=text,ENCODING=PLAIN",
        "insert into root.sg.dev(time,status) values(1,3.14)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void intAndLongConversionTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.t1.wf01.wt01(timestamp, status, type, grade) values (1000, true, 1, 1000)");
      statement.execute(
          "insert into root.t1.wf01.wt01(timestamp, status, type, grade) values (2000, false, 2, 2000)");

      try (ResultSet resultSet1 =
          statement.executeQuery("select count(status) from root.t1.wf01.wt01")) {
        resultSet1.next();
        // type of r1 is INT64(long), test long convert to int
        int countStatus = resultSet1.getInt(1);
        Assert.assertEquals(2L, countStatus);
      }

      try (ResultSet resultSet2 =
          statement.executeQuery("select type from root.t1.wf01.wt01 where time = 1000 limit 1")) {
        resultSet2.next();
        // type of r2 is INT32(int), test int convert to long
        long type = resultSet2.getLong(2);
        Assert.assertEquals(1, type);
      }

      try (ResultSet resultSet3 =
          statement.executeQuery("select grade from root.t1.wf01.wt01 where time = 1000 limit 1")) {
        resultSet3.next();
        // type of r3 is INT64(long), test long convert to int
        int grade = resultSet3.getInt(2);
        Assert.assertEquals(1000, grade);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void columnTypeTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select status from root.sg.dev")) {
        Assert.assertTrue(resultSet.next());
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(2, metaData.getColumnCount());
        assertEquals("Time", metaData.getColumnName(1));
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
        assertEquals("TIME", metaData.getColumnTypeName(1));
        assertEquals("root.sg.dev.status", metaData.getColumnName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        assertEquals("TEXT", metaData.getColumnTypeName(2));
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
