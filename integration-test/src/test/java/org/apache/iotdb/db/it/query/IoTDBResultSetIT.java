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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
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
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBResultSetIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.t1",
        "CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt01.type WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt01.grade WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.t1.wf01.wt02.temperature WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt02.type WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.t1.wf01.wt02.grade WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.dev.status WITH DATATYPE=text,ENCODING=PLAIN",
        "insert into root.sg.dev(time,status) values(1,3.14)"
      };

  private static final String[] emptyResultSet = new String[] {};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
        assertEquals("TIMESTAMP", metaData.getColumnTypeName(1));
        assertEquals("root.sg.dev.status", metaData.getColumnName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        assertEquals("TEXT", metaData.getColumnTypeName(2));
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void emptyQueryTest1() {
    String expectedHeader = ColumnHeaderConstant.TIME + ",";
    resultSetEqualTest("select * from root.sg1.d1", expectedHeader, emptyResultSet);
  }

  @Test
  public void emptyQueryTest2() {
    String expectedHeader =
        ColumnHeaderConstant.TIME
            + ","
            + "root.t1.wf01.wt02.grade,"
            + "root.t1.wf01.wt02.temperature,"
            + "root.t1.wf01.wt02.type,"
            + "root.t1.wf01.wt02.status,";
    resultSetEqualTest("select * from root.t1.wf01.wt02", expectedHeader, emptyResultSet);
  }

  @Test
  public void emptyShowTimeseriesTest() {
    String expectedHeader =
        ColumnHeaderConstant.TIMESERIES
            + ","
            + ColumnHeaderConstant.ALIAS
            + ","
            + ColumnHeaderConstant.DATABASE
            + ","
            + ColumnHeaderConstant.DATATYPE
            + ","
            + ColumnHeaderConstant.ENCODING
            + ","
            + ColumnHeaderConstant.COMPRESSION
            + ","
            + ColumnHeaderConstant.TAGS
            + ","
            + ColumnHeaderConstant.ATTRIBUTES
            + ","
            + ColumnHeaderConstant.DEADBAND
            + ","
            + ColumnHeaderConstant.DEADBAND_PARAMETERS
            + ","
            + ColumnHeaderConstant.VIEW_TYPE
            + ",";
    resultSetEqualTest("show timeseries root.sg1.**", expectedHeader, emptyResultSet);
  }

  @Test
  public void emptyShowDeviceTest() {
    String expectedHeader =
        ColumnHeaderConstant.DEVICE
            + ","
            + ColumnHeaderConstant.IS_ALIGNED
            + ","
            + ColumnHeaderConstant.TEMPLATE
            + ","
            + ColumnHeaderConstant.COLUMN_TTL
            + ",";
    resultSetEqualTest("show devices root.sg1.**", expectedHeader, emptyResultSet);
  }

  @Test
  public void timeWasNullTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s3 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");

      for (int i = 0; i < 10; i++) {
        statement.addBatch(
            "insert into root.sg1.d1(timestamp, s1, s2) values(" + i + "," + 1 + "," + 1 + ")");
      }

      statement.execute("insert into root.sg1.d1(timestamp, s3) values(103, 1)");
      statement.execute("insert into root.sg1.d1(timestamp, s3) values(104, 1)");
      statement.execute("insert into root.sg1.d1(timestamp, s3) values(105, 1)");
      statement.executeBatch();
      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
          for (int i = 1; i <= columnCount; i++) {
            int ct = metaData.getColumnType(i);
            if (ct == Types.TIMESTAMP) {
              if (resultSet.wasNull()) {
                fail();
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void emptyLastQueryTest() {
    String expectedHeader =
        ColumnHeaderConstant.TIME
            + ","
            + ColumnHeaderConstant.TIMESERIES
            + ","
            + ColumnHeaderConstant.VALUE
            + ","
            + ColumnHeaderConstant.DATATYPE
            + ",";
    resultSetEqualTest("select last s1 from root.sg.d1", expectedHeader, emptyResultSet);
  }
}
