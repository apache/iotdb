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

package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

@Category({LocalStandaloneTest.class})
public class IOTDBInsertAlignedValuesIT {
  private static Connection connection;
  private static final int oldTsFileGroupSizeInByte =
      TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
  private int numOfPointsPerPage;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    numOfPointsPerPage = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
  }

  @After
  public void tearDown() throws Exception {
    close();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(numOfPointsPerPage);
    EnvironmentUtils.cleanEnv();
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(oldTsFileGroupSizeInByte);
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testInsertAlignedValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (5000, true, 20.1)");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (6000, true, 22)");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));

    ResultSet rs2 = st1.executeQuery("select status, temperature from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(20.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(22, rs2.getFloat(3), 0.1);
    st1.close();
  }

  @Test
  public void testInsertAlignedNullableValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
    st0.execute("insert into root.t1.wf01.wt01(time, status) aligned values (5000, true)");
    st0.execute("insert into root.t1.wf01.wt01(time, temperature) aligned values (6000, 22)");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));

    ResultSet rs2 = st1.executeQuery("select status, temperature from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getObject(2));
    Assert.assertEquals(null, rs2.getObject(3));

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(null, rs2.getObject(2));
    Assert.assertEquals(22.0f, rs2.getObject(3));
    st1.close();
  }

  @Test
  public void testUpdatingAlignedValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
    st0.execute("insert into root.t1.wf01.wt01(time, status) aligned values (5000, true)");
    st0.execute("insert into root.t1.wf01.wt01(time, temperature) aligned values (5000, 20.1)");
    st0.execute("insert into root.t1.wf01.wt01(time, temperature) aligned values (6000, 22)");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));
    rs1.next();
    Assert.assertEquals(null, rs1.getObject(2));

    ResultSet rs2 = st1.executeQuery("select status, temperature from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getObject(2));
    Assert.assertEquals(20.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(null, rs2.getObject(2));
    Assert.assertEquals(22.0f, rs2.getObject(3));

    st1.execute("flush");
    ResultSet rs3 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs3.next();
    Assert.assertEquals(true, rs3.getBoolean(2));
    rs3.next();
    Assert.assertEquals(true, rs3.getBoolean(2));
    rs3.next();
    Assert.assertEquals(null, rs3.getObject(2));

    ResultSet rs4 = st1.executeQuery("select status, temperature from root.t1.wf01.wt01");
    rs4.next();
    Assert.assertEquals(4000, rs4.getLong(1));
    Assert.assertEquals(true, rs4.getBoolean(2));
    Assert.assertEquals(17.1, rs4.getFloat(3), 0.1);

    rs4.next();
    Assert.assertEquals(5000, rs4.getLong(1));
    Assert.assertEquals(true, rs4.getObject(2));
    Assert.assertEquals(20.1, rs4.getFloat(3), 0.1);

    rs4.next();
    Assert.assertEquals(6000, rs4.getLong(1));
    Assert.assertEquals(null, rs4.getObject(2));
    Assert.assertEquals(22.0f, rs4.getObject(3));
    st1.close();
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongMeasurementNum1() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values(11000, 100)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongMeasurementNum2() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute(
        "insert into root.t1.wf01.wt01(time, status, temperature) aligned values(11000, 100, 300, 400)");
  }

  @Test
  public void testInsertWithWrongType() throws SQLException {
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude INT32 encoding=PLAIN compressor=SNAPPY, longitude INT32 encoding=PLAIN compressor=SNAPPY) ");
      st1.execute("insert into root.lz.dev.GPS(time,latitude,longitude) aligned values(1,1.3,6.7)");
      Assert.fail();
    } catch (IoTDBSQLException e) {
      Assert.assertEquals(313, e.getErrorCode());
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage() throws SQLException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(S1 INT32 encoding=PLAIN compressor=SNAPPY, S2 INT32 encoding=PLAIN compressor=SNAPPY, S3 INT32 encoding=PLAIN compressor=SNAPPY) ");
      for (int i = 0; i < 100; i++) {
        if (i == 99) {
          st1.execute(
              "insert into root.lz.dev.GPS(time,S1,S3) aligned values("
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          st1.execute(
              "insert into root.lz.dev.GPS(time,S1,S2) aligned values("
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        }
      }
      st1.execute("flush");
    }
    try (Statement st2 = connection.createStatement()) {
      ResultSet rs1 = st2.executeQuery("select S3 from root.lz.dev.GPS");
      int rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(99, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(1, rowCount);

      rs1 = st2.executeQuery("select S2 from root.lz.dev.GPS");
      rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(rowCount, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(99, rowCount);

      rs1 = st2.executeQuery("select S1 from root.lz.dev.GPS");
      rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(rowCount, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(100, rowCount);
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage2() throws SQLException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(4);
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(S1 INT32 encoding=PLAIN compressor=SNAPPY, S2 INT32 encoding=PLAIN compressor=SNAPPY, S3 INT32 encoding=PLAIN compressor=SNAPPY) ");
      for (int i = 0; i < 100; i++) {
        if (i >= 49) {
          st1.execute(
              "insert into root.lz.dev.GPS(time,S1,S2,S3) aligned values("
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          st1.execute(
              "insert into root.lz.dev.GPS(time,S1,S2) aligned values("
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        }
      }
      st1.execute("flush");
    }
    try (Statement st2 = connection.createStatement()) {
      ResultSet rs1 = st2.executeQuery("select S3 from root.lz.dev.GPS");
      int rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(rowCount + 49, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(51, rowCount);

      rs1 = st2.executeQuery("select S2 from root.lz.dev.GPS");
      rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(rowCount, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(100, rowCount);

      rs1 = st2.executeQuery("select S1 from root.lz.dev.GPS");
      rowCount = 0;
      while (rs1.next()) {
        Assert.assertEquals(rowCount, rs1.getInt(2));
        rowCount++;
      }
      Assert.assertEquals(100, rowCount);
    }
  }
}
