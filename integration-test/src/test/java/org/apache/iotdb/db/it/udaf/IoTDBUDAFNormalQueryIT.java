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

package org.apache.iotdb.db.it.udaf;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFNormalQueryIT {
  private static final double DELTA = 1E-6;

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.noDataRegion.s1 WITH DATATYPE=INT32"
      };
  private static final String[] dataSet2 =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private static final String[] dataSet3 =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.sg.d1(timestamp,s1) values(5,5)",
        "insert into root.sg.d1(timestamp,s1) values(12,12)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(15,15)",
        "insert into root.sg.d1(timestamp,s1) values(25,25)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(1,111)",
        "insert into root.sg.d1(timestamp,s1) values(20,200)",
        "flush",
      };
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    // Reuse one session for schema creation, fixture writes, flushes and UDAF registration.
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);
      registerUDAF(session);
    }
  }

  private static void prepareData(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    for (String sql : creationSqls) {
      session.executeNonQueryStatement(sql);
    }

    // Batch the fixture without SQL parsing per row; UDAF queries retain all original input points.
    Tablet tablet =
        new Tablet(
            "root.vehicle.d0",
            List.of(
                new MeasurementSchema("s0", TSDataType.INT32),
                new MeasurementSchema("s1", TSDataType.INT64),
                new MeasurementSchema("s2", TSDataType.FLOAT),
                new MeasurementSchema("s3", TSDataType.TEXT),
                new MeasurementSchema("s4", TSDataType.BOOLEAN)),
            500);

    // prepare BufferWrite file
    insertRange(session, tablet, 5000, 7000, true);
    session.executeNonQueryStatement("flush");

    insertRange(session, tablet, 7500, 8500, false);
    session.executeNonQueryStatement("flush");
    // prepare Unseq-File
    insertRange(session, tablet, 500, 1500, true);
    session.executeNonQueryStatement("flush");
    insertRange(session, tablet, 3000, 6500, false);

    // prepare BufferWrite cache
    insertRange(session, tablet, 9000, 10000, true);
    // prepare Overflow cache
    insertRange(session, tablet, 2000, 2500, false);

    for (String sql : dataSet3) {
      session.executeNonQueryStatement(sql);
    }

    for (String sql : dataSet2) {
      session.executeNonQueryStatement(sql);
    }
  }

  private static void insertRange(
      ISession session, Tablet tablet, int startTime, int endTime, boolean booleanValue)
      throws IoTDBConnectionException, StatementExecutionException {
    // Reuse the bounded tablet while preserving write order and the callers' flush boundaries.
    for (int time = startTime; time < endTime; time++) {
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, time);
      tablet.addValue("s0", row, time);
      tablet.addValue("s1", row, (long) time);
      tablet.addValue("s2", row, (float) time);
      tablet.addValue("s3", row, Integer.toString(time));
      tablet.addValue("s4", row, booleanValue);
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() > 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDAF(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "CREATE FUNCTION avg_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFAvg'");
    session.executeNonQueryStatement(
        "CREATE FUNCTION count_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
    session.executeNonQueryStatement(
        "CREATE FUNCTION sum_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFSum'");
  }

  @Test
  public void singleUDAFTest() {
    String[] retArray = new String[] {"2001,2001,2001,2001", "7500,7500,7500,7500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void multipleUDAFTest() {
    double[][] retArray = {
      {1.4508E7, 7250.374812593702},
      {626750.0, 1250.9980039920158}
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void UDAFTypeMismatchErrorTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg_udaf(s3) FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: TEXT."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum_udaf(s3)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: TEXT."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg_udaf(s4)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum_udaf(s4)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT avg_udaf(status) FROM root.ln.wf01.wt01")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
