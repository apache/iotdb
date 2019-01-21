/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.control.OpenedFilePathsManager;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the IoTDB server should be
 * defined as integration test.
 */
public class IoTDBEngineTimeGeneratorIT {

  private static IoTDB daemon;
  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;
  private static Connection connection;

  private static int count = 0;
  private static int count2 = 150;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
    pageSizeInByte = tsFileConfig.pageSizeInByte;
    groupSizeInByte = tsFileConfig.groupSizeInByte;

    // new value
    tsFileConfig.maxNumberOfPointsInPage = 100;
    tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
    tsFileConfig.groupSizeInByte = 1024 * 1024 * 100;

    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();

    Thread.sleep(5000);
    insertData();
    connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    connection.close();

    daemon.stop();
    Thread.sleep(5000);

    // recovery value
    tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
    tsFileConfig.pageSizeInByte = pageSizeInByte;
    tsFileConfig.groupSizeInByte = groupSizeInByte;

    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();

      // create storage group and measurement
      for (String sql : Constant.create_sql) {
        statement.execute(sql);
      }

      // insert data (time from 300-999)
      for (long time = 300; time < 1000; time++) {
        String sql = String
            .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql = String
            .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
        statement.execute(sql);
        sql = String
            .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time,
            Constant.stringValue[(int) time % 5]);
        statement.execute(sql);

        if (satisfyTimeFilter1(time)) {
          count++;
        }

        if (satisfyTimeFilter2(time)) {
          count2++;
        }
      }

      statement.execute("flush");

      // insert data (time from 1200-1499)
      for (long time = 1200; time < 1500; time++) {
        String sql = null;
        if (time % 2 == 0) {
          sql = String
              .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
          statement.execute(sql);
          sql = String
              .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
          statement.execute(sql);
          if (satisfyTimeFilter1(time)) {
            count++;
          }
        }
        sql = String
            .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time,
            Constant.stringValue[(int) time % 5]);
        statement.execute(sql);
      }

      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  /**
   * value >= 14 && time > 500
   */
  private static boolean satisfyTimeFilter1(long time) {
    return time % 17 >= 14 && time > 500;
  }

  /**
   * root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11 || time > 900
   */
  private static boolean satisfyTimeFilter2(long time) {
    return (time % 17 >= 5 || time > 900) && (time % 31 >= 11.5 || time > 900);
  }

  /**
   * value >= 14 && time > 500
   */
  @Test
  public void testOneSeriesWithValueAndTimeFilter() throws IOException, FileNodeManagerException {
    System.out.println("Test >>> root.vehicle.d0.s0 >= 14 && time > 500");

    Path pd0s0 = new Path(Constant.d0s0);
    ValueFilter.ValueGtEq valueGtEq = ValueFilter.gtEq(14);
    TimeFilter.TimeGt timeGt = TimeFilter.gt(500);

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(pd0s0,
        FilterFactory.and(valueGtEq, timeGt));
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(0);
    EngineTimeGenerator timeGenerator = new EngineTimeGenerator(0, singleSeriesExpression);

    int cnt = 0;
    while (timeGenerator.hasNext()) {
      long time = timeGenerator.next();
      assertTrue(satisfyTimeFilter1(time));
      cnt++;
      // System.out.println("cnt =" + cnt + "; time = " + time);
    }
    assertEquals(count, cnt);
  }

  /**
   * root.vehicle.d1.s0 >= 5, and d1.s0 has no data
   */
  @Test
  public void testEmptySeriesWithValueFilter() throws IOException, FileNodeManagerException {
    System.out.println("Test >>> root.vehicle.d1.s0 >= 5");

    Path pd1s0 = new Path(Constant.d1s0);
    ValueFilter.ValueGtEq valueGtEq = ValueFilter.gtEq(5);

    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(0);
    IExpression singleSeriesExpression = new SingleSeriesExpression(pd1s0, valueGtEq);
    EngineTimeGenerator timeGenerator = new EngineTimeGenerator(0, singleSeriesExpression);

    int cnt = 0;
    while (timeGenerator.hasNext()) {
      cnt++;
    }
    assertEquals(0, cnt);
  }

  /**
   * root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900
   */
  @Test
  public void testMultiSeriesWithValueFilterAndTimeFilter()
      throws IOException, FileNodeManagerException {
    System.out
        .println("Test >>> root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900");

    Path pd0s0 = new Path(Constant.d0s0);
    Path pd0s2 = new Path(Constant.d0s2);

    ValueFilter.ValueGtEq valueGtEq5 = ValueFilter.gtEq(5);
    ValueFilter.ValueGtEq valueGtEq11 = ValueFilter.gtEq(11.5f);
    TimeFilter.TimeGt timeGt = TimeFilter.gt(900L);

    IExpression singleSeriesExpression1 = new SingleSeriesExpression(pd0s0,
        FilterFactory.or(valueGtEq5, timeGt));
    IExpression singleSeriesExpression2 = new SingleSeriesExpression(pd0s2,
        FilterFactory.or(valueGtEq11, timeGt));
    IExpression andExpression = BinaryExpression
        .and(singleSeriesExpression1, singleSeriesExpression2);

    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(0);
    EngineTimeGenerator timeGenerator = new EngineTimeGenerator(0, andExpression);
    int cnt = 0;
    while (timeGenerator.hasNext()) {
      long time = timeGenerator.next();
      assertTrue(satisfyTimeFilter2(time));
      cnt++;
      // System.out.println("cnt =" + cnt + "; time = " + time);
    }
    assertEquals(count2, cnt);
  }
}
