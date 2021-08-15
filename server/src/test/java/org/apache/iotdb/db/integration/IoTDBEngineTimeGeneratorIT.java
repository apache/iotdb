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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBEngineTimeGeneratorIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;

  private static int count = 0;
  private static int count2 = 150;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(100);
    tsFileConfig.setPageSizeInByte(1024 * 1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1024 * 100);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 1024 * 100);

    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    tsFileConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    tsFileConfig.setPageSizeInByte(pageSizeInByte);
    tsFileConfig.setGroupSizeInByte(groupSizeInByte);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);

    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // create storage group and measurement
      for (String sql : TestConstant.create_sql) {
        statement.execute(sql);
      }

      // insert data (time from 300-999)
      for (long time = 300; time < 1000; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[(int) time % 5]);
        statement.execute(sql);

        if (satisfyTimeFilter1(time)) {
          count++;
        }

        if (satisfyTimeFilter2(time)) {
          count2++;
        }
      }

      statement.execute("FLUSH");

      // insert data (time from 1200-1499)
      for (long time = 1200; time < 1500; time++) {
        String sql = null;
        if (time % 2 == 0) {
          sql =
              String.format(
                  "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
          statement.execute(sql);
          sql =
              String.format(
                  "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
          statement.execute(sql);
          if (satisfyTimeFilter1(time)) {
            count++;
          }
        }
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[(int) time % 5]);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** value >= 14 && time > 500 */
  private static boolean satisfyTimeFilter1(long time) {
    return time % 17 >= 14 && time > 500;
  }

  /** root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11 || time > 900 */
  private static boolean satisfyTimeFilter2(long time) {
    return (time % 17 >= 5 || time > 900) && (time % 31 >= 11.5 || time > 900);
  }

  /** value >= 14 && time > 500 */
  @Test
  public void testOneSeriesWithValueAndTimeFilter()
      throws IOException, StorageEngineException, IllegalPathException, QueryProcessException {
    // System.out.println("Test >>> root.vehicle.d0.s0 >= 14 && time > 500");

    PartialPath pd0s0 =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    ValueFilter.ValueGtEq valueGtEq = ValueFilter.gtEq(14);
    TimeFilter.TimeGt timeGt = TimeFilter.gt(500);

    SingleSeriesExpression singleSeriesExpression =
        new SingleSeriesExpression(pd0s0, FilterFactory.and(valueGtEq, timeGt));
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    List<PartialPath> paths = new ArrayList<>();
    paths.add(pd0s0);
    queryPlan.setDeduplicatedPathsAndUpdate(paths);
    queryPlan.setExpression(singleSeriesExpression);
    ServerTimeGenerator timeGenerator = new ServerTimeGenerator(TEST_QUERY_CONTEXT, queryPlan);

    int cnt = 0;
    while (timeGenerator.hasNext()) {
      long time = timeGenerator.next();
      assertTrue(satisfyTimeFilter1(time));
      cnt++;
      // System.out.println("cnt =" + cnt + "; time = " + time);
    }
    assertEquals(count, cnt);
  }

  /** root.vehicle.d1.s0 >= 5, and d1.s0 has no data */
  @Test
  public void testEmptySeriesWithValueFilter()
      throws IOException, StorageEngineException, IllegalPathException, QueryProcessException {
    // System.out.println("Test >>> root.vehicle.d1.s0 >= 5");

    PartialPath pd1s0 =
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    ValueFilter.ValueGtEq valueGtEq = ValueFilter.gtEq(5);

    IExpression singleSeriesExpression = new SingleSeriesExpression(pd1s0, valueGtEq);
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    List<PartialPath> paths = new ArrayList<>();
    paths.add(pd1s0);
    queryPlan.setDeduplicatedPathsAndUpdate(paths);
    queryPlan.setExpression(singleSeriesExpression);
    ServerTimeGenerator timeGenerator = new ServerTimeGenerator(TEST_QUERY_CONTEXT, queryPlan);

    int cnt = 0;
    while (timeGenerator.hasNext()) {
      cnt++;
    }
    assertEquals(0, cnt);
  }

  /** root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900 */
  @Test
  public void testMultiSeriesWithValueFilterAndTimeFilter()
      throws IOException, StorageEngineException, IllegalPathException, QueryProcessException {
    System.out.println(
        "Test >>> root.vehicle.d0.s0 >= 5 && root.vehicle.d0.s2 >= 11.5 || time > 900");

    PartialPath pd0s0 =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    PartialPath pd0s2 =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s2);

    ValueFilter.ValueGtEq valueGtEq5 = ValueFilter.gtEq(5);
    ValueFilter.ValueGtEq valueGtEq11 = ValueFilter.gtEq(11.5f);
    TimeFilter.TimeGt timeGt = TimeFilter.gt(900L);

    IExpression singleSeriesExpression1 =
        new SingleSeriesExpression(pd0s0, FilterFactory.or(valueGtEq5, timeGt));
    IExpression singleSeriesExpression2 =
        new SingleSeriesExpression(pd0s2, FilterFactory.or(valueGtEq11, timeGt));
    IExpression andExpression =
        BinaryExpression.and(singleSeriesExpression1, singleSeriesExpression2);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    List<PartialPath> paths = new ArrayList<>();
    paths.add(pd0s0);
    paths.add(pd0s2);
    queryPlan.setDeduplicatedPathsAndUpdate(paths);
    queryPlan.setExpression(andExpression);
    ServerTimeGenerator timeGenerator = new ServerTimeGenerator(TEST_QUERY_CONTEXT, queryPlan);
    int cnt = 0;
    while (timeGenerator.hasNext()) {
      long time = timeGenerator.next();
      assertTrue(satisfyTimeFilter2(time));
      cnt++;
      //       System.out.println("cnt =" + cnt + "; time = " + time);
    }
    assertEquals(count2, cnt);
  }
}
