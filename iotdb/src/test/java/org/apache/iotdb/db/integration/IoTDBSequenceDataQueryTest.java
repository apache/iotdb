/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the IoTDB server should be
 * defined as integration test. In this test case, no unseq insert data.
 */
public class IoTDBSequenceDataQueryTest {

  private static IoTDB daemon;
  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;
  private static Connection connection;

  // count : d0s0 >= 14
  private static int count = 0;

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

        if (time % 17 >= 14) {
          count++;
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
          if (time % 17 >= 14) {
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

  @Test
  public void readWithoutFilterTest() throws IOException, FileNodeManagerException {

    EngineQueryRouter engineExecutor = new EngineQueryRouter();
    QueryExpression queryExpression = QueryExpression.create();
    queryExpression.addSelectedPath(new Path(Constant.d0s0));
    queryExpression.addSelectedPath(new Path(Constant.d0s1));
    queryExpression.addSelectedPath(new Path(Constant.d0s2));
    queryExpression.addSelectedPath(new Path(Constant.d0s3));
    queryExpression.addSelectedPath(new Path(Constant.d0s4));
    queryExpression.addSelectedPath(new Path(Constant.d1s0));
    queryExpression.addSelectedPath(new Path(Constant.d1s1));
    queryExpression.setExpression(null);

    QueryDataSet queryDataSet = engineExecutor.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      // System.out.println("===" + rowRecord.toString());
      cnt++;
    }
    assertEquals(1000, cnt);

    QueryTokenManager.getInstance().endQueryForCurrentRequestThread();
  }

  @Test
  public void readWithTimeFilterTest() throws IOException, FileNodeManagerException {
    EngineQueryRouter engineExecutor = new EngineQueryRouter();
    QueryExpression queryExpression = QueryExpression.create();
    queryExpression.addSelectedPath(new Path(Constant.d0s0));
    queryExpression.addSelectedPath(new Path(Constant.d1s0));
    queryExpression.addSelectedPath(new Path(Constant.d1s1));

    GlobalTimeExpression globalTimeExpression = new GlobalTimeExpression(TimeFilter.gtEq(800L));
    queryExpression.setExpression(globalTimeExpression);
    QueryDataSet queryDataSet = engineExecutor.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      String value = rowRecord.getFields().get(0).getStringValue();
      long time = rowRecord.getTimestamp();
      // System.out.println(time + "===" + rowRecord.toString());
      assertEquals("" + time % 17, value);
      cnt++;
    }
    assertEquals(350, cnt);

    QueryTokenManager.getInstance().endQueryForCurrentRequestThread();
  }

  @Test
  public void readWithValueFilterTest() throws IOException, FileNodeManagerException {
    // select * from root where root.vehicle.d0.s0 >=14
    EngineQueryRouter engineExecutor = new EngineQueryRouter();
    QueryExpression queryExpression = QueryExpression.create();
    queryExpression.addSelectedPath(new Path(Constant.d0s0));
    queryExpression.addSelectedPath(new Path(Constant.d0s1));
    queryExpression.addSelectedPath(new Path(Constant.d0s2));
    queryExpression.addSelectedPath(new Path(Constant.d0s3));
    queryExpression.addSelectedPath(new Path(Constant.d0s4));
    queryExpression.addSelectedPath(new Path(Constant.d1s0));
    queryExpression.addSelectedPath(new Path(Constant.d1s1));

    Path queryPath = new Path(Constant.d0s0);
    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(queryPath,
        ValueFilter.gtEq(14));
    queryExpression.setExpression(singleSeriesExpression);

    QueryDataSet queryDataSet = engineExecutor.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      // System.out.println("readWithValueFilterTest===" + rowRecord.toString());
      cnt++;
    }
    assertEquals(count, cnt);

    QueryTokenManager.getInstance().endQueryForCurrentRequestThread();
  }

}
