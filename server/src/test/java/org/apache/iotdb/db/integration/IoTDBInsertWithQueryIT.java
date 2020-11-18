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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBInsertWithQueryIT {


  @Before
  public void setUp() {

    EnvironmentUtils.closeStatMonitor();

    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void insertWithQueryTest() throws ClassNotFoundException {
    // insert
    insertData(0, 1000);

    // select
    selectAndCount(1000);

    // insert
    insertData(1000, 2000);

    // select
    selectAndCount(2000);
  }

  @Test
  public void insertWithQueryMultiThreadTest() throws ClassNotFoundException, InterruptedException {
    // insert
    insertData(0, 1000);

    selectWithMultiThread(1000);

    // insert
    insertData(1000, 2000);

    // select
    selectWithMultiThread(2000);
  }

  @Test
  public void insertWithQueryTestUnsequence() throws ClassNotFoundException {
    // insert
    insertData(0, 1000);

    // select
    selectAndCount(1000);

    // insert
    insertData(500, 1500);

    // select
    selectAndCount(1500);

    // insert
    insertData(2000, 3000);

    // select
    selectAndCount(2500);
  }

  @Test
  public void insertWithQueryMultiThreadTestUnsequence()
      throws ClassNotFoundException, InterruptedException {
    // insert
    insertData(0, 1000);

    selectWithMultiThread(1000);

    // insert
    insertData(500, 1500);

    // select
    selectWithMultiThread(1500);

    // insert
    insertData(2000, 3000);

    // select
    selectWithMultiThread(2500);
  }

  private void selectWithMultiThread(int res) throws InterruptedException {
    List<Thread> queryThreadList = new ArrayList<>();

    // select with multi thread
    for (int i = 0; i < 5; i++) {
      Thread cur = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            selectAndCount(res);
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
          }
        }
      });

      queryThreadList.add(cur);
      cur.start();
    }

    for (Thread thread : queryThreadList) {
      thread.join();
    }
  }

  private void insertData(int start, int end) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // insert of data time range : start-end into fans
      for (int time = start; time < end; time++) {
        String sql = String
            .format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql = String
            .format("insert into root.fans.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  // "select * from root.vehicle" : test select wild data
  private void selectAndCount(int res) throws ClassNotFoundException {
    String selectSql = "select * from root";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR) + "," + resultSet
                  .getString("root.fans.d0.s0")
                  + "," + resultSet.getString("root.fans.d0.s1");
          cnt++;
        }
        assertEquals(res, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
