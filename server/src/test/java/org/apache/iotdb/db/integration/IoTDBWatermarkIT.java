/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.integration;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBWatermarkIT {

  private static IoTDB daemon;

  @BeforeClass
  public static void setUp() throws Exception {
//    EnvironmentUtils.closeStatMonitor();
//    daemon = IoTDB.getInstance();
//    daemon.active();
//    EnvironmentUtils.envSetUp();
//    Thread.sleep(5000);
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
//    daemon.stop();
//    EnvironmentUtils.cleanEnv();
  }

  private static void insertData()
      throws ClassNotFoundException, SQLException, InterruptedException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();

      for (String sql : Constant.create_sql) {
        statement.execute(sql);
      }

      // s0 INT32, s1 INT64, s2 FLOAT
      for (int time = 1; time < 1000; time++) {
        String sql = String
            .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 50);
        statement.execute(sql);
        sql = String
            .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 50);
        statement.execute(sql);
        sql = String
            .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 50);
        statement.execute(sql);
      }

      statement.execute("SET STORAGE GROUP TO root.vehicle_wm");
      statement
          .execute("CREATE TIMESERIES root.vehicle_wm.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement
          .execute("CREATE TIMESERIES root.vehicle_wm.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement
          .execute("CREATE TIMESERIES root.vehicle_wm.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE");

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
  public void EncodeAndSaveTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();

      statement.execute("GRANT DATA_AUTHORITY to root");
      boolean hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle.d0");
//      boolean hasResultSet = statement.execute("select * from root");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(Constant.TIMESTAMP_STR) + "," + resultSet.getString(Constant.d0s0)
                + "," + resultSet.getString(Constant.d0s1)
                + "," + resultSet.getString(Constant.d0s2);
        System.out.println(ans);
        cnt++;
        boolean res = statement.execute("insert into root.vehicle_wm.d0(timestamp,s0,s1,s2) values(" + ans + ")");
        System.out.println(res);
      }
      System.out.println("cnt: " + cnt);

      statement.execute("REVOKE DATA_AUTHORITY from root");
      hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle_wm.d0");
      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(Constant.TIMESTAMP_STR)
                + "," + resultSet.getString("root.vehicle_wm.d0.s1")
                + "," + resultSet.getString("root.vehicle_wm.d0.s1")
                + "," + resultSet.getString("root.vehicle_wm.d0.s1");
        System.out.println(ans);
        cnt++;
      }
      System.out.println("cnt: " + cnt);

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
}
