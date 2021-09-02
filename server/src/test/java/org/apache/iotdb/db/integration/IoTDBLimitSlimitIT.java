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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBDatabaseMetadata;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBLimitSlimitIT {

  private static String[] insertSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
      };

  @BeforeClass
  public static void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void Test() throws ClassNotFoundException {
    insertData();
    SelectTest();
  }

  public void SelectTest() throws ClassNotFoundException {
    String[] sqlS = {
      "SELECT s1 FROM root.vehicle.d0 WHERE time<200 limit 3",
      "1,1101,\n" + "2,40000,\n" + "50,50000,\n",
      "SELECT s0 FROM root.vehicle.d0 WHERE s1 > 190 limit 3",
      "1,101,\n" + "2,10000,\n" + "50,10000,\n",
      "SELECT s1,s2 FROM root.vehicle.d0 where s1>190 or s2<10.0 limit 3 offset 2",
      "3,null,3.33,\n" + "4,null,4.44,\n" + "50,50000,null,\n",
      "select * from root.vehicle.d0 slimit 1",
      "1,101,\n"
          + "2,10000,\n"
          + "50,10000,\n"
          + "100,99,\n"
          + "101,99,\n"
          + "102,80,\n"
          + "103,99,\n"
          + "104,90,\n"
          + "105,99,\n"
          + "106,99,\n"
          + "1000,22222,\n",
      "select * from root.vehicle.d0 slimit 1 soffset 2",
      "2,2.22,\n" + "3,3.33,\n" + "4,4.44,\n" + "102,10.0,\n" + "105,11.11,\n" + "1000,1000.11,\n",
      "select d0 from root.vehicle slimit 1 soffset 2",
      "2,2.22,\n" + "3,3.33,\n" + "4,4.44,\n" + "102,10.0,\n" + "105,11.11,\n" + "1000,1000.11,\n",
      "select * from root.vehicle.d0 where s1>190 or s2 < 10.0 limit 3 offset 1 slimit 1 soffset 2 ",
      "3,3.33,\n" + "4,4.44,\n" + "105,11.11,\n"
    };
    executeSQL(sqlS);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String result = "";
      Long now_start = 0L;
      boolean cmp = false;

      for (String sql : sqls) {
        //         System.out.println("----" + sql);
        if (cmp) {
          Assert.assertEquals(sql, result);
          cmp = false;
        } else if (sql.equals("SHOW TIMESERIES")) {
          DatabaseMetaData data = connection.getMetaData();
          result = ((IoTDBDatabaseMetadata) data).getMetadataInJson();
          cmp = true;
        } else {
          if (sql.contains("NOW()") && now_start == 0L) {
            now_start = System.currentTimeMillis();
          }

          statement.execute(sql);
          if (sql.split(" ")[0].equals("SELECT") | sql.split(" ")[0].equals("select")) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData metaData = resultSet.getMetaData();
              int count = metaData.getColumnCount();
              String[] column = new String[count];
              for (int i = 0; i < count; i++) {
                column[i] = metaData.getColumnName(i + 1);
              }
              result = "";
              while (resultSet.next()) {
                for (int i = 1; i <= count; i++) {
                  if (now_start > 0L && column[i - 1] == TestConstant.TIMESTAMP_STR) {
                    String timestr = resultSet.getString(i);
                    Long tn = Long.valueOf(timestr);
                    Long now = System.currentTimeMillis();
                    if (tn >= now_start && tn <= now) {
                      timestr = "NOW()";
                    }
                    result += timestr + ',';
                  } else {
                    result += resultSet.getString(i) + ',';
                  }
                }
                result += '\n';
              }
              cmp = true;
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
