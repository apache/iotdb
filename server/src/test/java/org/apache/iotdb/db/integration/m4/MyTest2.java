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
package org.apache.iotdb.db.integration.m4;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

public class MyTest2 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static CompactionStrategy originalCompactionStrategy;
  private static int originalAvgSeriesPointNumberThreshold;
  private static long originalSeqTsFileSize;
  private static long originalUnSeqTsFileSize;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    originalEnableCPV = config.isEnableCPV();
    originalCompactionStrategy = config.getCompactionStrategy();
    originalAvgSeriesPointNumberThreshold = config.getAvgSeriesPointNumberThreshold();
    originalSeqTsFileSize = config.getSeqTsFileSize();
    originalUnSeqTsFileSize = config.getUnSeqTsFileSize();

    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    config.setSeqTsFileSize(1024 * 1024 * 1024); // 1G
    config.setUnSeqTsFileSize(1024 * 1024 * 1024); // 1G
    config.setAvgSeriesPointNumberThreshold(4); // this step cannot be omitted

    config.setEnableCPV(
        true); // this test cannot be false, as the expected answer for bottomTime and topTime can
    // be different
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setAvgSeriesPointNumberThreshold(originalAvgSeriesPointNumberThreshold);
    config.setEnableCPV(originalEnableCPV);
    config.setSeqTsFileSize(originalSeqTsFileSize);
    config.setUnSeqTsFileSize(originalUnSeqTsFileSize);
  }

  @Test
  public void test1() {
    prepareData1();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,27,45,20,30,9[33],40[30]",
          "50,52,54,8,18,8[52],18[54]",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152085323-321ecd70-1253-494f-81ab-fe227d1f5351.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));

      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2() {
    prepareData2();

    String[] res =
        new String[] {
          "0,1,20,5,5,5[1],5[1]",
          "25,30,40,5,5,5[30],5[30]",
          "50,55,72,5,5,5[65],5[65]",
          "75,80,90,5,5,5[80],5[80]"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152085361-571f64dc-0c32-4f70-9481-bc30e4f6f78a.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 5));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 66, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 67, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 5));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 82, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 85, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 5));

      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3() {
    prepareData3();

    String[] res =
        new String[] {
          "0,1,20,5,15,5[1],15[2]",
          "25,30,40,5,15,5[30],15[40]",
          "50,55,72,5,15,5[65],15[66]",
          "75,80,90,5,15,5[80],15[82]"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152085386-ebe57e83-cb49-49e8-b8f8-b80719547c42.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 15));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 66, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 67, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 15));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 82, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 85, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 15));

      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test4() {
    prepareData4();

    String[] res = new String[] {"0,1,10,5,18,3[9],25[6]", "50,60,60,1,1,1[60],1[60]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),50ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData4() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152088562-830e3272-749a-493a-83ca-1279e66ab145.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 25));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 24));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 17));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 22));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 18));

      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test5() {
    prepareData5();

    String[] res = new String[] {"0,1,10,5,18,3[9],25[6]", "50,60,60,1,1,1[60],1[60]"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),50ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData5() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152088820-49351c49-9da2-43dd-8da1-2940ae81ae9d.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 3, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 4, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 6, 25));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 7, 24));

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 17));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 22));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 9, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 18));

      statement.execute("flush");

      statement.execute("delete from root.vehicle.d0.s0 where time>=7 and time<=8");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
