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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.tools.watermark.WatermarkDetector;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBWatermarkTest {

  private static String filePath1 =
      TestConstant.BASE_OUTPUT_PATH.concat("watermarked_query_result.csv");
  private static String filePath2 =
      TestConstant.BASE_OUTPUT_PATH.concat("notWatermarked_query_result.csv");
  private static PrintWriter writer1;
  private static PrintWriter writer2;
  private static String secretKey = "ASDFGHJKL";
  private static String watermarkBitString = "10101000100";
  private static int embed_row_cycle = 5;
  private static int embed_lsb_num = 5;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    IoTDBDescriptor.getInstance().getConfig().setEnableWatermark(true); // default false
    IoTDBDescriptor.getInstance().getConfig().setWatermarkSecretKey(secretKey);
    IoTDBDescriptor.getInstance().getConfig().setWatermarkBitString(watermarkBitString);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setWatermarkMethod(
            String.format(
                "GroupBasedLSBMethod" + "(embed_row_cycle=%d,embed_lsb_num=%d)",
                embed_row_cycle, embed_lsb_num));

    EnvironmentUtils.envSetUp();
    insertData();

    File file1 = new File(filePath1);
    if (file1.exists()) {
      file1.delete();
    }
    writer1 = new PrintWriter(file1);
    writer1.println("time,root.vehicle.d0.s0,root.vehicle.d0.s1,root.vehicle.d0.s2");

    File file2 = new File(filePath2);
    if (file2.exists()) {
      file2.delete();
    }
    writer2 = new PrintWriter(file2);
    writer2.println("time,root.vehicle.d0.s0,root.vehicle.d0.s1,root.vehicle.d0.s2");
  }

  @After
  public void tearDown() throws Exception {
    File file1 = new File(filePath1);
    if (file1.exists()) {
      file1.delete();
    }
    File file2 = new File(filePath2);
    if (file2.exists()) {
      file2.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] create_sql =
          new String[] {
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE"
          };
      for (String sql : create_sql) {
        statement.execute(sql);
      }

      for (int time = 1; time < 1000; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0,s2) values(%s,%s,%s)",
                time, time % 50, time % 50, time % 50);
        statement.execute(sql);
        if (time % 10 == 0) {
          sql =
              String.format(
                  "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 50);
          statement.execute(sql);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void EncodeAndDecodeTest1()
      throws IOException, ClassNotFoundException, LogicalOperatorException {
    // Watermark Embedding
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("GRANT WATERMARK_EMBEDDING TO root");
      boolean hasResultSet = statement.execute("SELECT s0,s1,s2 FROM root.vehicle.d0");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s1)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s2);
          writer1.println(ans);
        }
        writer1.close();
      } finally {
        resultSet.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Watermark Detection
    double alpha = 0.1;
    int columnIndex = 1;
    boolean isWatermarked =
        WatermarkDetector.isWatermarked(
            filePath1,
            secretKey,
            watermarkBitString,
            embed_row_cycle,
            embed_lsb_num,
            alpha,
            columnIndex,
            "int");
    Assert.assertTrue(isWatermarked);
  }

  @Test
  public void EncodeAndDecodeTest2()
      throws IOException, ClassNotFoundException, LogicalOperatorException {
    // No Watermark Embedding
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("REVOKE WATERMARK_EMBEDDING FROM root");
      boolean hasResultSet = statement.execute("SELECT s0,s1,s2 FROM root.vehicle.d0");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s1)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s2);
          writer2.println(ans);
        }
        writer2.close();
      } finally {
        resultSet.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Watermark Detection
    double alpha = 0.1;
    int columnIndex = 1;
    boolean isWatermarked =
        WatermarkDetector.isWatermarked(
            filePath2,
            secretKey,
            watermarkBitString,
            embed_row_cycle,
            embed_lsb_num,
            alpha,
            columnIndex,
            "int");
    Assert.assertFalse(isWatermarked);
  }
}
