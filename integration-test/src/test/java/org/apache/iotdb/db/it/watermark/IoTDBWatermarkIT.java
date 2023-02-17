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
package org.apache.iotdb.db.it.watermark;

import org.apache.iotdb.db.tools.watermark.WatermarkDetector;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.itbase.constant.TestConstant.BASE_OUTPUT_PATH;
import static org.junit.Assert.fail;

@Ignore // TODO add it back when we support watermark in mpp mode
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBWatermarkIT {

  private static final String filePath1 = BASE_OUTPUT_PATH.concat("watermarked_query_result.csv");
  private static final String filePath2 =
      BASE_OUTPUT_PATH.concat(File.separator).concat("notWatermarked_query_result.csv");
  private static PrintWriter writer1;
  private static PrintWriter writer2;
  private static String secretKey = "ASDFGHJKL";
  private static String watermarkBitString = "10101000100";
  private static int embed_row_cycle = 5;
  private static int embed_lsb_num = 5;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableWatermark(true)
        .setWatermarkSecretKey(secretKey)
        .setWatermarkBitString(watermarkBitString)
        .setWatermarkMethod(
            String.format(
                "GroupBasedLSBMethod" + "(embed_row_cycle=%d,embed_lsb_num=%d)",
                embed_row_cycle, embed_lsb_num));

    EnvFactory.getEnv().initClusterEnvironment();

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
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] create_sql =
          new String[] {
            "CREATE DATABASE root.vehicle",
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
                time, time % 50, time % 50);
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
  public void EncodeAndDecodeTest1() {
    // Watermark Embedding
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("GRANT WATERMARK_EMBEDDING TO root");
      try (ResultSet resultSet = statement.executeQuery("SELECT s0,s1,s2 FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s0)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s1)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s2);
          writer1.println(ans);
        }
        writer1.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Watermark Detection
    double alpha = 0.1;
    int columnIndex = 1;
    try {
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void EncodeAndDecodeTest2() {
    // No Watermark Embedding
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("REVOKE WATERMARK_EMBEDDING FROM root");
      try (ResultSet resultSet = statement.executeQuery("SELECT s0,s1,s2 FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s0)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s1)
                  + ","
                  + resultSet.getString(TestConstant.d0 + "." + TestConstant.s2);
          writer2.println(ans);
        }
        writer2.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Watermark Detection
    double alpha = 0.1;
    int columnIndex = 1;
    try {
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
