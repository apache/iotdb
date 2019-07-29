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
package org.apache.iotdb.db.tools;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.integration.Constant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.thrift.EncodingUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBWatermarkTest {

  private static IoTDB daemon;
  private static String filePath = "watermarked_query_res.csv";
  private static PrintWriter writer;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Thread.sleep(5000);
    insertData();
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    writer = new PrintWriter(file);
    writer.println("time,root.vehicle.d0.s0,root.vehicle.d0.s1,root.vehicle.d0.s2");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData()
      throws ClassNotFoundException, SQLException, InterruptedException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();

      String[] create_sql = new String[]{"SET STORAGE GROUP TO root.vehicle",
          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE"
      };
      for (String sql : create_sql) {
        statement.execute(sql);
      }
      for (int time = 1; time < 1000; time++) {
        String sql = String
            .format("insert into root.vehicle.d0(timestamp,s0,s1,s2) values(%s,%s,%s,%s)", time,
                time % 50, time % 50, time % 50);
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
  public void EncodeAndDecodeTest() throws IOException, ClassNotFoundException, SQLException {
    // Watermark Encoding
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      statement.execute("GRANT WATERMARK_EMBEDDING to root");
      boolean hasResultSet = statement.execute("select s0,s1,s2 from root.vehicle.d0");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans =
            resultSet.getString(Constant.TIMESTAMP_STR)
                + "," + resultSet.getString(Constant.d0s0)
                + "," + resultSet.getString(Constant.d0s1)
                + "," + resultSet.getString(Constant.d0s2);
        writer.println(ans);
      }
      writer.close();
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }

    // Watermark Decoding
    String secretKey = "MLH8O7T*Y8HO*&";
    String watermarkBitString = "11001010010101";
    int mark_rate = 5;
    int max_right_bit = 5;
    int columnIndex = 1; // TODO temporarily one column
    int[] trueNums = new int[watermarkBitString.length()];
    int[] falseNums = new int[watermarkBitString.length()];
    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    reader.readLine(); // header
    String line;
    while ((line = reader.readLine()) != null) {
      String[] items = line.split(",");
      long timestamp = Long.parseLong(items[0]);
      double value = Double.parseDouble(items[columnIndex]); //TODO null
      if (GroupedLSBWatermarkEncoder.hashMod(String.format("%s%d", secretKey, timestamp), mark_rate)
          == 0) {
        int targetBitPosition = GroupedLSBWatermarkEncoder
            .hashMod(String.format("%s%d%s", secretKey, timestamp, secretKey),
                max_right_bit);
        int groupId = GroupedLSBWatermarkEncoder
            .hashMod(String.format("%d%s", timestamp, secretKey), watermarkBitString.length());

        int integerPart = (int) value;
        boolean isTrue = EncodingUtils.testBit(integerPart, targetBitPosition);
        if (isTrue) {
          trueNums[groupId] += 1;
        } else {
          falseNums[groupId] += 1;
        }
      }
    }
    reader.close();

    int cnt = 0;
    int hit_cnt = 0;
    for (int i = 0; i < watermarkBitString.length(); i++) {
      int res = trueNums[i] - falseNums[i];
      if (res > 0 && watermarkBitString.charAt(i) == '1') {
        hit_cnt += 1;
      } else if (res < 0 && watermarkBitString.charAt(i) == '0') {
        hit_cnt += 1;
      }
      if (res != 0) {
        cnt += 1;
      }
    }

    double alpha = 0.007; // significance level
    int b = compare(cnt, alpha);
    System.out.println("b=" + b);
    if (hit_cnt >= b) {
      System.out.println("watermarked");
    } else {
      System.out.println("not watermarked");
    }
  }

  private int compare(int l, double alpha) {
    int b = l;
    double sum = 1;
    double thrs = alpha;
    for (int i = 0; i < l; i++) {
      thrs *= 2;
    }
    while (sum < thrs) {
      b -= 1;
      sum += C(l, b);
    }
    return b + 1;
  }

  private int C(int n, int m) { // TODO big number
    System.out.println("n=" + n + ",m=" + m);
    int res1 = 1;
    for (int i = n; i > m; i--) {
      res1 *= i;
    }
    int res2 = 1;
    for (int i = 2; i <= n - m; i++) {
      res2 *= i;
    }
    int res = res1 / res2;
    System.out.println("res=" + res);
    return res;
  }

  private int cal(int n) {
    int res = n;
    for (int i = n - 1; i >= 1; i--) {
      res *= i;
    }
    return res;
  }
}
