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

package org.apache.iotdb.db.it.udaf;

import org.apache.iotdb.db.it.aggregation.IoTDBAggregationAndUDAFTestBase;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFNormalQueryIT extends IoTDBAggregationAndUDAFTestBase {
  private static final double DELTA = 1E-6;

  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";

  @Test
  public void singleUDAFTest() {
    String[] retArray = new String[] {"2001,2001,2001,2001", "7500,7500,7500,7500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(s0),count_udaf(s1),count_udaf(s2),count_udaf(s3) "
                  + "FROM root.vehicle.d0 order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(countUDAF(d0s0))
                  + ","
                  + resultSet.getString(countUDAF(d0s1))
                  + ","
                  + resultSet.getString(countUDAF(d0s2))
                  + ","
                  + resultSet.getString(countUDAF(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void multipleUDAFTest() {
    double[][] retArray = {
      {1.4508E7, 7250.374812593702},
      {626750.0, 1250.9980039920158}
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(s0),avg_udaf(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        while (resultSet.next()) {
          double[] ans = new double[2];
          ans[0] = Double.parseDouble(resultSet.getString(sumUDAF(d0s0)));
          ans[1] = Double.parseDouble(resultSet.getString(avgUDAF(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void UDAFTypeMismatchErrorTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg_udaf(s3) FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: TEXT."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum_udaf(s3)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: TEXT."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg_udaf(s4)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum_udaf(s4)"
                    + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT avg_udaf(status) FROM root.ln.wf01.wt01")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "the data type of the input series (index: 0) is not valid. expected: [INT32, INT64, FLOAT, DOUBLE]. actual: BOOLEAN."));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
