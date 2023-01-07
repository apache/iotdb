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
package org.apache.iotdb.db.it;

import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBFloatPrecisionIT {

  private static final String CREATE_TEMPLATE_SQL =
      "CREATE TIMESERIES root.vehicle.%s.%s WITH DATATYPE=%s, ENCODING=%s, 'MAX_POINT_NUMBER'='%d'";
  private static final String INSERT_TEMPLATE_SQL =
      "insert into root.vehicle.%s(timestamp,%s) values(%d,%s)";
  private static List<String> sqls = new ArrayList<>();
  private static final int TIMESTAMP = 10;
  private static final String VALUE = "1.2345678901";
  private static final float DELTA_FLOAT = 0.0000001f;
  private static final double DELTA_DOUBLE = 0.0000001d;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    initCreateSQLStatement();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void initCreateSQLStatement() {
    sqls.add("CREATE DATABASE root.vehicle.f0");
    sqls.add("CREATE DATABASE root.vehicle.d0");
    for (int i = 0; i < 10; i++) {
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "rle", "FLOAT", "RLE", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "2f", "FLOAT", "TS_2DIFF", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "rle", "DOUBLE", "RLE", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "2f", "DOUBLE", "TS_2DIFF", i));
    }
    for (int i = 0; i < 10; i++) {
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "rle", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "2f", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "rle", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "2f", TIMESTAMP, VALUE));
    }
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectAllSQLTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        assertNotNull(resultSet);
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(TIMESTAMP + "", resultSet.getString(TIMESTAMP_STR));
          for (int i = 0; i < 10; i++) {
            assertEquals(
                MathUtils.roundWithGivenPrecision(Float.parseFloat(VALUE), i),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "rle")),
                DELTA_FLOAT);
            assertEquals(
                MathUtils.roundWithGivenPrecision(Float.parseFloat(VALUE), i),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "2f")),
                DELTA_FLOAT);
            assertEquals(
                MathUtils.roundWithGivenPrecision(Double.parseDouble(VALUE), i),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "rle")),
                DELTA_DOUBLE);
            assertEquals(
                MathUtils.roundWithGivenPrecision(Double.parseDouble(VALUE), i),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "2f")),
                DELTA_DOUBLE);
          }
          cnt++;
        }
        assertEquals(1, cnt);
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(TIMESTAMP + "", resultSet.getString(TIMESTAMP_STR));
          for (int i = 0; i < 10; i++) {
            BigDecimal b = new BigDecimal(VALUE);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).floatValue(),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "rle")),
                DELTA_FLOAT);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).floatValue(),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "2f")),
                DELTA_FLOAT);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).doubleValue(),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "rle")),
                DELTA_DOUBLE);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).doubleValue(),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "2f")),
                DELTA_DOUBLE);
          }
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
