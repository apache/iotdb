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
package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import java.util.StringJoiner;

import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
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

  private static final String INSERT_TEMPLATE_SQL = "insert into %s(id1,time,%s) values(%s,%d,%s)";
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
    sqls.add("CREATE DATABASE test");
    StringJoiner createTableSql =
        new StringJoiner(",", "CREATE TABLE vehicle (id1 string id, ", ")");
    for (int i = 0; i < 10; i++) {
      createTableSql.add("s" + i + "f" + " FLOAT measurement");
      createTableSql.add("s" + i + "d" + " DOUBLE measurement");
    }
    sqls.add("USE \"test\"");
    sqls.add(createTableSql.toString());
    for (int i = 0; i < 10; i++) {
      sqls.add(
          String.format(INSERT_TEMPLATE_SQL, "vehicle", "s" + i + "f", "\'fd\'", TIMESTAMP, VALUE));
      sqls.add(
          String.format(INSERT_TEMPLATE_SQL, "vehicle", "s" + i + "d", "\'fd\'", TIMESTAMP, VALUE));
    }
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  @Ignore
  // CREATE TIMESERIES root.vehicle.%s.%s WITH DATATYPE=%s, ENCODING=%s, 'MAX_POINT_NUMBER'='%d'
  // Should support 'MAX_POINT_NUMBER'='%d'
  public void selectAllSQLTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection(TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");
      int cnt;
      try (ResultSet resultSet = statement.executeQuery("select * from vehicle")) {
        assertNotNull(resultSet);
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(TIMESTAMP, resultSet.getTimestamp("time").getTime());
          for (int i = 0; i < 10; i++) {
            assertEquals(
                MathUtils.roundWithGivenPrecision(Float.parseFloat(VALUE), i),
                resultSet.getFloat("s" + i + "f"),
                DELTA_FLOAT);
            assertEquals(
                MathUtils.roundWithGivenPrecision(Double.parseDouble(VALUE), i),
                resultSet.getDouble("s" + i + "d"),
                DELTA_DOUBLE);
          }
          cnt++;
        }
        assertEquals(1, cnt);
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select * from vehicle")) {
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(TIMESTAMP, resultSet.getTimestamp("time").getTime());
          for (int i = 0; i < 10; i++) {
            BigDecimal b = new BigDecimal(VALUE);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).floatValue(),
                resultSet.getFloat("s" + i + "f"),
                DELTA_FLOAT);
            assertEquals(
                b.setScale(i, RoundingMode.HALF_UP).doubleValue(),
                resultSet.getDouble("s" + i + "d"),
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
