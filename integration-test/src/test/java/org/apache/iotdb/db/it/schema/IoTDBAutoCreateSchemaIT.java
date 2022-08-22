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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAutoCreateSchemaIT {
  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();

    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterTest();
  }

  /** create timeseries without setting storage group */
  @Test
  public void createTimeseriesTest() throws ClassNotFoundException {
    String[] sqls = {
      "CREATE TIMESERIES root.sg0.d1.s2 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123)",
    };
    executeSQL(sqls);
  }

  /** insert data when storage group has been set but timeseries hasn't been created */
  @Test
  public void insertTest1() throws ClassNotFoundException {
    String[] sqls = {
      "SET STORAGE GROUP TO root.sg0",
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123.123)",
      "INSERT INTO root.sg0.d1(timestamp,s3) values(1,\"abc\")",
    };
    executeSQL(sqls);
  }

  /** insert data when storage group hasn't been set and timeseries hasn't been created */
  @Test
  public void insertTest2() throws ClassNotFoundException {
    String[] sqls = {
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,\"abc\")",
      "INSERT INTO root.sg0.d2(timestamp,s3) values(1,123.123)",
      "INSERT INTO root.sg0.d2(timestamp,s4) values(1,123456)",
    };
    executeSQL(sqls);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String result = "";
      Long now_start = 0L;
      boolean cmp = false;

      for (String sql : sqls) {
        if (cmp) {
          Assert.assertEquals(sql, result);
          cmp = false;
        } else if (sql.equals("SHOW TIMESERIES")) {
          DatabaseMetaData data = connection.getMetaData();
          result = data.toString();
          cmp = true;
        } else {
          if (sql.contains("NOW()") && now_start == 0L) {
            now_start = System.currentTimeMillis();
          }

          if (sql.split(" ")[0].equals("SELECT")) {
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            String[] column = new String[count];
            for (int i = 0; i < count; i++) {
              column[i] = metaData.getColumnName(i + 1);
            }
            result = "";
            while (resultSet.next()) {
              for (int i = 1; i <= count; i++) {
                if (now_start > 0L && column[i - 1].equals(TestConstant.TIMESTAMP_STR)) {
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * test if automatically creating a time series will cause the storage group with same name to
   * disappear
   */
  @Test
  public void testInsertAutoCreate2() throws Exception {
    String storageGroup = "root.sg2.a.b.c";
    String timeSeriesPrefix = "root.sg2.a.b";

    statement.execute(String.format("SET storage group TO %s", storageGroup));
    try {
      statement.execute(
          String.format("INSERT INTO %s(timestamp, c) values(123, \"aabb\")", timeSeriesPrefix));
    } catch (SQLException ignored) {
    }

    // ensure that current storage group in cache is right.
    InsertAutoCreate2Tool(storageGroup, timeSeriesPrefix);

    statement.close();
    connection.close();
    // todo restart test
    //    EnvironmentUtils.stopDaemon();
    //    setUp();
    //
    //    // ensure that storage group in cache is right after recovering.
    //    InsertAutoCreate2Tool(storageGroup, timeSeriesPrefix);
  }

  private void InsertAutoCreate2Tool(String storageGroup, String timeSeriesPrefix)
      throws SQLException {
    Set<String> resultList = new HashSet<>();
    try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
      while (resultSet.next()) {
        String str = resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES);
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(timeSeriesPrefix + "c"));

    resultList.clear();
    try (ResultSet resultSet = statement.executeQuery("show storage group")) {
      while (resultSet.next()) {
        resultList.add(resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP));
      }
    }
    Assert.assertTrue(resultList.contains(storageGroup));
  }
}
