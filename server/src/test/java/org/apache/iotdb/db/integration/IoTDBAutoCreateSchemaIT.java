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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the IoTDB server should be
 * defined as integration test.
 */
public class IoTDBAutoCreateSchemaIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /**
   * create timeseries without setting storage group
   */
  @Test
  public void createTimeseriesTest() throws ClassNotFoundException {
    String[] sqls = {"CREATE TIMESERIES root.sg0.d1.s2 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123)",
    };
    executeSQL(sqls);
  }

  /**
   * insert data when storage group has been set but timeseries hasn't been created
   */
  @Test
  public void insertTest1() throws ClassNotFoundException {
    String[] sqls = {"SET STORAGE GROUP TO root.sg0",
        "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123.123)",
        "INSERT INTO root.sg0.d1(timestamp,s3) values(1,\"abc\")",
    };
    executeSQL(sqls);
  }

  /**
   * insert data when storage group hasn't been set and timeseries hasn't been created
   */
  @Test
  public void insertTest2() throws ClassNotFoundException {
    String[] sqls = {"INSERT INTO root.sg0.d1(timestamp,s2) values(1,\"abc\")",
        "INSERT INTO root.sg0.d2(timestamp,s3) values(1,123.123)",
        "INSERT INTO root.sg0.d2(timestamp,s4) values(1,123456)",
    };
    executeSQL(sqls);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
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

          statement.execute(sql);
          if (sql.split(" ")[0].equals("SELECT")) {
            ResultSet resultSet = statement.getResultSet();
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
}
