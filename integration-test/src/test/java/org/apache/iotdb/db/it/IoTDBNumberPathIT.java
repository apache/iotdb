/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBNumberPathIT {
  @BeforeClass
  public static void setUp() throws InterruptedException {
    EnvFactory.getEnv().initBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void test() throws ClassNotFoundException {
    String[] sqls = {"SET STORAGE GROUP TO root.123"};
    executeSQL(sqls);
    selectTest();
  }

  public void selectTest() {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,101)",
      "INSERT INTO root.123.456(timestamp,789) values(2,102)",
      "INSERT INTO root.123.456(timestamp,789) values(3,103)",
      "INSERT INTO root.123.456(timestamp,789) values(4,104)",
      "INSERT INTO root.123.456(timestamp,789) values(5,105)",
      "INSERT INTO root.123.456(timestamp,789) values(6,106)",
      "INSERT INTO root.123.456(timestamp,789) values(7,107)",
      "INSERT INTO root.123.456(timestamp,789) values(8,108)",
      "INSERT INTO root.123.456(timestamp,789) values(9,109)",
      "INSERT INTO root.123.456(timestamp,789) values(10,110)",
      "SELECT * FROM root.123.456 WHERE 789 < 104",
      "1,101,\n" + "2,102,\n" + "3,103,\n",
      "SELECT * FROM root.123.456 WHERE 789 > 105 and time < 8",
      "6,106,\n" + "7,107,\n",
      "SELECT * FROM root.123.456",
      "1,101,\n"
          + "2,102,\n"
          + "3,103,\n"
          + "4,104,\n"
          + "5,105,\n"
          + "6,106,\n"
          + "7,107,\n"
          + "8,108,\n"
          + "9,109,\n"
          + "10,110,\n",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  private void executeSQL(String[] sqls) {
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
