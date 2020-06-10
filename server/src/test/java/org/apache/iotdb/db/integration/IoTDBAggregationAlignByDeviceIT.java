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

import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBAggregationAlignByDeviceIT {

  private static final String[] dataSet = new String[]{
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(1, 1.0, 1.1, 1.2)",
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(2, 2.0, 2.1, 2.2)",
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(3, 3.0, 3.1, 3.2)",
      "flush",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(1, 1.0, 1.1, 1.2)",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(2, 2.0, 2.1, 2.2)",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(3, 3.0, 3.1, 3.2)",
      "flush",
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(1, 11.0, 11.1, 11.2)",
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(2, 12.0, 12.1, 12.2)",
      "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(3, 13.0, 13.1, 13.2)",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(1, 11.0, 11.1, 11.2)",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(2, 12.0, 12.1, 12.2)",
      "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(3, 13.0, 13.1, 13.2)",
  };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws SQLException {
    String[] retArray = new String[]{"root.sg1.d1,3,3,3", "root.sg1.d2,3,3,3",};
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement
          .execute("SELECT count(*) from root align by device");

      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString("Device") + "," + resultSet.getString(count("s1"))
                  + "," + resultSet.getString(count("s2")) + "," + resultSet
                  .getString(count("s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
