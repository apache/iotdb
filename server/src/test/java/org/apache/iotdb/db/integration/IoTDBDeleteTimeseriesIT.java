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
package org.apache.iotdb.db.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBDeleteTimeseriesIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeleteTimeseries() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String[] retArray = new String[]{
        "1,2,",
        "1,2.1,"
    };
    int cnt = 0;

    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s3 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s2,s3) VALUES(1,2,3)");
      boolean hasResult = statement.execute("SELECT s2 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s2");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=FLOAT, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s2,s3) VALUES(1,2.1,3)");
      hasResult = statement.execute("SELECT s2 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }
  }
}
