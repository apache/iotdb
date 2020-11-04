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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBDeleteTimeseriesIT {

  private long memtableSizeThreshold;
  private CompactionStrategy tsFileManagementStrategy;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    memtableSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(16);
    tsFileManagementStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getCompactionStrategy();
    IoTDBDescriptor.getInstance().getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(memtableSizeThreshold);
    IoTDBDescriptor.getInstance().getConfig()
        .setCompactionStrategy(tsFileManagementStrategy);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void deleteTimeseriesAndCreateDifferentTypeTest() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String[] retArray = new String[]{
        "1,1,",
        "2,1.1,"
    };
    int cnt = 0;

    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");
      boolean hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
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
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,1.1)");
      statement.execute("FLUSH");

      hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
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

    EnvironmentUtils.restartDaemon();

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {
      boolean hasResult = statement.execute("SELECT * FROM root");
      Assert.assertTrue(hasResult);
    }
  }

  @Test
  public void deleteTimeseriesAndCreateSameTypeTest() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String[] retArray = new String[]{
        "1,1,",
        "2,5,"
    };
    int cnt = 0;

    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");
      boolean hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
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
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,5)");
      statement.execute("FLUSH");

      hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
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

    EnvironmentUtils.restartDaemon();

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      boolean hasResult = statement.execute("SELECT * FROM root");
      Assert.assertTrue(hasResult);
    }
  }
}
