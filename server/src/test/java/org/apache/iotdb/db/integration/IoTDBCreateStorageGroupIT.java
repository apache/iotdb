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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBCreateStorageGroupIT {
  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();

    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  /** The test creates three storage groups */
  @Test
  public void testCreateStorageGroup() throws Exception {
    String[] storageGroups = {"root.sg1", "root.sg2", "root.sg3"};

    for (String storageGroup : storageGroups) {
      statement.execute(String.format("create storage group %s", storageGroup));
    }

    // ensure that current StorageGroup in cache is right.
    createStorageGroupTool(storageGroups);

    statement.close();
    connection.close();
    EnvironmentUtils.stopDaemon();
    setUp();

    // ensure StorageGroup in cache is right after recovering.
    createStorageGroupTool(storageGroups);
  }

  private void createStorageGroupTool(String[] storageGroups) throws SQLException {
    boolean hasResult = statement.execute("show storage group");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String storageGroupPath = resultSet.getString("storage group");
        resultList.add(storageGroupPath);
      }
    }
    Assert.assertEquals(3, resultList.size());

    resultList = resultList.stream().sorted().collect(Collectors.toList());

    Assert.assertEquals(storageGroups[0], resultList.get(0));
    Assert.assertEquals(storageGroups[1], resultList.get(1));
    Assert.assertEquals(storageGroups[2], resultList.get(2));
  }

  /** Test creating a storage group that path is an existence storage group */
  @Test
  public void testCreateExistStorageGroup1() throws Exception {
    String storageGroup = "root.sg";

    statement.execute(String.format("set storage group to %s", storageGroup));

    try {
      statement.execute(String.format("create storage group %s", storageGroup));
    } catch (SQLException e) {
      Assert.assertEquals(e.getMessage(), "300: root.sg has already been set to storage group");
    }
  }

  /** Test the parent node has been set as a storage group */
  @Test
  public void testCreateExistStorageGroup2() throws Exception {

    statement.execute("create storage group root.sg");

    try {
      statement.execute("create storage group root.sg.device");
    } catch (SQLException e) {
      Assert.assertEquals(e.getMessage(), "300: root.sg has already been set to storage group");
    }
  }
}
