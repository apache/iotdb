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
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCreateStorageGroupIT extends AbstractSchemaIT {

  public IoTDBCreateStorageGroupIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  /** The test creates three databases */
  @Test
  public void testCreateStorageGroup() throws Exception {
    String[] storageGroups = {"root.sg1", "root.sg2", "root.sg3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String storageGroup : storageGroups) {
        statement.execute(String.format("create database %s", storageGroup));
      }

      // ensure that current StorageGroup in cache is right.
      createStorageGroupTool(statement, storageGroups);
    }
    // todo test restart
    //    EnvironmentUtils.stopDaemon();
    //    setUp();
    //
    //    // ensure StorageGroup in cache is right after recovering.
    //    createStorageGroupTool(storageGroups);
  }

  private void createStorageGroupTool(Statement statement, String[] storageGroups)
      throws SQLException {

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        String storageGroupPath = resultSet.getString(ColumnHeaderConstant.DATABASE);
        resultList.add(storageGroupPath);
      }
    }
    Assert.assertEquals(3, resultList.size());

    resultList = resultList.stream().sorted().collect(Collectors.toList());

    Assert.assertEquals(storageGroups[0], resultList.get(0));
    Assert.assertEquals(storageGroups[1], resultList.get(1));
    Assert.assertEquals(storageGroups[2], resultList.get(2));
  }

  /** Test creating a database that path is an existence database */
  @Test
  public void testCreateExistStorageGroup1() throws Exception {
    String storageGroup = "root.sg";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE %s", storageGroup));

      try {
        statement.execute(String.format("create database %s", storageGroup));
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
                + ": root.sg has already been created as database",
            e.getMessage());
      }
    }
  }

  /** Test the parent node has been set as a database */
  @Test
  public void testCreateExistStorageGroup2() throws Exception {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database root.sg");

      try {
        statement.execute("create database root.sg.`device`");
        fail();
      } catch (SQLException e) {
        Assert.assertEquals(
            TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
                + ": root.sg has already been created as database",
            e.getMessage());
      }
    }
  }
}
