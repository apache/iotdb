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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

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
public class IoTDBCreateDatabaseIT extends AbstractSchemaIT {

  public IoTDBCreateDatabaseIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  /** The test creates three databases */
  @Test
  public void testCreateDatabase() throws Exception {
    final String[] databases = {"root.sg1", "root.sg2", "root.sg3"};
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (final String database : databases) {
        statement.execute(String.format("create database %s", database));
      }

      // ensure that current Database in cache is right.
      createDatabaseTool(statement, databases);
    }
    // todo test restart
    //    EnvironmentUtils.stopDaemon();
    //    setUp();
    //
    //    // ensure Database in cache is right after recovering.
    //    createDatabaseTool(Databases);
  }

  private void createDatabaseTool(final Statement statement, final String[] Databases)
      throws SQLException {

    List<String> resultList = new ArrayList<>();
    try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES root.sg*")) {
      while (resultSet.next()) {
        final String databasePath = resultSet.getString(ColumnHeaderConstant.DATABASE);
        resultList.add(databasePath);
      }
    }
    Assert.assertEquals(3, resultList.size());

    resultList = resultList.stream().sorted().collect(Collectors.toList());

    Assert.assertEquals(Databases[0], resultList.get(0));
    Assert.assertEquals(Databases[1], resultList.get(1));
    Assert.assertEquals(Databases[2], resultList.get(2));
  }

  /** Test creating a database that path is an existence database */
  @Test
  public void testCreateExistDatabase1() throws Exception {
    final String database = "root.db";

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE %s", database));

      try {
        statement.execute(String.format("create database %s", database));
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
                + ": root.db has already been created as database",
            e.getMessage());
      }
    }
  }

  /** Test the parent node has been set as a database */
  @Test
  public void testCreateExistDatabase2() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root.db");

      try {
        statement.execute("create database root.db.`device`");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
                + ": root.db has already been created as database",
            e.getMessage());
      }
    }
  }

  /** Test creating a database exceeding 64 letters */
  @Test
  public void testCreateTooLongDatabase() throws Exception {
    final String database =
        "root.thisDatabaseNameHasExceeded64BySevenAndCanNotBeSuccessfullyCreated";

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      try {
        statement.execute(String.format("create database %s", database));
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            TSStatusCode.ILLEGAL_PATH.getStatusCode()
                + ": root.thisDatabaseNameHasExceeded64BySevenAndCanNotBeSuccessfullyCreated is not a legal path, because the length of database name shall not exceed 64",
            e.getMessage());
      }
    }
  }

  /** Test creating a "root" database */
  @Test
  public void testCreateRootDatabase() throws Exception {
    final String database = "root";

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      try {
        statement.execute(String.format("create database %s", database));
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            TSStatusCode.ILLEGAL_PATH.getStatusCode()
                + ": root is not a legal path, because the database name in tree model must start with 'root.'.",
            e.getMessage());
      }
    }
  }
}
