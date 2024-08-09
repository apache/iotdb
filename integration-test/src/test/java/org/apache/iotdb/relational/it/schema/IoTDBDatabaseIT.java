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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showDBColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDatabaseIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testManageDatabase() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      // create
      statement.execute("create database test");

      // create duplicated database without IF NOT EXISTS
      try {
        statement.execute("create database test");
        fail("create database test shouldn't succeed because test already exists");
      } catch (SQLException e) {
        assertEquals("501: Database test already exists", e.getMessage());
      }

      // create duplicated database with IF NOT EXISTS
      statement.execute("create database IF NOT EXISTS test");

      String[] databaseNames = new String[] {"test"};
      int[] schemaReplicaFactors = new int[] {1};
      int[] dataReplicaFactors = new int[] {1};
      int[] timePartitionInterval = new int[] {604800000};

      // show
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(2));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(4));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      // use
      statement.execute("use test");

      // use nonexistent database
      try {
        statement.execute("use test1");
        fail("use test1 shouldn't succeed because test1 doesn't exist");
      } catch (SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      // drop
      statement.execute("drop database test");
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        assertFalse(resultSet.next());
      }

      // drop nonexistent database without IF EXISTS
      try {
        statement.execute("drop database test");
        fail("drop database test shouldn't succeed because test doesn't exist");
      } catch (SQLException e) {
        assertEquals("500: Database test doesn't exist", e.getMessage());
      }

      // drop nonexistent database with IF EXISTS
      statement.execute("drop database IF EXISTS test");

      // create with strange name
      try {
        statement.execute("create database 1test");
        fail(
            "create database 1test shouldn't succeed because 1test is not a legal identifier; identifiers must not start with a digit; surround the identifier with double quotes");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }

      statement.execute("create database \"1test\"");
      statement.execute("use \"1test\"");
      statement.execute("drop database \"1test\"");

      try {
        statement.execute("create database 1");
        fail("create database 1 shouldn't succeed because 1 is not a legal identifier");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }
      //
      //      // TODO fix it, should succeed
      //      statement.execute("create database \"1\"");
      //      statement.execute("use \"1\"");
      //      statement.execute("drop database \"1\"");
      //
      try {
        statement.execute("create database a.b");
        fail("create database a.b shouldn't succeed because a.b is not a legal identifier");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '.'"));
      }
      //
      //      // TODO fix it, should succeed
      //      statement.execute("create database \"a.b\"");
      //      statement.execute("use \"a.b\"");
      //      statement.execute("drop database \"a.b\"");

      // Test length limitation
      statement.execute(
          "create database thisDatabaseLengthIsPreciselySixtyFourThusItCanBeNormallyCreated");

      try {
        statement.execute(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated");
        fail(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated shouldn't succeed because it's length has exceeded 64");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("the length of database name shall not exceed 64"));
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
