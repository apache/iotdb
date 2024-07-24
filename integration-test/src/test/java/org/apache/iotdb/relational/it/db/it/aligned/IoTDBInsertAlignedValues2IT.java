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
package org.apache.iotdb.relational.it.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertAlignedValues2IT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setMaxNumberOfPointsInPage(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertAlignedWithEmptyPage() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database lz");
      statement.execute("use \"lz\"");
      statement.execute(
          "create table dev (id1 string id, s1 int32 measurement, s2 int32 measurement, s3 int32 measurement)");
      for (int i = 0; i < 100; i++) {
        if (i == 99) {
          statement.addBatch(
              "insert into dev(id1,time,s1,s3) values("
                  + "'GPS''"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          statement.addBatch(
              "insert into dev(id1, time,s1,s2) values("
                  + "'GPS'"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        }
      }
      statement.executeBatch();

      statement.execute("flush");
      int rowCount = 0;
      try (ResultSet resultSet = statement.executeQuery("select time, s3 from dev")) {
        while (resultSet.next()) {
          assertEquals(99, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(1, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select time, s2 from dev")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(99, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select time, s1 from dev")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute(
          "create table sg (id1 string id, s1 string measurement, s2 string measurement)");

      statement.execute("insert into sg(id1, time, s1, s2) aligned values('d1', 1,'aa','bb')");
      statement.execute("insert into sg(id1, time, s1, s2) aligned values('d1', 1,'aa','bb')");
      statement.execute("insert into sg(id1, time, s1, s2) aligned values('d2', 1,'aa','bb')");
      statement.execute("flush");
      statement.execute("insert into sg(id1, time, s1, s2) aligned values('d1', 1,'aa','bb')");
    }
  }

  @Test
  public void testInsertComplexAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database sg");
      statement.addBatch("use database \"test\"");
      statement.addBatch(
          "create table sg (id1 string id, s1 int32 measurement, s2 int32 measurement)");
      statement.addBatch("insert into sg(id1, time, s1) values('id1', 3,1)");
      statement.addBatch("insert into sg(id1, time, s1) values('id1', 3,1)");
      statement.addBatch("insert into sg(id1, time, s1) values('id1', 1,1)");
      statement.addBatch("insert into sg(id1, time, s1) values('id1', 2,1)");
      statement.addBatch("insert into sg(id1, time, s2) values('id1', 2,2)");
      statement.addBatch("insert into sg(id1, time, s2) values('id1', 1,2)");
      statement.addBatch("insert into sg(id1, time, s2) values('id1', 3,2)");
      statement.addBatch("insert into sg(id1, time, s3) values('id1', 1,3)");
      statement.addBatch("insert into sg(id1, time, s3) values('id1', 3,3)");
      statement.executeBatch();

      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from sg")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from sg")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }
    }
  }
}
