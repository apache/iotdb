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

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertAlignedValues3IT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setMaxNumberOfPointsInPage(4);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertAlignedWithEmptyPage2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database lz");
      statement.execute("use \"lz\"");
      statement.execute(
          "create table dev (id1 string id, s1 int32 measurement, s2 int32 measurement, s3 int32 measurement)");
      for (int i = 0; i < 100; i++) {
        if (i >= 49) {
          statement.addBatch(
              "insert into dev(id1,time,s1,s2,s3) values("
                  + "GPS"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          statement.addBatch(
              "insert into root.lz.dev.GPS(id1,time,s1,s2) values("
                  + "GPS"
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
      try (ResultSet resultSet = statement.executeQuery("select s3 from dev")) {
        while (resultSet.next()) {
          assertEquals(rowCount + 49, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(51, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select s2 from dev")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from dev")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }
    }
  }
}
