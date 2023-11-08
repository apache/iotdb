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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(S1 INT32 encoding=PLAIN compressor=SNAPPY, S2 INT32 encoding=PLAIN compressor=SNAPPY, S3 INT32 encoding=PLAIN compressor=SNAPPY) ");
      for (int i = 0; i < 100; i++) {
        if (i == 99) {
          statement.addBatch(
              "insert into root.lz.dev.GPS(time,S1,S3) aligned values("
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          statement.addBatch(
              "insert into root.lz.dev.GPS(time,S1,S2) aligned values("
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
      try (ResultSet resultSet = statement.executeQuery("select S3 from root.lz.dev.GPS")) {
        while (resultSet.next()) {
          assertEquals(99, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(1, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select S2 from root.lz.dev.GPS")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(99, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select S1 from root.lz.dev.GPS")) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1(time, s1,s2) aligned values(1,'aa','bb')");
      statement.execute("insert into root.sg.d1(time, s1,s2) aligned values(1,'aa','bb')");
      statement.execute("insert into root.sg.d1(time, s1,s2) aligned values(1,'aa','bb')");
      statement.execute("flush");
      statement.execute("insert into root.sg.d1(time, s1,s2) aligned values(1,'aa','bb')");
    }
  }

  @Test
  public void testInsertComplexAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("create aligned timeseries root.sg.d1(s1 int32, s2 int32, s3 int32)");
      statement.addBatch("insert into root.sg.d1(time,s1) values(3,1)");
      statement.addBatch("insert into root.sg.d1(time,s1) values(1,1)");
      statement.addBatch("insert into root.sg.d1(time,s1) values(2,1)");
      statement.addBatch("insert into root.sg.d1(time,s2) values(2,2)");
      statement.addBatch("insert into root.sg.d1(time,s2) values(1,2)");
      statement.addBatch("insert into root.sg.d1(time,s2) values(3,2)");
      statement.addBatch("insert into root.sg.d1(time,s3) values(1,3)");
      statement.addBatch("insert into root.sg.d1(time,s3) values(3,3)");
      statement.executeBatch();

      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from root.sg.d1")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from root.sg.d1")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }
    }
  }
}
