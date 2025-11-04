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
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowTimeseriesOrderByTimeseriesIT extends AbstractSchemaIT {

  public IoTDBShowTimeseriesOrderByTimeseriesIT(SchemaTestMode schemaTestMode) {
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

  private void prepareSimpleSchema() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln");
      statement.execute("CREATE DATABASE root.sg");
      statement.execute(
          "create timeseries root.ln.d0.s0 with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.sg.d0.s2 with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.sg.d0.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }
  }

  @Test
  public void testOrderByTimeseriesAsc() throws Exception {
    prepareSimpleSchema();

    List<String> expected =
        new ArrayList<>(Arrays.asList("root.ln.d0.s0", "root.sg.d0.s1", "root.sg.d0.s2"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show timeseries order by timeseries")) {
      List<String> actual = new ArrayList<>();
      while (resultSet.next()) {
        actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testOrderByTimeseriesDescWithLimit() throws Exception {
    prepareSimpleSchema();

    List<String> all =
        new ArrayList<>(Arrays.asList("root.ln.d0.s0", "root.sg.d0.s1", "root.sg.d0.s2"));
    Collections.sort(all);
    Collections.reverse(all);
    List<String> expectedTop2 = all.subList(0, 2);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("show timeseries order by timeseries desc limit 2")) {
      List<String> actual = new ArrayList<>();
      while (resultSet.next()) {
        actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
      assertEquals(expectedTop2, actual);
    }
  }

  @Test
  public void testConflictWithLatest() throws Exception {
    prepareSimpleSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet ignored =
          statement.executeQuery("show latest timeseries order by timeseries")) {
        fail("Expected exception for conflict between LATEST and ORDER BY TIMESERIES");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().toLowerCase().contains("latest")
                && e.getMessage().toLowerCase().contains("order by timeseries"));
      }
    }
  }

  @Test
  public void testConflictWithTimeCondition() throws Exception {
    prepareSimpleSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet ignored =
          statement.executeQuery("show timeseries where time > 0 order by timeseries")) {
        fail("Expected exception for conflict between TIME condition and ORDER BY TIMESERIES");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().toLowerCase().contains("time condition")
                && e.getMessage().toLowerCase().contains("order by timeseries"));
      }
    }
  }
}
