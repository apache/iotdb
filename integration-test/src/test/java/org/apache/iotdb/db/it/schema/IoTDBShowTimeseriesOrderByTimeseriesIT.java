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

  private static final List<String> BASE_TIMESERIES =
      Arrays.asList(
          "root.db1.devA.m1",
          "root.db1.devA.m2",
          "root.db1.devB.m1",
          "root.db1.devB.x",
          "root.db2.devA.m1",
          "root.db2.devC.m0",
          "root.db2.devC.m3",
          "root.db3.z.m1",
          "root.db3.z.m10",
          "root.db3.z.m2");

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

  private void prepareComplexSchema() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db1");
      statement.execute("CREATE DATABASE root.db2");
      statement.execute("CREATE DATABASE root.db3");

      for (String ts : BASE_TIMESERIES) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT32, encoding=RLE, compression=SNAPPY", ts));
      }
    }
  }

  private List<String> queryTimeseries(final String sql) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      List<String> result = new ArrayList<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
      return result;
    }
  }

  @Test
  public void testOrderAscWithoutLimit() throws Exception {
    prepareComplexSchema();
    List<String> expected = new ArrayList<>(BASE_TIMESERIES);
    Collections.sort(expected);

    List<String> actual = queryTimeseries("show timeseries root.db*.** order by timeseries");
    assertEquals(expected, actual);
  }

  @Test
  public void testOrderDescWithOffsetLimit() throws Exception {
    prepareComplexSchema();
    List<String> expected = new ArrayList<>(BASE_TIMESERIES);
    Collections.sort(expected);
    Collections.reverse(expected);
    expected = expected.subList(2, 6); // offset 2 limit 4

    List<String> actual =
        queryTimeseries("show timeseries root.db*.** order by timeseries desc offset 2 limit 4");
    assertEquals(expected, actual);
  }

  @Test
  public void testInsertThenQueryOrder() throws Exception {
    prepareComplexSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.db0.devX.a with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES);
    expected.add("root.db0.devX.a");
    Collections.sort(expected);

    List<String> actual = queryTimeseries("show timeseries root.db*.** order by timeseries");
    assertEquals(expected, actual);
  }

  @Test
  public void testDeleteSubtreeThenQueryOrder() throws Exception {
    prepareComplexSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.db2.devC.**");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES);
    expected.remove("root.db2.devC.m0");
    expected.remove("root.db2.devC.m3");
    Collections.sort(expected);

    List<String> actual = queryTimeseries("show timeseries root.db*.** order by timeseries");
    assertEquals(expected, actual);
  }

  @Test
  public void testOffsetLimitAfterDeletesAndAdds() throws Exception {
    prepareComplexSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.db1.devB.x");
      statement.execute(
          "create timeseries root.db1.devC.m0 with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.db4.devZ.z with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES);
    expected.remove("root.db1.devB.x");
    expected.add("root.db1.devC.m0");
    expected.add("root.db4.devZ.z");
    Collections.sort(expected);
    expected = expected.subList(5, 10); // offset 5 limit 5

    List<String> actual = queryTimeseries("show timeseries root.db*.** order by timeseries offset 5 limit 5");
    assertEquals(expected, actual);
  }

  @Test
  public void testConflictWithLatest() throws Exception {
    prepareComplexSchema();
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
    prepareComplexSchema();
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
