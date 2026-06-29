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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowTimeseriesOrderByTimeseriesIT extends AbstractSchemaIT {

  private static final List<String> BASE_TIMESERIES_DB1 =
      Arrays.asList("root.db1.devA.m1", "root.db1.devB.m1", "root.db1.devA.m2", "root.db1.devB.x");
  private static final List<String> BASE_TIMESERIES_DB2 =
      Arrays.asList("root.db2.devA.m1", "root.db2.devC.m3", "root.db2.devC.m0");
  private static final List<String> BASE_TIMESERIES = // combine db1 and db2
      Stream.concat(BASE_TIMESERIES_DB1.stream(), BASE_TIMESERIES_DB2.stream())
          .collect(Collectors.toList());

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
    List<String> expected = new ArrayList<>(BASE_TIMESERIES_DB1);
    Collections.sort(expected);
    Collections.reverse(expected);
    expected = expected.subList(1, 3); // offset 1 limit 2

    List<String> actual =
        queryTimeseries("show timeseries root.db1.** order by timeseries desc offset 1 limit 2");
    assertEquals(expected, actual);
  }

  @Test
  public void testInsertThenQueryOrder() throws Exception {
    prepareComplexSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.db1.devX.a with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES_DB1);
    expected.add("root.db1.devX.a");
    Collections.sort(expected);

    List<String> actual = queryTimeseries("show timeseries root.db1.** order by timeseries");
    assertEquals(expected, actual);
  }

  @Test
  public void testDeleteSubtreeThenQueryOrder() throws Exception {
    prepareComplexSchema();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.db2.devC.**");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES_DB2);
    expected.remove("root.db2.devC.m0");
    expected.remove("root.db2.devC.m3");
    Collections.sort(expected);

    List<String> actual = queryTimeseries("show timeseries root.db2.** order by timeseries");
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
          "create timeseries root.db1.devZ.z with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    List<String> expected = new ArrayList<>(BASE_TIMESERIES_DB1);
    expected.remove("root.db1.devB.x");
    expected.add("root.db1.devC.m0");
    expected.add("root.db1.devZ.z");
    Collections.sort(expected);
    expected = expected.subList(2, 4); // offset 2 limit 2

    List<String> actual =
        queryTimeseries("show timeseries root.db1.** order by timeseries offset 2 limit 2");
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
  public void testOrderByWithTimeCondition() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db1");
      statement.execute(
          "create timeseries root.db1.devA.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.db1.devA.s2 with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.db1.devB.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");

      // s1 has points in [1..5], s2 only [1..3], devB.s1 only [4..5]
      for (int t = 1; t <= 5; t++) {
        statement.execute(
            String.format("insert into root.db1.devA(timestamp, s1) values (%d, %d)", t, t));
      }
      for (int t = 1; t <= 3; t++) {
        statement.execute(
            String.format("insert into root.db1.devA(timestamp, s2) values (%d, %d)", t, t));
      }
      for (int t = 4; t <= 5; t++) {
        statement.execute(
            String.format("insert into root.db1.devB(timestamp, s1) values (%d, %d)", t, t));
      }
    }

    List<String> actual =
        queryTimeseries("show timeseries root.db1.** where time > 3 order by timeseries desc");
    assertEquals(Arrays.asList("root.db1.devB.s1", "root.db1.devA.s1"), actual);
  }

  @Test
  public void testWhereClauseOffsetAppliedAfterFilter() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln");
      statement.execute(
          "create timeseries root.ln.wf01.wt01.status with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.ln.wf02.wt01.status with datatype=INT32, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.ln.wf02.wt02.status with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    List<String> actual =
        queryTimeseries(
            "show timeseries root.ln.** where timeseries contains 'wf02.wt' order by timeseries offset 1 limit 1");
    assertEquals(Collections.singletonList("root.ln.wf02.wt02.status"), actual);
  }

  @Test
  public void testAlterTemplateUpdatesOffsetOrder() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("create device template t1 (s1 INT32, s0 INT32)");
      statement.execute("set device template t1 to root.sg1.d1");
      statement.execute("create timeseries using device template on root.sg1.d1");
      statement.execute("set device template t1 to root.sg1.d2");
      statement.execute("create timeseries using device template on root.sg1.d2");
    }

    List<String> before =
        queryTimeseries("show timeseries root.sg1.** order by timeseries desc offset 2 limit 2");
    assertEquals(Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s0"), before);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter device template t1 add (s00 INT32)");
    }

    List<String> after =
        queryTimeseries("show timeseries root.sg1.** order by timeseries offset 3 limit 3");
    assertEquals(Arrays.asList("root.sg1.d2.s0", "root.sg1.d2.s00", "root.sg1.d2.s1"), after);
  }
}
