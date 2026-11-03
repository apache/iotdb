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
package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.runtime.ClusterTestResultSet;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqualUnordered;
import static org.apache.iotdb.itbase.runtime.ClusterTestResultSet.getNodeResultSets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Deterministic coverage of cross-node assertions without depending on network arrival order. */
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBClusterResultSetIT {

  @Test
  public void testUnorderedRowsCheckEveryNode() throws SQLException {
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"b", "a"})) {
      assertResultSetEqualUnordered(result, "value,", Arrays.asList("a,", "b,"));
    }
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"b", "c"})) {
      assertThrows(
          AssertionError.class,
          () -> assertResultSetEqualUnordered(result, "value,", Arrays.asList("a,", "b,")));
    }
  }

  @Test
  public void testUnorderedRowsPreserveDuplicateMultiplicity() throws SQLException {
    try (ResultSet result = cluster(new String[] {"a", "a", "b"}, new String[] {"b", "a", "a"})) {
      assertResultSetEqualUnordered(result, "value,", Arrays.asList("a,", "a,", "b,"));
    }
    for (String[] values : new String[][] {{"a", "b"}, {"a", "a", "a", "b"}}) {
      try (ResultSet result = cluster(new String[] {"a", "a", "b"}, values)) {
        assertThrows(
            AssertionError.class,
            () -> assertResultSetEqualUnordered(result, "value,", Arrays.asList("a,", "a,", "b,")));
      }
    }
  }

  @Test
  public void testSetAssertionChecksEveryNode() throws SQLException {
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"b", "a"})) {
      TestUtils.assertResultSetEqual(result, "value,", Set.of("a,", "b,"));
    }
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"a"})) {
      assertThrows(
          AssertionError.class,
          () -> TestUtils.assertResultSetEqual(result, "value,", Set.of("a,", "b,")));
    }
  }

  @Test
  public void testSetConsumerAssertionChecksEveryNode() throws SQLException {
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"b", "a"})) {
      TestUtils.assertResultSetEqual(result, "value,", Set.of("a,", "b,"), message -> {});
    }
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"a"})) {
      assertThrows(
          AssertionError.class,
          () ->
              TestUtils.assertResultSetEqual(result, "value,", Set.of("a,", "b,"), message -> {}));
    }
  }

  @Test
  public void testOrderedRowsStillRejectDifferentNodeOrder() throws SQLException {
    try (ResultSet result = cluster(new String[] {"a", "b"}, new String[] {"b", "a"})) {
      assertThrows(
          AssertionError.class,
          () -> TestUtils.assertResultSetEqual(result, "value,", Arrays.asList("a,", "b,")));
    }
  }

  @Test
  public void testSingleNodeAndNullValues() throws SQLException {
    try (ResultSet result = rows("a", null, "a")) {
      assertEquals(1, getNodeResultSets(result).size());
      assertResultSetEqualUnordered(result, "value,", Arrays.asList("null,", "a,", "a,"));
    }
    try (ResultSet result = cluster(new String[] {"a"})) {
      assertEquals(1, getNodeResultSets(result).size());
      assertResultSetEqualUnordered(result, "value,", Arrays.asList("a,"));
    }
  }

  private static ResultSet cluster(String[]... nodeRows) throws SQLException {
    List<Statement> statements = new ArrayList<>();
    List<String> endpoints = new ArrayList<>();
    for (String[] values : nodeRows) {
      ResultSet result = rows(values);
      Statement statement =
          (Statement)
              Proxy.newProxyInstance(
                  Statement.class.getClassLoader(),
                  new Class<?>[] {Statement.class},
                  (proxy, method, arguments) -> {
                    if (method.getName().equals("executeQuery")) {
                      return result;
                    }
                    throw new UnsupportedOperationException(method.getName());
                  });
      statements.add(statement);
      endpoints.add("node-" + endpoints.size());
    }
    return new ClusterTestResultSet(statements, endpoints, "SELECT value", 10, null);
  }

  private static CachedRowSet rows(String... values) throws SQLException {
    CachedRowSet result = RowSetProvider.newFactory().createCachedRowSet();
    RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
    metadata.setColumnCount(1);
    metadata.setColumnName(1, "value");
    metadata.setColumnType(1, Types.VARCHAR);
    result.setMetaData(metadata);
    for (String value : values) {
      result.afterLast();
      result.moveToInsertRow();
      result.updateString(1, value);
      result.insertRow();
      result.moveToCurrentRow();
    }
    result.beforeFirst();
    return result;
  }
}
