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

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.itbase.runtime.ClusterTestResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Explicit SQL-aware assertions for queries affected by ready-input Collect consumption. */
public final class CollectQueryAssertions {
  private CollectQueryAssertions() {}

  /** Only queries without final ordering or an arbitrary top-level slice may use this path. */
  public static List<ResultSet> getUnorderedResultSets(String sql, ResultSet resultSet) {
    org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement =
        new SqlParser().createStatement(sql, ZoneOffset.UTC, null);
    assertTrue("Unordered assertions require a SELECT query: " + sql, statement instanceof Query);
    Query query = (Query) statement;
    while (true) {
      assertFalse(
          "Final ORDER BY requires ordered assertions: " + sql, query.getOrderBy().isPresent());
      assertFalse(
          "LIMIT requires deterministic result selection: " + sql, query.getLimit().isPresent());
      assertFalse(
          "OFFSET requires deterministic result selection: " + sql, query.getOffset().isPresent());
      QueryBody body = query.getQueryBody();
      if (body instanceof QuerySpecification) {
        QuerySpecification specification = (QuerySpecification) body;
        assertFalse(
            "Final ORDER BY requires ordered assertions: " + sql,
            specification.getOrderBy().isPresent());
        assertFalse(
            "LIMIT requires deterministic result selection: " + sql,
            specification.getLimit().isPresent());
        assertFalse(
            "OFFSET requires deterministic result selection: " + sql,
            specification.getOffset().isPresent());
      }
      if (!(body instanceof TableSubquery)) {
        break;
      }
      // Parentheses around the whole query retain its ordering; nested FROM/UNION branches do not.
      query = ((TableSubquery) body).getQuery();
    }
    return nodeResults(resultSet);
  }

  /** EXPLAIN text may contain node-specific details; each node must satisfy the plan assertions. */
  public static List<ResultSet> getExplainResultSets(String sql, ResultSet resultSet) {
    assertTrue(
        "Plan assertions require EXPLAIN: " + sql,
        new SqlParser().createStatement(sql, ZoneOffset.UTC, null) instanceof Explain);
    return nodeResults(resultSet);
  }

  private static List<ResultSet> nodeResults(ResultSet resultSet) {
    return resultSet instanceof ClusterTestResultSet
        ? ClusterTestResultSet.getNodeResultSets(resultSet)
        : Collections.singletonList(resultSet);
  }

  /** Compare every node independently while retaining duplicate multiplicity. */
  public static void assertUnorderedQueryResult(
      String sql, ResultSet resultSet, String expectedHeader, List<String> expectedRows)
      throws SQLException {
    List<String> sortedExpected = new ArrayList<>(expectedRows);
    Collections.sort(sortedExpected);
    for (ResultSet nodeResult : getUnorderedResultSets(sql, resultSet)) {
      ResultSetMetaData metadata = nodeResult.getMetaData();
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        header.append(metadata.getColumnName(i)).append(",");
      }
      assertEquals(expectedHeader, header.toString());
      List<String> actualRows = new ArrayList<>();
      while (nodeResult.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          row.append(nodeResult.getString(i)).append(",");
        }
        actualRows.add(row.toString());
      }
      Collections.sort(actualRows);
      assertEquals(sortedExpected, actualRows);
    }
  }
}
