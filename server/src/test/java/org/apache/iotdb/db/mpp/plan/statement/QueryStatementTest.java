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

package org.apache.iotdb.db.mpp.plan.statement;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement.RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QueryStatementTest {

  private static final Logger logger = LoggerFactory.getLogger(QueryStatementTest.class);

  private static final String ALIGN_BY_DEVICE_ONE_LEVEL_ERROR =
      "ALIGN BY DEVICE: the suffix paths can only be measurement or one-level wildcard";

  @Test
  public void semanticCheckTest() {
    List<Pair<String, String>> errorSqlWithMessages =
        Arrays.asList(
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 GROUP BY ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d)",
                "Common queries and aggregated queries are not allowed to appear at the same time"),
            new Pair<>(
                "SELECT count(s1),s2 FROM root.sg.d1", RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG),

            // test for where clause
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 WHERE count(s1) > 0",
                "Aggregate functions are not supported in WHERE clause"),

            // test for having clause
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 HAVING(s1 > 0)",
                "Expression of HAVING clause must to be an Aggregation"),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 HAVING(count(s1) > 0)",
                "Expression of HAVING clause can not be used in NonAggregationQuery"),
            new Pair<>(
                "SELECT count(d1.s1) FROM root.sg.d1 GROUP BY level=1 HAVING (count(s1) > 0)",
                "When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard"),
            new Pair<>(
                "SELECT count(s1) FROM root.sg.d1 GROUP BY level=1 HAVING (count(sg.d1.s1) > 0)",
                "When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard"),

            // test for align by device clause
            new Pair<>(
                "SELECT d1.s1 FROM root.sg.d1 align by device", ALIGN_BY_DEVICE_ONE_LEVEL_ERROR),
            new Pair<>(
                "SELECT count(s1) FROM root.sg.d1 group by variation(sg.s1) align by device",
                ALIGN_BY_DEVICE_ONE_LEVEL_ERROR),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 order by root.sg.d1.s1 align by device",
                ALIGN_BY_DEVICE_ONE_LEVEL_ERROR),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 where root.sg.d1.s1 > 0 align by device",
                ALIGN_BY_DEVICE_ONE_LEVEL_ERROR),
            new Pair<>(
                "SELECT count(s1) FROM root.sg.d1 having(count(root.sg.d1.s1) > 0) align by device",
                ALIGN_BY_DEVICE_ONE_LEVEL_ERROR),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 order by timeseries align by device",
                "Sorting by timeseries is only supported in last queries."),

            // test for last query
            new Pair<>(
                "SELECT last s1 FROM root.sg.d1 align by device",
                "Last query doesn't support align by device."),
            new Pair<>(
                "SELECT last s1+s2 FROM root.sg.d1",
                "Last queries can only be applied on raw time series."),
            new Pair<>(
                "SELECT last s1 FROM root.sg.d1 order by device",
                "Sorting by device is only supported in ALIGN BY DEVICE queries."),
            new Pair<>(
                "SELECT last s1 FROM root.sg.d1 SLIMIT 1 SOFFSET 2",
                "SLIMIT and SOFFSET can not be used in LastQuery."),

            // test for select into clause
            new Pair<>(
                "SELECT s1 INTO root.sg.d2(t1) FROM root.sg.d1 SLIMIT 5",
                "Select into: slimit clauses are not supported."),
            new Pair<>(
                "SELECT s1 INTO root.sg.d2(t1) FROM root.sg.d1 SOFFSET 6",
                "Select into: soffset clauses are not supported."),
            new Pair<>(
                "SELECT last s1 INTO root.sg.d2(t1) FROM root.sg.d1",
                "Select into: last clauses are not supported."),
            new Pair<>(
                "SELECT count(s1) INTO root.sg.d2(t1) FROM root.sg.d1 GROUP BY TAGS(a)",
                "Select into: GROUP BY TAGS clause are not supported."),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 order by timeseries",
                "Sorting by timeseries is only supported in last queries."),
            new Pair<>(
                "SELECT s1 FROM root.sg.d1 order by device",
                "Sorting by device is only supported in ALIGN BY DEVICE queries."));

    for (Pair<String, String> pair : errorSqlWithMessages) {
      String errorSql = pair.left;
      String errorMsg = pair.right;
      try {
        checkErrorQuerySql(errorSql);
      } catch (SemanticException e) {
        assertEquals(errorMsg, e.getMessage());
      } catch (Exception ex) {
        logger.error("Meets error in test sql: {}", errorSql, ex);
        fail();
      }
    }
  }

  private void checkErrorQuerySql(String sql) {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    statement.semanticCheck();
  }
}
