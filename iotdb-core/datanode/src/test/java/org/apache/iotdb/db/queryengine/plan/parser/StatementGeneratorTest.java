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

package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StatementGeneratorTest {

  @Test
  public void rawDataQueryTest() {
    String sql = "SELECT s1, s2 FROM root.sg1.d1 WHERE time > 1 and s3 > 2 LIMIT 10 OFFSET 11";
    checkQueryStatement(
        sql,
        Arrays.asList("s1", "s2"),
        Collections.singletonList("root.sg1.d1"),
        "Time > 1 & s3 > 2",
        10,
        11);
  }

  @Test
  public void groupByTagWithDuplicatedKeysTest() {
    try {
      checkQueryStatement(
          "SELECT avg(*) FROM root.sg.** GROUP BY TAGS(k1, k2, k1)",
          Collections.emptyList(),
          Collections.emptyList(),
          "",
          10,
          10);
      Assert.fail();
    } catch (SemanticException e) {
      assertEquals("duplicated key in GROUP BY TAGS: k1", e.getMessage());
    }
  }

  // TODO: add more tests

  private void checkQueryStatement(
      String sql,
      List<String> selectExprList,
      List<String> fromPrefixPaths,
      String wherePredicateString,
      int rowLimit,
      int rowOffset) {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    // check SELECT clause
    int cnt = 0;
    for (ResultColumn resultColumn : statement.getSelectComponent().getResultColumns()) {
      String selectExpr = resultColumn.getExpression().toString();
      assertEquals(selectExprList.get(cnt++), selectExpr);
    }
    assertEquals(selectExprList.size(), statement.getSelectComponent().getResultColumns().size());

    // check FROM clause
    cnt = 0;
    for (PartialPath path : statement.getFromComponent().getPrefixPaths()) {
      assertEquals(fromPrefixPaths.get(cnt++), path.toString());
    }
    assertEquals(fromPrefixPaths.size(), statement.getFromComponent().getPrefixPaths().size());

    // check WHERE clause
    assertEquals(
        wherePredicateString, statement.getWhereCondition().getPredicate().getExpressionString());

    // check LIMIT & OFFSET clause
    assertEquals(rowLimit, statement.getRowLimit());
    assertEquals(rowOffset, statement.getRowOffset());

    // TODO: add more clause
  }
}
