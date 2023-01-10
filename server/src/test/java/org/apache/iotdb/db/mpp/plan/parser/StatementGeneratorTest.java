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

package org.apache.iotdb.db.mpp.plan.parser;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StatementGeneratorTest {

  @Test
  public void testRawDataQuery() {
    List<String> selectExprList = Arrays.asList("s1", "s2");
    List<String> prefixPaths = Collections.singletonList("root.sg1.d1");
    checkQueryStatement(
        "SELECT s1, s2 FROM root.sg1.d1 LIMIT 10 OFFSET 10", selectExprList, prefixPaths, 10, 10);
  }

  @Test
  public void testGroupByTagWithDuplicatedKeys() {
    try {
      checkQueryStatement(
          "SELECT avg(*) FROM root.sg.** GROUP BY TAGS(k1, k2, k1)",
          Collections.emptyList(),
          Collections.emptyList(),
          10,
          10);
      Assert.fail();
    } catch (SemanticException e) {
      Assert.assertEquals("duplicated key in GROUP BY TAGS: k1", e.getMessage());
    }
  }

  // TODO: add more tests

  private void checkQueryStatement(
      String sql,
      List<String> selectExprList,
      List<String> prefixPaths,
      int rowLimit,
      int rowOffset) {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    // check SELECT clause
    int cnt = 0;
    for (ResultColumn resultColumn : statement.getSelectComponent().getResultColumns()) {
      String selectExpr = resultColumn.getExpression().toString();
      Assert.assertEquals(selectExprList.get(cnt++), selectExpr);
    }
    Assert.assertEquals(selectExprList.size(), cnt);

    // check FROM clause
    cnt = 0;
    for (PartialPath path : statement.getFromComponent().getPrefixPaths()) {
      Assert.assertEquals(prefixPaths.get(cnt++), path.toString());
    }
    Assert.assertEquals(prefixPaths.size(), cnt);

    // check LIMIT & OFFSET clause
    Assert.assertEquals(rowLimit, statement.getRowLimit());
    Assert.assertEquals(rowOffset, statement.getRowOffset());

    // TODO: add more clause
  }
}
