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

package org.apache.iotdb.db.sql;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.sql.parser.StatementGenerator;
import org.apache.iotdb.db.sql.statement.QueryStatement;
import org.apache.iotdb.db.sql.statement.component.ResultColumn;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.*;

public class StatementGeneratorTest {

  @Test
  @Ignore
  public void testRawDataQuery() {
    List<String> selectExprList = Arrays.asList("s1", "s2");
    List<String> prefixPaths = Collections.singletonList("root.sg1.d1");
    checkQueryStatement(
        "SELECT s1, s2 FROM root.sg1.d1 LIMIT 10 OFFSET 10", selectExprList, prefixPaths, 10, 10);
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
    for (ResultColumn resultColumn : statement.getSelectComponent().getResultColumns()) {
      String selectExpr = resultColumn.getExpression().toString();
      Assert.assertTrue(selectExprList.contains(selectExpr));
      selectExprList.remove(selectExpr);
    }
    Assert.assertTrue(selectExprList.isEmpty());

    // check FROM clause
    for (PartialPath path : statement.getFromComponent().getPrefixPaths()) {
      Assert.assertTrue(prefixPaths.contains(path.toString()));
      selectExprList.remove(path.toString());
    }
    Assert.assertTrue(prefixPaths.isEmpty());

    // check LIMIT & OFFSET clause
    Assert.assertEquals(rowLimit, statement.getRowLimit());
    Assert.assertEquals(rowOffset, statement.getRowOffset());

    // TODO: add more clause
  }
}
