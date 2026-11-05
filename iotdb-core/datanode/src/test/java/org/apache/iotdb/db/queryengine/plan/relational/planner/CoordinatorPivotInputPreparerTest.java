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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PivotRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CoordinatorPivotInputPreparerTest {
  @Test
  public void testOrdinaryStatementDoesNotRequireAstChildren() {
    new CoordinatorPivotInputPreparer()
        .prepare(
            new SqlParser()
                .createStatement(
                    "CREATE DATABASE pivot_test",
                    ZoneId.systemDefault(),
                    new InternalClientSession("pivot-preparer-test")),
            null,
            null,
            null,
            List.of(),
            Map.of(),
            null);
  }

  @Test
  public void testExplicitGroupByDiscoveryProjectsReferencedColumns() {
    PivotRelation pivot = parsePivot("PIVOT table1 ON tag1 USING COUNT(s1) GROUP BY tag2");
    List<?> items = new CoordinatorPivotInputPreparer().buildDiscoverySelectItems(pivot);
    assertEquals(4, items.size());
    assertTrue(
        items.get(0)
            instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn);
    assertTrue(
        items.get(3)
            instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn);
  }

  @Test
  public void testNestedWithKeepsOuterDefinitions() {
    With outer = parseWith("WITH outer_cte AS (SELECT s1 FROM table1) SELECT s1 FROM outer_cte");
    With inner = parseWith("WITH inner_cte AS (SELECT s1 FROM table1) SELECT s1 FROM inner_cte");
    With merged = CoordinatorPivotInputPreparer.mergeVisibleWith(outer, inner.getQueries(), false);
    assertEquals(
        List.of("outer_cte", "inner_cte"),
        merged.getQueries().stream().map(query -> query.getName().getValue()).toList());
  }

  @Test
  public void testNestedWithShadowingKeepsInnerDefinition() {
    With outer = parseWith("WITH cte AS (SELECT s1 FROM table1) SELECT s1 FROM cte");
    With inner = parseWith("WITH cte AS (SELECT s2 FROM table1) SELECT s2 FROM cte");
    With merged = CoordinatorPivotInputPreparer.mergeVisibleWith(outer, inner.getQueries(), false);
    assertEquals(1, merged.getQueries().size());
    assertSame(inner.getQueries().get(0), merged.getQueries().get(0));
  }

  @Test
  public void testInheritedRecursiveWithIsNotDowngraded() {
    With outer =
        parseWith("WITH RECURSIVE outer_cte AS (SELECT s1 FROM table1) SELECT s1 FROM outer_cte");
    With local = parseWith("WITH local_cte AS (SELECT s2 FROM table1) SELECT s2 FROM local_cte");
    With merged = CoordinatorPivotInputPreparer.mergeVisibleWith(outer, local.getQueries(), false);
    assertTrue(merged.isRecursive());
    assertEquals(2, merged.getQueries().size());
  }

  private With parseWith(String sql) {
    return ((Query)
            new SqlParser()
                .createStatement(
                    sql, ZoneId.systemDefault(), new InternalClientSession("pivot-with-test")))
        .getWith()
        .orElseThrow();
  }

  private PivotRelation parsePivot(String sql) {
    return (PivotRelation)
        ((org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query)
                new SqlParser()
                    .createStatement(
                        sql,
                        ZoneId.systemDefault(),
                        new InternalClientSession("pivot-projection-test")))
            .getQueryBody();
  }

  @Test
  public void testDiscoveredValuesPreserveTypedLiteralsAndNames() {
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.STRING, TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeBinary(new Binary("north", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(1).writeInt(20261028);
    builder.declarePosition();
    TsBlock block = builder.build();
    PivotRelation.Value text =
        CoordinatorPivotInputPreparer.valueOf(block.getColumn(0), 0, TSDataType.STRING);
    assertEquals("north", text.alias().orElseThrow().getValue());
    assertTrue(
        text.expression()
            instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral);
    TsBlockBuilder emptyBuilder = new TsBlockBuilder(List.of(TSDataType.STRING));
    emptyBuilder.getTimeColumnBuilder().writeLong(0);
    emptyBuilder.getColumnBuilder(0).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    emptyBuilder.declarePosition();
    PivotRelation.Value empty =
        CoordinatorPivotInputPreparer.valueOf(
            emptyBuilder.build().getColumn(0), 0, TSDataType.STRING);
    assertFalse(empty.alias().isPresent());
    assertEquals(
        "",
        ((org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral)
                empty.expression())
            .getValue());
    PivotRelation.Value date =
        CoordinatorPivotInputPreparer.valueOf(block.getColumn(1), 0, TSDataType.DATE);
    assertTrue(date.expression() instanceof GenericLiteral);
    assertEquals("2026-10-28", date.alias().orElseThrow().getValue());
  }
}
