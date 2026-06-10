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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class AnalyzeUtilsTest {

  @Test
  public void testParseDeletePredicateWithRenamedTimeColumn() {
    TsTable table = new TsTable("table1");
    table.addColumnSchema(new TimeColumnSchema("ts", TSDataType.TIMESTAMP));
    Expression expression =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            new Identifier("ts"),
            new LongLiteral("100"));

    List<TableDeletionEntry> entries = AnalyzeUtils.parseExpressions2ModEntries(expression, table);

    assertEquals(1, entries.size());
    assertEquals(Long.MIN_VALUE, entries.get(0).getStartTime());
    assertEquals(100, entries.get(0).getEndTime());
  }

  @Test
  public void testParseDeleteTimePredicateWithBoundary() {
    TsTable table = new TsTable("table1");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));

    Expression expression =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Identifier("time"),
            new LongLiteral(String.valueOf(Long.MAX_VALUE - 1)));
    List<TableDeletionEntry> entries = AnalyzeUtils.parseExpressions2ModEntries(expression, table);
    assertEquals(1, entries.size());
    assertEquals(Long.MAX_VALUE, entries.get(0).getStartTime());
    assertEquals(Long.MAX_VALUE, entries.get(0).getEndTime());

    expression =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN,
            new Identifier("time"),
            new LongLiteral(String.valueOf(Long.MIN_VALUE + 1)));
    entries = AnalyzeUtils.parseExpressions2ModEntries(expression, table);
    assertEquals(1, entries.size());
    assertEquals(Long.MIN_VALUE, entries.get(0).getStartTime());
    assertEquals(Long.MIN_VALUE, entries.get(0).getEndTime());
  }

  @Test
  public void testParseDeleteTimePredicateWithEmptyBoundary() {
    TsTable table = new TsTable("table1");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));

    assertThrows(
        SemanticException.class,
        () ->
            AnalyzeUtils.parseExpressions2ModEntries(
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Identifier("time"),
                    new LongLiteral(String.valueOf(Long.MAX_VALUE))),
                table));
    assertThrows(
        SemanticException.class,
        () ->
            AnalyzeUtils.parseExpressions2ModEntries(
                new ComparisonExpression(
                    ComparisonExpression.Operator.LESS_THAN,
                    new Identifier("time"),
                    new LongLiteral(String.valueOf(Long.MIN_VALUE))),
                table));
  }
}
