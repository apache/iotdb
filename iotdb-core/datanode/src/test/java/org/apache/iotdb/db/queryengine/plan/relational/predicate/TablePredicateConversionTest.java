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

package org.apache.iotdb.db.queryengine.plan.relational.predicate;

import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.FalseLiteralFilter;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueIsNotNullOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;

public class TablePredicateConversionTest {

  SymbolReference int64Column = new SymbolReference("int64Column");
  Map<String, Integer> int64MeasurementColumnsIndexMap = Collections.singletonMap("int64Column", 0);
  Map<Symbol, ColumnSchema> int64SchemaMap =
      Collections.singletonMap(
          new Symbol("int64Column"), new ColumnSchema("int64Column", LongType.INT64, false, FIELD));

  SymbolReference int32Column = new SymbolReference("int32Column");
  Map<String, Integer> int32MeasurementColumnsIndexMap = Collections.singletonMap("int32Column", 0);
  Map<Symbol, ColumnSchema> int32SchemaMap =
      Collections.singletonMap(
          new Symbol("int32Column"), new ColumnSchema("int32Column", IntType.INT32, false, FIELD));

  @Test
  public void testDoubleLiteralAndInt64Column() {

    // 1. GreaterEqual (>=): int64 >= 123.3 -> int64 >= 124 (ceil)
    // Result: ValueGtEq(124)
    DoubleLiteral doubleLiteral = new DoubleLiteral("123.3");
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, int64Column, doubleLiteral);

    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, 124), actualFilter1);

    // 2. LessThanOrEqual (<=): int64 <= 123.3 -> int64 <= 123 (floor)
    // Result: ValueLtEq(123)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, int64Column, doubleLiteral);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueLtEq(0, 123), actualFilter2);

    // 3. GreaterThan (>): int64 > 123.3 -> int64 > 123 (floor)
    // Result: ValueGt(123)
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN, int64Column, doubleLiteral);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueGt(0, 123), actualFilter3);

    // 4. LessThan (<): int64 < 123.3 -> int64 < 124 (ceil)
    // Result: ValueLt(124)
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int64Column, doubleLiteral);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueLt(0, 124), actualFilter4);

    // 5. Equal (=): int64 = 123.3 -> Impossible for int64
    // Result: FalseLiteralFilter (Always False)
    Expression predicate5 =
        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, int64Column, doubleLiteral);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int64 != 123.3
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int64Column, doubleLiteral);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testNegativeDoubleLiteralAndInt64Column() {
    DoubleLiteral doubleLiteral = new DoubleLiteral("-123.3");

    // 1. GreaterEqual (>=): int64 >= -123.3 -> int64 >= -123 (ceil)
    // Result: ValueGtEq(-123)
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, int64Column, doubleLiteral);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, -123), actualFilter1);

    // 2. LessThanOrEqual (<=): int64 <= -123.3 -> int64 <= -124 (floor)
    // Result: ValueLtEq(-124)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, int64Column, doubleLiteral);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueLtEq(0, -124), actualFilter2);

    // 3. GreaterThan (>): int64 > -123.3 -> int64 > -124 (floor)
    // Note: > -124 (integer) is logically equivalent to >= -123
    // Result: ValueGt(-124)
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN, int64Column, doubleLiteral);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueGt(0, -124), actualFilter3);

    // 4. LessThan (<): int64 < -123.3 -> int64 < -123 (ceil)
    // Note: < -123 (integer) is logically equivalent to <= -124
    // Result: ValueLt(-123)
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int64Column, doubleLiteral);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new LongFilterOperators.ValueLt(0, -123), actualFilter4);

    // 5. Equal (=): int64 = -123.3 -> Impossible for int64
    // Result: FalseLiteralFilter (Always False)
    Expression predicate5 =
        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, int64Column, doubleLiteral);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int64 != -123.3
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int64Column, doubleLiteral);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, int64MeasurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralOverLongMaxAndInt64Column() {
    // Long.MAX_VALUE is approximately 9.22E18.
    // We use "1.0E19" which is definitely greater than Long.MAX_VALUE.
    DoubleLiteral outPositiveRangeDoubleConstant = new DoubleLiteral("1.0E19");
    Map<String, Integer> measurementColumnsIndexMap = Collections.singletonMap("int64Column", 0);

    // 1. GreaterEqual (>=): int64 >= 1.0E19
    // Result: FalseLiteralFilter (Always False)
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            int64Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter1);

    // 2. LessThanOrEqual (<=): int64 <= 1.0E19
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            int64Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter2);

    // 3. GreaterThan (>): int64 > 1.0E19
    // Result: FalseLiteralFilter
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            int64Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter3);

    // 4. LessThan (<): int64 < 1.0E19
    // Result: ValueIsNotNullOperator
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter4);

    // 5. Equal (=): int64 = 1.0E19
    // Result: FalseLiteralFilter
    Expression predicate5 =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int64 != 1.0E19
    // Result: ValueIsNotNullOperator
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralUnderLongMinAndInt64Column() {
    // Long.MIN_VALUE is approximately -9.22E18.
    // We use "-1.0E19" which is definitely smaller than Long.MIN_VALUE.
    DoubleLiteral outNegativeRangeDoubleConstant = new DoubleLiteral("-1.0E19");
    Map<String, Integer> measurementColumnsIndexMap = Collections.singletonMap("int64Column", 0);

    // 1. GreaterEqual (>=): int64 >= -1.0E19
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            int64Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter1);

    // 2. LessThanOrEqual (<=): int64 <= -1.0E19
    // Result: FalseLiteralFilter (Always False)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            int64Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter2);

    // 3. GreaterThan (>): int64 > -1.0E19
    // Result: ValueIsNotNullOperator
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            int64Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter3);

    // 4. LessThan (<): int64 < -1.0E19
    // Result: FalseLiteralFilter
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter4);

    // 5. Equal (=): int64 = -1.0E19
    // Result: FalseLiteralFilter
    Expression predicate5 =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int64 != -1.0E19
    // Result: ValueIsNotNullOperator
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, measurementColumnsIndexMap, int64SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralOverIntegerMaxAndInt32Column() {
    // Integer.MAX_VALUE is approximately 2.14E9.
    // We use "3.0E9" which is definitely greater than Integer.MAX_VALUE.
    DoubleLiteral outPositiveRangeDoubleConstant = new DoubleLiteral("3.0E9");
    Map<String, Integer> measurementColumnsIndexMap = Collections.singletonMap("int32Column", 0);

    // 1. GreaterEqual (>=): int32 >= 3.0E9
    // Result: FalseLiteralFilter (Always False)
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            int32Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter1);

    // 2. LessThanOrEqual (<=): int32 <= 3.0E9
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            int32Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter2);

    // 3. GreaterThan (>): int32 > 3.0E9
    // Result: FalseLiteralFilter
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            int32Column,
            outPositiveRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter3);

    // 4. LessThan (<): int32 < 3.0E9
    // Result: ValueIsNotNullOperator
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter4);

    // 5. Equal (=): int32 = 3.0E9
    // Result: FalseLiteralFilter
    Expression predicate5 =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int32 != 3.0E9
    // Result: ValueIsNotNullOperator
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, measurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralUnderIntegerMinAndInt32Column() {
    // Integer.MIN_VALUE is approximately -2.14E9.
    // We use "-3.0E9" which is definitely smaller than Integer.MIN_VALUE.
    DoubleLiteral outNegativeRangeDoubleConstant = new DoubleLiteral("-3.0E9");

    // 1. GreaterEqual (>=): int32 >= -3.0E9
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression predicate1 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            int32Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            predicate1, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter1);

    // 2. LessThanOrEqual (<=): int32 <= -3.0E9
    // Result: FalseLiteralFilter (Always False)
    Expression predicate2 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            int32Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            predicate2, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter2);

    // 3. GreaterThan (>): int32 > -3.0E9
    // Result: ValueIsNotNullOperator
    Expression predicate3 =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            int32Column,
            outNegativeRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            predicate3, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter3);

    // 4. LessThan (<): int32 < -3.0E9
    // Result: FalseLiteralFilter
    Expression predicate4 =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            predicate4, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter4);

    // 5. Equal (=): int32 = -3.0E9
    // Result: FalseLiteralFilter
    Expression predicate5 =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            predicate5, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NotEqual (!=): int32 != -3.0E9
    // Result: ValueIsNotNullOperator
    Expression predicate6 =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            predicate6, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testInt64LiteralAndInt32Column() {
    // Test INT64 constant ("123") with INT32 column.
    // Since this logic relies on standard type promotion (INT32 -> INT64) and shares the same
    // construction path for all operators, verifying one operator (>=) is sufficient.
    LongLiteral int64Constant = new LongLiteral("123");

    // int32Column >= 123 (Long)
    // result: LongFilterOperators.ValueGtEq(123)
    Expression predicate =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            new SymbolReference("int32Column"),
            int64Constant);

    Filter actualFilter =
        PredicateUtils.convertPredicateToFilter(
            predicate, int32MeasurementColumnsIndexMap, int32SchemaMap, null, null, null);

    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, 123), actualFilter);
  }
}
