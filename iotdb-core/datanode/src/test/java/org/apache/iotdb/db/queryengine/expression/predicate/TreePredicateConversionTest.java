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

package org.apache.iotdb.db.queryengine.expression.predicate;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.FalseLiteralFilter;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueIsNotNullOperator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class TreePredicateConversionTest {

  Expression int64Column = new TimeSeriesOperand(new PartialPath("root.sg.d1.Int64Column"));
  Expression int32Column = new TimeSeriesOperand(new PartialPath("root.sg.d1.Int32Column"));

  public TreePredicateConversionTest() throws IllegalPathException {}

  private TypeProvider getMockTypeProvider(TSDataType dataType) {
    TypeProvider mockTypeProvider = Mockito.mock(TypeProvider.class);
    Mockito.when(mockTypeProvider.getTreeModelType(Mockito.anyString())).thenReturn(dataType);
    return mockTypeProvider;
  }

  @Test
  public void testDoubleLiteralAndInt64Column() {
    Expression doubleConstant = new ConstantOperand(TSDataType.DOUBLE, "123.3");

    // 1. GreaterEqual (>=): int64 >= 123.3 -> int64 >= 124 (ceil)
    // Result: ValueGtEq(124)
    Expression greaterEqualExpression = new GreaterEqualExpression(int64Column, doubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, 124), actualFilter1);

    // 2. LessEqual (<=): int64 <= 123.3 -> int64 <= 123 (floor)
    // Result: ValueLtEq(123)
    LessEqualExpression lessEqualExpression = new LessEqualExpression(int64Column, doubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueLtEq(0, 123), actualFilter2);

    // 3. GreaterThan (>): int64 > 123.3 -> int64 > 123 (floor)
    // Result: ValueGt(123)
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int64Column, doubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueGt(0, 123), actualFilter3);

    // 4. LessThan (<): int64 < 123.3 -> int64 < 124 (ceil)
    // Result: ValueLt(124)
    LessThanExpression lessThanExpression = new LessThanExpression(int64Column, doubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueLt(0, 124), actualFilter4);

    // 5. EqualTo (=): int64 = 123.3 -> Impossible for int64
    // Result: FalseLiteralFilter (Always False)
    EqualToExpression equalToExpression = new EqualToExpression(int64Column, doubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int64 != 123.3
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    NonEqualExpression nonEqualExpression = new NonEqualExpression(int64Column, doubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testNegativeDoubleLiteralAndInt64Column() {
    Expression doubleConstant = new ConstantOperand(TSDataType.DOUBLE, "-123.3");

    // 1. GreaterEqual (>=): int64 >= -123.3 -> int64 >= -123 (ceil)
    // Result: ValueGtEq(-123)
    Expression greaterEqualExpression = new GreaterEqualExpression(int64Column, doubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, -123), actualFilter1);

    // 2. LessEqual (<=): int64 <= -123.3 -> int64 <= -124 (floor)
    // Result: ValueLtEq(-124)
    LessEqualExpression lessEqualExpression = new LessEqualExpression(int64Column, doubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueLtEq(0, -124), actualFilter2);

    // 3. GreaterThan (>): int64 > -123.3 -> int64 > -124 (floor)
    // Result: ValueGt(-124)
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int64Column, doubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueGt(0, -124), actualFilter3);

    // 4. LessThan (<): int64 < -123.3 -> int64 < -123 (ceil)
    // Result: ValueLt(-123)
    LessThanExpression lessThanExpression = new LessThanExpression(int64Column, doubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueLt(0, -123), actualFilter4);

    // 5. EqualTo (=): int64 = -123.3 -> Impossible for int64
    // Result:FalseLiteralFilter(Always False)
    EqualToExpression equalToExpression = new EqualToExpression(int64Column, doubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int64 != -123.3
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    NonEqualExpression nonEqualExpression = new NonEqualExpression(int64Column, doubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralOverLongMaxAndInt64Column() {
    // Long.MAX_VALUE is approximately 9.22E18.
    // We use "1.0E19" which is definitely greater than Long.MAX_VALUE.
    Expression outPositiveRangeDoubleConstant = new ConstantOperand(TSDataType.DOUBLE, "1.0E19");

    // 1. GreaterEqual (>=): int64 >= 1.0E19
    // Result: FalseLiteralFilter (Always False)
    Expression greaterEqualExpression =
        new GreaterEqualExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter1);

    // 2. LessEqual (<=): int64 <= 1.0E19
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    LessEqualExpression lessEqualExpression =
        new LessEqualExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter2);

    // 3. GreaterThan (>): int64 > 1.0E19
    // Result: FalseLiteralFilter
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter3);

    // 4. LessThan (<): int64 < 1.0E19
    // Result: ValueIsNotNullOperator
    LessThanExpression lessThanExpression =
        new LessThanExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter4);

    // 5. EqualTo (=): int64 = 1.0E19
    // Result: FalseLiteralFilter
    EqualToExpression equalToExpression =
        new EqualToExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int64 != 1.0E19
    // Result: ValueIsNotNullOperator
    NonEqualExpression nonEqualExpression =
        new NonEqualExpression(int64Column, outPositiveRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralUnderLongMinAndInt64Column() {
    // Long.MIN_VALUE is approximately -9.22E18.
    // We use "-1.0E19" which is definitely smaller than Long.MIN_VALUE.
    Expression outNegativeRangeDoubleConstant = new ConstantOperand(TSDataType.DOUBLE, "-1.0E19");

    // 1. GreaterEqual (>=): int64 >= -1.0E19
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    Expression greaterEqualExpression =
        new GreaterEqualExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter1);

    // 2. LessEqual (<=): int64 <= -1.0E19
    // Result: FalseLiteralFilter (Always False)
    LessEqualExpression lessEqualExpression =
        new LessEqualExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter2);

    // 3. GreaterThan (>): int64 > -1.0E19
    // Result: ValueIsNotNullOperator
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter3);

    // 4. LessThan (<): int64 < -1.0E19
    // Result: FalseLiteralFilter
    LessThanExpression lessThanExpression =
        new LessThanExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter4);

    // 5. EqualTo (=): int64 = -1.0E19
    // Result: FalseLiteralFilter
    EqualToExpression equalToExpression =
        new EqualToExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int64 != -1.0E19
    // Result: ValueIsNotNullOperator
    NonEqualExpression nonEqualExpression =
        new NonEqualExpression(int64Column, outNegativeRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int64Column"),
            false,
            getMockTypeProvider(TSDataType.INT64),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralOverIntegerMaxAndInt32Column() {
    // Integer.MAX_VALUE is approximately 2.14E9.
    // We use "3.0E9" which is definitely greater than Integer.MAX_VALUE.
    Expression outPositiveRangeDoubleConstant = new ConstantOperand(TSDataType.DOUBLE, "3.0E9");

    // 1. GreaterEqual (>=): int32 >= 3.0E9
    // Result: FalseLiteralFilter (Always False)
    Expression greaterEqualExpression =
        new GreaterEqualExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter1);

    // 2. LessEqual (<=): int32 <= 3.0E9
    // Result: ValueIsNotNullOperator (Always True, as long as value exists)
    LessEqualExpression lessEqualExpression =
        new LessEqualExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter2);

    // 3. GreaterThan (>): int32 > 3.0E9
    // Result: FalseLiteralFilter
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter3);

    // 4. LessThan (<): int32 < 3.0E9
    // Result: ValueIsNotNullOperator
    LessThanExpression lessThanExpression =
        new LessThanExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter4);

    // 5. EqualTo (=): int32 = 3.0E9
    // Result: FalseLiteralFilter
    EqualToExpression equalToExpression =
        new EqualToExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int32 != 3.0E9
    // Result: ValueIsNotNullOperator
    NonEqualExpression nonEqualExpression =
        new NonEqualExpression(int32Column, outPositiveRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testDoubleLiteralUnderIntegerMinAndInt32Column() {
    // Integer.MIN_VALUE is approximately -2.14E9.
    // We use "-3.0E9" which is definitely smaller than Integer.MIN_VALUE.
    Expression outNegativeRangeDoubleConstant = new ConstantOperand(TSDataType.DOUBLE, "-3.0E9");

    // 1. GreaterEqual (>=): int32 >= -3.0E9
    // Result: ValueIsNotNullOperator
    Expression greaterEqualExpression =
        new GreaterEqualExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter1);

    // 2. LessEqual (<=): int32 <= -3.0E9
    // Result: FalseLiteralFilter (Always False)
    LessEqualExpression lessEqualExpression =
        new LessEqualExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter2 =
        PredicateUtils.convertPredicateToFilter(
            lessEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter2);

    // 3. GreaterThan (>): int32 > -3.0E9
    // Result: ValueIsNotNullOperator
    GreaterThanExpression greaterThanExpression =
        new GreaterThanExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter3 =
        PredicateUtils.convertPredicateToFilter(
            greaterThanExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter3);

    // 4. LessThan (<): int32 < -3.0E9
    // Result: FalseLiteralFilter
    LessThanExpression lessThanExpression =
        new LessThanExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter4 =
        PredicateUtils.convertPredicateToFilter(
            lessThanExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter4);

    // 5. EqualTo (=): int32 = -3.0E9
    // Result: FalseLiteralFilter
    EqualToExpression equalToExpression =
        new EqualToExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter5 =
        PredicateUtils.convertPredicateToFilter(
            equalToExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new FalseLiteralFilter(), actualFilter5);

    // 6. NonEqual (!=): int32 != -3.0E9
    // Result: ValueIsNotNullOperator
    NonEqualExpression nonEqualExpression =
        new NonEqualExpression(int32Column, outNegativeRangeDoubleConstant);
    Filter actualFilter6 =
        PredicateUtils.convertPredicateToFilter(
            nonEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new ValueIsNotNullOperator(0), actualFilter6);
  }

  @Test
  public void testInt64LiteralAndInt32Column() {
    // Test INT64 constant ("123") with INT32 column.
    // Since this logic relies on standard type promotion (INT32 -> INT64) and shares the same
    // construction path for all operators, verifying one operator (>=) is sufficient.
    Expression int64Constant = new ConstantOperand(TSDataType.INT64, "123");
    Expression greaterEqualExpression = new GreaterEqualExpression(int32Column, int64Constant);
    Filter actualFilter1 =
        PredicateUtils.convertPredicateToFilter(
            greaterEqualExpression,
            Collections.singletonList("Int32Column"),
            false,
            getMockTypeProvider(TSDataType.INT32),
            null);
    Assert.assertEquals(new LongFilterOperators.ValueGtEq(0, 123), actualFilter1);
  }
}
