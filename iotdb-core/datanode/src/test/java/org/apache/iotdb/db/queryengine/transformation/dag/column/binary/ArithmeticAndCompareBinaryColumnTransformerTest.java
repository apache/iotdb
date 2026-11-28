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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticDivisionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticModuloColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticMultiplicationColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticSubtractionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.BinaryColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class ArithmeticAndCompareBinaryColumnTransformerTest {

  private static final Type returnType = TypeFactory.getType(TSDataType.INT32);

  private static final Type booleanType = TypeFactory.getType(TSDataType.BOOLEAN);

  private static final int POSITION_COUNT = 4;

  private static final int[] leftInput = new int[] {1, 2, 3, 4};

  private static final int[] rightInput = new int[] {1, 2, 3, 4};

  private static IdentityColumnTransformer leftOperand;

  private static IdentityColumnTransformer rightOperand;

  @Before
  public void setUp() {
    TsBlockBuilder builder =
        new TsBlockBuilder(POSITION_COUNT, Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder leftColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder rightColumnBuilder = builder.getColumnBuilder(1);
    for (int i = 0; i < POSITION_COUNT; i++) {
      timeColumnBuilder.writeLong(i);
      leftColumnBuilder.writeInt(leftInput[i]);
      rightColumnBuilder.writeInt(rightInput[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    leftOperand = new IdentityColumnTransformer(returnType, 0);
    rightOperand = new IdentityColumnTransformer(returnType, 1);
    leftOperand.addReferenceCount();
    rightOperand.addReferenceCount();
    leftOperand.initFromTsBlock(tsBlock);
    rightOperand.initFromTsBlock(tsBlock);
  }

  @Test
  public void testAddition() {
    BinaryColumnTransformer transformer =
        new ArithmeticAdditionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] + rightInput[i], res.getInt(i));
    }
  }

  @Test
  public void testSubtraction() {
    BinaryColumnTransformer transformer =
        new ArithmeticSubtractionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] - rightInput[i], res.getInt(i));
    }
  }

  @Test
  public void testMultiplication() {
    BinaryColumnTransformer transformer =
        new ArithmeticMultiplicationColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] * rightInput[i], res.getInt(i));
    }
  }

  @Test
  public void testDivision() {
    BinaryColumnTransformer transformer =
        new ArithmeticDivisionColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] / rightInput[i], res.getInt(i));
    }
  }

  @Test
  public void testModulo() {
    BinaryColumnTransformer transformer =
        new ArithmeticModuloColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] % rightInput[i], res.getInt(i));
    }
  }

  @Test
  public void testEqualTo() {
    BinaryColumnTransformer transformer =
        new CompareEqualToColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] == rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testGreaterEqual() {
    BinaryColumnTransformer transformer =
        new CompareGreaterEqualColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] >= rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testGreaterThan() {
    BinaryColumnTransformer transformer =
        new CompareGreaterThanColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] > rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testLessEqual() {
    BinaryColumnTransformer transformer =
        new CompareLessEqualColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] <= rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testLessThan() {
    BinaryColumnTransformer transformer =
        new CompareGreaterThanColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] < rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testInt64ComparisonDoesNotRoundThroughDouble() {
    // Every pair below collides once the INT64 side is converted to double
    long[] longs = {Long.MAX_VALUE, 9007199254740993L, Long.MIN_VALUE, Long.MAX_VALUE - 1};
    double[] doubles = {0x1p63, 9007199254740992.0, -0x1p63, 0x1p63};
    long[] otherLongs = {Long.MAX_VALUE - 1, 9007199254740992L, Long.MIN_VALUE, Long.MAX_VALUE};

    TsBlockBuilder builder =
        new TsBlockBuilder(
            longs.length, Arrays.asList(TSDataType.INT64, TSDataType.DOUBLE, TSDataType.INT64));
    for (int i = 0; i < longs.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeLong(longs[i]);
      builder.getColumnBuilder(1).writeDouble(doubles[i]);
      builder.getColumnBuilder(2).writeLong(otherLongs[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();

    // INT64 = DOUBLE
    Column res = evaluateComparison(tsBlock, TSDataType.INT64, 0, TSDataType.DOUBLE, 1, true);
    Assert.assertFalse(res.getBoolean(0));
    Assert.assertFalse(res.getBoolean(1));
    Assert.assertTrue(res.getBoolean(2));
    Assert.assertFalse(res.getBoolean(3));
    // DOUBLE = INT64, with the INT64 side on the right
    res = evaluateComparison(tsBlock, TSDataType.DOUBLE, 1, TSDataType.INT64, 0, true);
    Assert.assertFalse(res.getBoolean(0));
    Assert.assertTrue(res.getBoolean(2));
    // INT64 > DOUBLE: 9007199254740993 > 9007199254740992.0
    res = evaluateComparison(tsBlock, TSDataType.INT64, 0, TSDataType.DOUBLE, 1, false);
    Assert.assertFalse(res.getBoolean(0));
    Assert.assertTrue(res.getBoolean(1));
    Assert.assertFalse(res.getBoolean(2));
    // INT64 = INT64 above 2^53
    res = evaluateComparison(tsBlock, TSDataType.INT64, 0, TSDataType.INT64, 2, true);
    Assert.assertFalse(res.getBoolean(0));
    Assert.assertFalse(res.getBoolean(1));
    Assert.assertTrue(res.getBoolean(2));
    Assert.assertFalse(res.getBoolean(3));
  }

  private static Column evaluateComparison(
      TsBlock tsBlock,
      TSDataType leftType,
      int leftIndex,
      TSDataType rightType,
      int rightIndex,
      boolean equal) {
    IdentityColumnTransformer left =
        new IdentityColumnTransformer(TypeFactory.getType(leftType), leftIndex);
    IdentityColumnTransformer right =
        new IdentityColumnTransformer(TypeFactory.getType(rightType), rightIndex);
    left.addReferenceCount();
    right.addReferenceCount();
    left.initFromTsBlock(tsBlock);
    right.initFromTsBlock(tsBlock);
    BinaryColumnTransformer transformer =
        equal
            ? new CompareEqualToColumnTransformer(booleanType, left, right)
            : new CompareGreaterThanColumnTransformer(booleanType, left, right);
    transformer.addReferenceCount();
    transformer.evaluate();
    return transformer.getColumn();
  }

  @Test
  public void testNonEqual() {
    BinaryColumnTransformer transformer =
        new CompareNonEqualColumnTransformer(booleanType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] != rightInput[i], res.getBoolean(i));
    }
  }
}
