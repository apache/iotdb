/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for transformeral information
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

import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

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
