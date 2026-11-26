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

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.BinaryColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.LogicAndColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.LogicOrColumnTransformer;
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
import java.util.function.BiFunction;

public class LogicBinaryColumnTransformerTest {

  private static final Type returnType = TypeFactory.getType(TSDataType.BOOLEAN);

  private static final int POSITION_COUNT = 4;

  private static final Boolean[] AND_EXPECTED = {
    null, false, null, false, false, false, null, false, true
  };

  private static final Boolean[] OR_EXPECTED = {
    null, null, true, null, false, true, true, true, true
  };

  private static final boolean[] leftInput = new boolean[] {true, false, true, false};

  private static final boolean[] rightInput = new boolean[] {true, false, true, false};

  private static IdentityColumnTransformer leftOperand;

  private static IdentityColumnTransformer rightOperand;

  @Before
  public void setUp() {
    TsBlockBuilder builder =
        new TsBlockBuilder(POSITION_COUNT, Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder leftColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder rightColumnBuilder = builder.getColumnBuilder(1);
    for (int i = 0; i < POSITION_COUNT; i++) {
      timeColumnBuilder.writeLong(i);
      leftColumnBuilder.writeBoolean(leftInput[i]);
      rightColumnBuilder.writeBoolean(rightInput[i]);
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
  public void testLogicOr() {
    BinaryColumnTransformer transformer =
        new LogicOrColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] || rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testLogicAnd() {
    BinaryColumnTransformer transformer =
        new LogicAndColumnTransformer(returnType, leftOperand, rightOperand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(leftInput[i] && rightInput[i], res.getBoolean(i));
    }
  }

  @Test
  public void testInputType() {
    try {
      leftOperand = new IdentityColumnTransformer(TypeFactory.getType(TSDataType.INT32), 0);
      rightOperand = new IdentityColumnTransformer(returnType, 1);
      BinaryColumnTransformer transformer =
          new LogicAndColumnTransformer(returnType, leftOperand, rightOperand);
      transformer.evaluate();
      Assert.fail("Left and Right input must both be of boolean type.");
    } catch (Exception ignored) {

    }
  }

  @Test
  public void testLogicAndWithNullsAndSelection() {
    assertSelected(
        (left, right) -> new LogicAndColumnTransformer(returnType, left, right),
        AND_EXPECTED,
        false);
  }

  @Test
  public void testLogicAndWithNullsAndPartialSelection() {
    assertSelected(
        (left, right) -> new LogicAndColumnTransformer(returnType, left, right),
        AND_EXPECTED,
        true);
  }

  @Test
  public void testLogicOrWithNullsAndSelection() {
    assertSelected(
        (left, right) -> new LogicOrColumnTransformer(returnType, left, right), OR_EXPECTED, false);
  }

  @Test
  public void testLogicOrWithNullsAndPartialSelection() {
    assertSelected(
        (left, right) -> new LogicOrColumnTransformer(returnType, left, right), OR_EXPECTED, true);
  }

  private void assertSelected(
      BiFunction<ColumnTransformer, ColumnTransformer, BinaryColumnTransformer> transformerFactory,
      Boolean[] expected,
      boolean partial) {
    Boolean[] values = {null, false, true};
    TsBlockBuilder builder =
        new TsBlockBuilder(9, Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN));
    for (Boolean left : values) {
      for (Boolean right : values) {
        builder.getTimeColumnBuilder().writeLong(0);
        if (left == null) {
          builder.getColumnBuilder(0).appendNull();
        } else {
          builder.getColumnBuilder(0).writeBoolean(left);
        }
        if (right == null) {
          builder.getColumnBuilder(1).appendNull();
        } else {
          builder.getColumnBuilder(1).writeBoolean(right);
        }
        builder.declarePosition();
      }
    }
    TsBlock block = builder.build();
    IdentityColumnTransformer left = new IdentityColumnTransformer(returnType, 0);
    IdentityColumnTransformer right = new IdentityColumnTransformer(returnType, 1);
    left.addReferenceCount();
    right.addReferenceCount();
    left.initFromTsBlock(block);
    right.initFromTsBlock(block);
    BinaryColumnTransformer transformer = transformerFactory.apply(left, right);
    transformer.addReferenceCount();
    boolean[] selection = new boolean[9];
    for (int i = 0; i < selection.length; i++) {
      // Keep the right-FALSE rows (1 and 7) selected while excluding some left-FALSE rows.
      selection[i] = !partial || (i != 3 && i != 5);
    }
    boolean[] selected = selection.clone();
    transformer.evaluateWithSelection(selection);
    Assert.assertArrayEquals(selected, selection);
    Column result = transformer.getColumn();
    Assert.assertEquals(expected.length, result.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (!selected[i] || expected[i] == null) {
        Assert.assertTrue(result.isNull(i));
      } else {
        Assert.assertFalse(result.isNull(i));
        Assert.assertEquals(expected[i], result.getBoolean(i));
      }
    }
  }
}
