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

public class LogicBinaryColumnTransformerTest {

  private static final Type returnType = TypeFactory.getType(TSDataType.BOOLEAN);

  private static final int POSITION_COUNT = 4;

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
}
