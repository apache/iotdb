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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class UnaryColumnTransformerTest {

  private static final Type returnType = TypeFactory.getType(TSDataType.INT32);

  private static final int POSITION_COUNT = 4;

  private static final int[] input = new int[] {1, 2, 3, 4};

  private static IdentityColumnTransformer operand;

  @Before
  public void setUp() {
    TsBlockBuilder builder =
        new TsBlockBuilder(POSITION_COUNT, Collections.singletonList(TSDataType.INT32));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    for (int i = 0; i < POSITION_COUNT; i++) {
      timeColumnBuilder.writeLong(i);
      columnBuilder.writeInt(input[i]);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    operand = new IdentityColumnTransformer(returnType, 0);
    operand.addReferenceCount();
    operand.initFromTsBlock(tsBlock);
  }

  @Test
  public void testNegation() {
    UnaryColumnTransformer transformer =
        new ArithmeticNegationColumnTransformer(returnType, operand);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(-input[i], res.getInt(i));
    }
  }

  @Test
  public void testIn() {
    Set<String> values = new HashSet<>();
    values.add("2");
    UnaryColumnTransformer transformer =
        new InColumnTransformer(TypeFactory.getType(TSDataType.BOOLEAN), operand, false, values);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertEquals(values.contains(String.valueOf(input[i])), res.getBoolean(i));
    }
  }

  @Test
  public void testIsNull() {
    UnaryColumnTransformer transformer =
        new IsNullColumnTransformer(TypeFactory.getType(TSDataType.BOOLEAN), operand, false);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column res = transformer.getColumn();
    Assert.assertEquals(POSITION_COUNT, res.getPositionCount());
    for (int i = 0; i < POSITION_COUNT; i++) {
      Assert.assertFalse(res.getBoolean(i));
    }
  }

  @Test
  public void testLogicNot() {
    try {
      // check input type
      new LogicNotColumnTransformer(TypeFactory.getType(TSDataType.BOOLEAN), operand);
      Assert.fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testRegular() {
    try {
      // check input type
      new RegularColumnTransformer(
          TypeFactory.getType(TSDataType.BOOLEAN), operand, Pattern.compile("%d"));
      Assert.fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testLike() {
    try {
      // check input type
      new LikeColumnTransformer(
          TypeFactory.getType(TSDataType.BOOLEAN),
          operand,
          LikePattern.compile("%d", Optional.empty()));
      Assert.fail();
    } catch (Exception ignored) {
    }
  }
}
