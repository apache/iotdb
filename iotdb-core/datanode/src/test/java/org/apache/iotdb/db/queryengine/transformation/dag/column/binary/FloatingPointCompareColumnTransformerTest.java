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
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareLessThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.ternary.BetweenColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** -0.0 must compare equal to 0.0, as it does in the pushed-down TsFile value filters. */
public class FloatingPointCompareColumnTransformerTest {

  private static final Type BOOLEAN = TypeFactory.getType(TSDataType.BOOLEAN);

  // v column; each row is compared with 0.0 (and 1.0 as the BETWEEN upper bound)
  private static final double[] V = {-0.0, 0.0, Double.NaN, 1.0};

  private IdentityColumnTransformer v;
  private IdentityColumnTransformer zero;
  private IdentityColumnTransformer one;

  private void init(TSDataType dataType) {
    TsBlockBuilder builder =
        new TsBlockBuilder(V.length, Arrays.asList(dataType, dataType, dataType));
    for (int i = 0; i < V.length; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      write(builder.getColumnBuilder(0), dataType, V[i]);
      write(builder.getColumnBuilder(1), dataType, 0.0);
      write(builder.getColumnBuilder(2), dataType, 1.0);
      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    Type type = TypeFactory.getType(dataType);
    v = new IdentityColumnTransformer(type, 0);
    zero = new IdentityColumnTransformer(type, 1);
    one = new IdentityColumnTransformer(type, 2);
    for (IdentityColumnTransformer t : Arrays.asList(v, zero, one)) {
      t.addReferenceCount();
      t.initFromTsBlock(tsBlock);
    }
  }

  private static void write(ColumnBuilder builder, TSDataType dataType, double value) {
    if (dataType == TSDataType.FLOAT) {
      builder.writeFloat((float) value);
    } else {
      builder.writeDouble(value);
    }
  }

  private static void assertResult(ColumnTransformer transformer, boolean... expected) {
    transformer.addReferenceCount();
    transformer.tryEvaluate();
    Column res = transformer.getColumn();
    boolean[] actual = new boolean[res.getPositionCount()];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = res.getBoolean(i);
    }
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testDouble() {
    check(TSDataType.DOUBLE);
  }

  @Test
  public void testFloat() {
    check(TSDataType.FLOAT);
  }

  // rows: -0.0, 0.0, NaN, 1.0
  private void check(TSDataType dataType) {
    init(dataType);
    assertResult(new CompareEqualToColumnTransformer(BOOLEAN, v, zero), true, true, false, false);
    init(dataType);
    assertResult(new CompareNonEqualColumnTransformer(BOOLEAN, v, zero), false, false, false, true);
    init(dataType);
    assertResult(
        new CompareLessThanColumnTransformer(BOOLEAN, v, zero), false, false, false, false);
    init(dataType);
    assertResult(new CompareLessEqualColumnTransformer(BOOLEAN, v, zero), true, true, false, false);
    init(dataType);
    assertResult(
        new CompareGreaterThanColumnTransformer(BOOLEAN, v, zero), false, false, false, true);
    init(dataType);
    assertResult(
        new CompareGreaterEqualColumnTransformer(BOOLEAN, v, zero), true, true, false, true);
    init(dataType);
    assertResult(
        new CompareGreaterThanColumnTransformer(BOOLEAN, zero, v), false, false, false, false);
    init(dataType);
    assertResult(
        new BetweenColumnTransformer(BOOLEAN, v, zero, one, false), true, true, false, true);
    init(dataType);
    assertResult(
        new BetweenColumnTransformer(BOOLEAN, v, zero, one, true), false, false, true, false);
  }
}
