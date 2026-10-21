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

package org.apache.iotdb.calc.transformation.dag.column;

import org.apache.iotdb.calc.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.CoalesceColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffFunctionColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ColumnTransformerTest {

  @Test
  public void testCollectionDeduplicatesByIdentity() {
    ColumnTransformer first = new EqualIdentityColumnTransformer(0);
    ColumnTransformer second = new EqualIdentityColumnTransformer(1);
    ColumnTransformer root = new ArithmeticAdditionColumnTransformer(DOUBLE, first, second);

    ColumnTransformer[] collected =
        ColumnTransformer.collectTransformers(Arrays.asList(root, first, root));

    assertEquals(3, collected.length);
    assertSame(root, collected[0]);
    assertSame(first, collected[1]);
    assertSame(second, collected[2]);
  }

  @Test
  public void testNewInputClearingOnlyClearsOwnCache() {
    ColumnTransformer left = new IdentityColumnTransformer(DOUBLE, 0);
    ColumnTransformer right = new IdentityColumnTransformer(DOUBLE, 1);
    ColumnTransformer root = new ArithmeticAdditionColumnTransformer(DOUBLE, left, right);
    left.addReferenceCount();
    right.addReferenceCount();
    root.addReferenceCount();
    left.initializeColumnCache(column(1));
    right.initializeColumnCache(column(2));
    root.initializeColumnCache(column(3));

    root.clearCacheForNewInput();

    assertEquals(0, root.getColumnCachePositionCount());
    assertEquals(1.0, left.getColumn().getDouble(0), 0.0);
    assertEquals(2.0, right.getColumn().getDouble(0), 0.0);
  }

  @Test
  public void testMaskedCommonExpressionKeepsItsInputLength() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    input.addReferenceCount();
    input.initFromTsBlock(
        new TsBlock(
            new TimeColumn(2, new long[] {0, 1}),
            new DoubleColumn(2, Optional.empty(), new double[] {1, 2})));
    ColumnTransformer common = new CoalesceColumnTransformer(DOUBLE, Arrays.asList(input));
    common.addReferenceCount();
    common.tryEvaluateWithSelection(new boolean[] {false, true});
    Column result = common.getColumn();
    assertEquals(2, result.getPositionCount());
    assertEquals(2.0, result.getDouble(1), 0.0);
  }

  @Test
  public void testSelectionEvaluationCapabilityPropagation() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    ColumnTransformer flag =
        new ConstantColumnTransformer(
            BOOLEAN, new BooleanColumn(1, Optional.empty(), new boolean[] {true}));
    ColumnTransformer diff = new DiffColumnTransformer(DOUBLE, input, flag);
    ColumnTransformer root = new ArithmeticAdditionColumnTransformer(DOUBLE, diff, input);

    assertFalse(diff.canEvaluateWithSelection());
    assertFalse(root.canEvaluateWithSelection());
    assertTrue(
        new ArithmeticAdditionColumnTransformer(DOUBLE, input, input).canEvaluateWithSelection());
    // The unary diff stays selection-capable: it forces an all-true child mask internally, so a
    // masked evaluation is equivalent to a full one.
    assertTrue(new DiffFunctionColumnTransformer(DOUBLE, input, false).canEvaluateWithSelection());
  }

  private static Column column(double value) {
    return new DoubleColumn(1, Optional.empty(), new double[] {value});
  }

  private static class EqualIdentityColumnTransformer extends IdentityColumnTransformer {
    private EqualIdentityColumnTransformer(int index) {
      super(DOUBLE, index);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof EqualIdentityColumnTransformer;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }
}
