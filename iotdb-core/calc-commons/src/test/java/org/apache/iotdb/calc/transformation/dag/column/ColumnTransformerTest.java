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
import org.apache.iotdb.calc.transformation.dag.column.binary.LogicAndColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.LogicOrColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

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
  public void testSelectedResultDoesNotBecomeFullCache() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    input.addReferenceCount();
    Column values = new DoubleColumn(2, Optional.empty(), new double[] {1, 2});
    input.initFromTsBlock(new TsBlock(new TimeColumn(2, new long[2]), values));

    Column selected = input.getColumnWithSelection(new boolean[] {true, false});
    assertEquals(1.0, selected.getDouble(0), 0.0);
    org.junit.Assert.assertTrue(selected.isNull(1));
    assertEquals(0, input.getColumnCachePositionCount());

    input.tryEvaluate();
    // Borrowing a full cached column for a selected consumer must not consume its reference.
    assertSame(values, input.getColumnWithSelection(new boolean[] {false, true}));
    assertSame(values, input.getColumn());
  }

  @Test
  public void testLegacySelectedEntryDoesNotReplacePendingFullColumn() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    input.addReferenceCount();
    Column values = new DoubleColumn(2, Optional.empty(), new double[] {1, 2});
    input.initFromTsBlock(new TsBlock(new TimeColumn(2, new long[2]), values));
    input.tryEvaluate();
    input.evaluateWithSelection(new boolean[] {true, false});
    input.getColumn();
    input.tryEvaluate();
    assertSame(values, input.getColumn());
  }

  @Test
  public void testSelectedLogicalOperatorsPreserveThreeValuedLogicWithCachedInputs() {
    Boolean[] values = {null, false, true};
    ColumnBuilder left = BOOLEAN.createColumnBuilder(9);
    ColumnBuilder right = BOOLEAN.createColumnBuilder(9);
    Boolean[] expectedAnd = {null, false, null, false, false, false, null, false, true};
    Boolean[] expectedOr = {null, null, true, null, false, true, true, true, true};
    for (Boolean a : values) {
      for (Boolean b : values) {
        if (a == null) {
          left.appendNull();
        } else {
          left.writeBoolean(a);
        }
        if (b == null) {
          right.appendNull();
        } else {
          right.writeBoolean(b);
        }
      }
    }
    ColumnTransformer a = new IdentityColumnTransformer(BOOLEAN, 0);
    ColumnTransformer b = new IdentityColumnTransformer(BOOLEAN, 1);
    a.addReferenceCount();
    b.addReferenceCount();
    a.initializeColumnCache(left.build());
    b.initializeColumnCache(right.build());
    ColumnTransformer[] operators = {
      new LogicAndColumnTransformer(BOOLEAN, a, b), new LogicOrColumnTransformer(BOOLEAN, a, b)
    };
    for (int i = 0; i < operators.length; i++) {
      Boolean[] expected = i == 0 ? expectedAnd : expectedOr;
      for (boolean partial : new boolean[] {false, true}) {
        boolean[] mask = new boolean[9];
        for (int j = 0; j < mask.length; j++) {
          mask[j] = !partial || j % 2 == 0;
        }
        Column result = operators[i].getColumnWithSelection(mask);
        assertEquals(9, result.getPositionCount());
        for (int j = 0; j < mask.length; j++) {
          if (!mask[j] || expected[j] == null) {
            org.junit.Assert.assertTrue(result.isNull(j));
          } else {
            assertEquals(expected[j], result.getBoolean(j));
          }
        }
      }
    }
    // Selected consumers did not consume the two pending full-column references.
    assertEquals(9, a.getColumn().getPositionCount());
    assertEquals(9, b.getColumn().getPositionCount());
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
