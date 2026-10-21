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

import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiffColumnTransformerTest {
  @Test
  public void testMasksAndConsumedFullCacheAdvanceStateOncePerInput() {
    for (Boolean ignoreNull : new Boolean[] {true, false, null}) {
      IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
      IdentityColumnTransformer flags = new IdentityColumnTransformer(BOOLEAN, 1);
      input.addReferenceCount();
      flags.addReferenceCount();
      DiffColumnTransformer diff = new DiffColumnTransformer(DOUBLE, input, flags);
      diff.addReferenceCount();
      ColumnTransformer[] nodes = ColumnTransformer.collectTransformers(Arrays.asList(diff));
      TsBlock[] blocks = {
        block(new Double[] {1.0, 3.0, null}, new Boolean[] {false, null, ignoreNull}),
        block(new Double[] {10.0, null, 20.0, null}, new Boolean[] {false, true, false, null}),
        block(new Double[] {30.0, 32.0}, new Boolean[] {true, false}),
        block(new Double[] {40.0, 41.0}, new Boolean[] {false, true})
      };
      Double[][] expected = {
        {null, 2.0, null},
        {Boolean.TRUE.equals(ignoreNull) ? 7.0 : null, null, 10.0, null},
        {null, 2.0},
        {8.0, 1.0}
      };
      for (int i = 0; i < blocks.length; i++) {
        for (ColumnTransformer node : nodes) {
          node.clearCacheForNewInput();
        }
        input.initFromTsBlock(blocks[i]);
        flags.initFromTsBlock(blocks[i]);
        boolean[] mask = new boolean[blocks[i].getPositionCount()];
        // Even an all-false mask must advance DIFF across the complete input exactly once.
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
        Arrays.fill(mask, true);
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
        diff.tryEvaluate();
        assertValues(diff.getColumn(), expected[i]);
        diff.tryEvaluate();
        assertValues(diff.getColumn(), expected[i]);
        // Clearing ordinary caches within the same input must not advance state again.
        diff.clearCache();
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
        diff.tryEvaluate();
        assertValues(diff.getColumn(), expected[i]);
      }
      diff.close();
    }
  }

  @Test
  public void testPartialSelectionDoesNotMaskInputs() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    IdentityColumnTransformer flags = new IdentityColumnTransformer(BOOLEAN, 1);
    input.addReferenceCount();
    flags.addReferenceCount();
    TsBlock block = block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true});
    input.initFromTsBlock(block);
    flags.initFromTsBlock(block);
    DiffColumnTransformer diff = new DiffColumnTransformer(DOUBLE, input, flags);
    diff.addReferenceCount();
    assertValues(
        diff.getColumnWithSelection(new boolean[] {false, true, false}),
        new Double[] {null, 2.0, 3.0});
    diff.tryEvaluate();
    assertValues(diff.getColumn(), new Double[] {null, 2.0, 3.0});
    diff.close();
  }

  @Test
  public void testConsumedFullCacheDoesNotAdvanceStateAgain() {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    IdentityColumnTransformer flags = new IdentityColumnTransformer(BOOLEAN, 1);
    input.addReferenceCount();
    flags.addReferenceCount();
    TsBlock block = block(new Double[] {1.0, 3.0, 6.0}, new Boolean[] {true, false, true});
    input.initFromTsBlock(block);
    flags.initFromTsBlock(block);
    DiffColumnTransformer diff = new DiffColumnTransformer(DOUBLE, input, flags);
    diff.addReferenceCount();
    diff.tryEvaluate();
    assertValues(diff.getColumn(), new Double[] {null, 2.0, 3.0});
    diff.tryEvaluate();
    assertValues(diff.getColumn(), new Double[] {null, 2.0, 3.0});
    diff.close();
  }

  private static TsBlock block(Double[] values, Boolean[] flags) {
    ColumnBuilder valueBuilder = DOUBLE.createColumnBuilder(values.length);
    ColumnBuilder flagBuilder = BOOLEAN.createColumnBuilder(values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        valueBuilder.appendNull();
      } else {
        valueBuilder.writeDouble(values[i]);
      }
      if (flags[i] == null) {
        flagBuilder.appendNull();
      } else {
        flagBuilder.writeBoolean(flags[i]);
      }
    }
    return new TsBlock(
        new TimeColumn(values.length, new long[values.length]),
        valueBuilder.build(),
        flagBuilder.build());
  }

  private static void assertValues(Column column, Double[] expected) {
    assertEquals(expected.length, column.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(column.isNull(i));
      } else {
        assertFalse(column.isNull(i));
        assertEquals(expected[i], column.getDouble(i), 0.0);
      }
    }
  }
}
