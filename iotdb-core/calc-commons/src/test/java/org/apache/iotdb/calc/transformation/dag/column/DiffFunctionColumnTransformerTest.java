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
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffFunctionColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiffFunctionColumnTransformerTest {
  @Test
  public void testMasksAndConsumedFullCacheAdvanceStateOncePerInput() {
    for (boolean ignoreNull : new boolean[] {true, false}) {
      IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
      input.addReferenceCount();
      DiffFunctionColumnTransformer diff =
          new DiffFunctionColumnTransformer(DOUBLE, input, ignoreNull);
      diff.addReferenceCount();
      ColumnTransformer[] nodes = ColumnTransformer.collectTransformers(Arrays.asList(diff));
      TsBlock[] blocks = {
        block(new double[] {1, 3, 0}, new boolean[] {false, false, true}),
        block(new double[] {10, 11}, null)
      };
      Double[][] expected = {{null, 2.0, null}, {ignoreNull ? 7.0 : null, 1.0}};
      for (int i = 0; i < blocks.length; i++) {
        for (ColumnTransformer node : nodes) {
          node.clearCacheForNewInput();
        }
        input.initFromTsBlock(blocks[i]);
        boolean[] mask = new boolean[blocks[i].getPositionCount()];
        // Even an all-false mask must advance DIFF across all input rows exactly once.
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
        Arrays.fill(mask, true);
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
        diff.tryEvaluate();
        assertValues(diff.getColumn(), expected[i]);
        // The ordinary cache has been consumed; re-requesting it must not advance state again.
        diff.tryEvaluate();
        assertValues(diff.getColumn(), expected[i]);
        assertValues(diff.getColumnWithSelection(mask), expected[i]);
      }
    }
  }

  private static TsBlock block(double[] values, boolean[] nulls) {
    return new TsBlock(
        new TimeColumn(values.length, new long[values.length]),
        new DoubleColumn(values.length, Optional.ofNullable(nulls), values));
  }

  private static void assertValues(Column column, Double[] expected) {
    assertEquals(expected.length, column.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(column.isNull(i));
      } else {
        assertEquals(expected[i], column.getDouble(i), 0.0);
      }
    }
  }
}
