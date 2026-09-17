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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class GroupedApproxCountDistinctMaskTest {
  @Test
  public void testSparseMaskUsesLogicalPositionsAndSelectedCount() {
    GroupedApproxCountDistinctAccumulator accumulator =
        new GroupedApproxCountDistinctAccumulator(TSDataType.INT32);
    accumulator.setGroupCount(2);
    Column values = new IntColumn(4, Optional.empty(), new int[] {10, 20, 30, 20});
    // The backing positions array is shorter than the input; unselected values must not enter HLL.
    accumulator.addInput(
        new int[] {0, 1, 0, 1},
        new Column[] {values},
        AggregationMask.createSelectedPositions(4, new int[] {1, 3}, 2));
    LongColumnBuilder result = new LongColumnBuilder(null, 1);
    accumulator.evaluateFinal(1, result);
    assertEquals(1L, result.build().getLong(0));
  }
}
