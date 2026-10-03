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

package org.apache.iotdb.db.queryengine.transformation.dag.intermediate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.transformation.api.YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
import static org.apache.iotdb.db.queryengine.transformation.api.YieldableState.YIELDABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlidingTimeWindowBoundaryTest {

  @Test
  public void testSingleInputSingleReference() throws Exception {
    assertLastWindow(
        new SingleInputSingleReferenceLayer(
            mock(Expression.class), "boundary_single", 1, parentReader()),
        1);
  }

  @Test
  public void testSingleInputMultiReference() throws Exception {
    assertLastWindow(
        new SingleInputMultiReferenceLayer(
            mock(Expression.class), "boundary_multi_reference", 1, parentReader()),
        1);
  }

  @Test
  public void testMultiInput() throws Exception {
    assertLastWindow(
        new MultiInputLayer(
            mock(Expression.class),
            "boundary_multi_input",
            1,
            Arrays.asList(parentReader(), parentReader())),
        2);
  }

  private void assertLastWindow(IntermediateLayer layer, int columnCount) throws Exception {
    LayerRowWindowReader reader =
        layer.constructRowWindowReader(
            new SlidingTimeWindowAccessStrategy(10, 10, Long.MAX_VALUE - 3, Long.MAX_VALUE), 1);
    assertEquals(YIELDABLE, reader.yield());
    RowWindow window = reader.currentWindow();
    assertEquals(Long.MAX_VALUE - 3, window.windowStartTime());
    assertEquals(Long.MAX_VALUE, window.windowEndTime());
    assertEquals(3, window.windowSize());
    for (int i = 0; i < 3; i++) {
      assertEquals(Long.MAX_VALUE - 3 + i, window.getRow(i).getTime());
      for (int column = 0; column < columnCount; column++) {
        assertEquals(i + 1, window.getRow(i).getInt(column));
      }
    }
    reader.readyForNext();
    assertEquals(NOT_YIELDABLE_NO_MORE_DATA, reader.yield());
  }

  private LayerReader parentReader() throws Exception {
    LayerReader reader = mock(LayerReader.class);
    when(reader.getDataTypes()).thenReturn(new TSDataType[] {TSDataType.INT32});
    when(reader.yield()).thenReturn(YIELDABLE, NOT_YIELDABLE_NO_MORE_DATA);
    when(reader.current())
        .thenReturn(
            new Column[] {
              new IntColumn(3, Optional.empty(), new int[] {1, 2, 3}),
              new TimeColumn(
                  3, new long[] {Long.MAX_VALUE - 3, Long.MAX_VALUE - 2, Long.MAX_VALUE - 1})
            });
    return reader;
  }
}
