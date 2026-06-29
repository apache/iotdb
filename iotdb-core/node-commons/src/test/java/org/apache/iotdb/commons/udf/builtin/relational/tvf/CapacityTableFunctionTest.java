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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CapacityTableFunctionTest {

  private final CapacityTableFunction function = new CapacityTableFunction();

  // ======================== analyze() tests ========================

  @Test
  public void testAnalyzeWithSlideDefault() throws UDFException {
    Map<String, Argument> args = new HashMap<>();
    args.put("SIZE", new ScalarArgument(Type.INT64, 5L));
    args.put("SLIDE", new ScalarArgument(Type.INT64, -1L));

    TableFunctionAnalysis analysis = function.analyze(args);
    assertNotNull(analysis);
  }

  @Test
  public void testAnalyzeWithExplicitSlide() throws UDFException {
    Map<String, Argument> args = new HashMap<>();
    args.put("SIZE", new ScalarArgument(Type.INT64, 4L));
    args.put("SLIDE", new ScalarArgument(Type.INT64, 2L));

    TableFunctionAnalysis analysis = function.analyze(args);
    assertNotNull(analysis);
  }

  @Test(expected = UDFException.class)
  public void testAnalyzeSizeZero() throws UDFException {
    Map<String, Argument> args = new HashMap<>();
    args.put("SIZE", new ScalarArgument(Type.INT64, 0L));
    args.put("SLIDE", new ScalarArgument(Type.INT64, -1L));

    function.analyze(args);
  }

  @Test(expected = UDFException.class)
  public void testAnalyzeSizeNegative() throws UDFException {
    Map<String, Argument> args = new HashMap<>();
    args.put("SIZE", new ScalarArgument(Type.INT64, -3L));
    args.put("SLIDE", new ScalarArgument(Type.INT64, -1L));

    function.analyze(args);
  }

  // ======================== processor tests ========================

  /**
   * Helper: builds the processor from analyze() -> getProcessorProvider() chain, then feeds N rows
   * through process(). Returns captured (windowIndex, passThroughIndex) pairs.
   */
  private List<long[]> runProcessor(long size, long slide, int rowCount) throws UDFException {
    Map<String, Argument> args = new HashMap<>();
    args.put("SIZE", new ScalarArgument(Type.INT64, size));
    args.put("SLIDE", new ScalarArgument(Type.INT64, slide == -1 ? -1L : slide));

    TableFunctionAnalysis analysis = function.analyze(args);
    TableFunctionHandle handle = analysis.getTableFunctionHandle();

    TableFunctionProcessorProvider provider = function.getProcessorProvider(handle);
    TableFunctionDataProcessor processor = provider.getDataProcessor();

    Record mockRecord = Mockito.mock(Record.class);
    List<long[]> results = new ArrayList<>();

    for (int i = 0; i < rowCount; i++) {
      ArgumentCaptor<Long> windowCaptor = ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Long> indexCaptor = ArgumentCaptor.forClass(Long.class);

      ColumnBuilder properBuilder = Mockito.mock(ColumnBuilder.class);
      ColumnBuilder passThroughBuilder = Mockito.mock(ColumnBuilder.class);

      processor.process(mockRecord, Collections.singletonList(properBuilder), passThroughBuilder);

      Mockito.verify(properBuilder, Mockito.atLeast(0)).writeLong(windowCaptor.capture());
      Mockito.verify(passThroughBuilder, Mockito.atLeast(0)).writeLong(indexCaptor.capture());

      List<Long> windows = windowCaptor.getAllValues();
      List<Long> indices = indexCaptor.getAllValues();
      for (int j = 0; j < windows.size(); j++) {
        results.add(new long[] {windows.get(j), indices.get(j)});
      }
    }
    return results;
  }

  @Test
  public void testSlideEqualsSize() throws UDFException {
    // SIZE=2, SLIDE=2 (non-overlapping), 5 rows
    // window0: rows 0,1; window1: rows 2,3; window2: row 4
    List<long[]> results = runProcessor(2, 2, 5);
    long[][] expected = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {2, 4}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testSlideDefault() throws UDFException {
    // SIZE=2, SLIDE=-1 (defaults to SIZE=2), 5 rows — same as above
    List<long[]> results = runProcessor(2, -1, 5);
    long[][] expected = {{0, 0}, {0, 1}, {1, 2}, {1, 3}, {2, 4}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testSlideLessThanSize() throws UDFException {
    // SIZE=2, SLIDE=1 (overlapping), 3 rows
    // row0: window 0
    // row1: window 0, 1
    // row2: window 1, 2
    List<long[]> results = runProcessor(2, 1, 3);
    long[][] expected = {{0, 0}, {0, 1}, {1, 1}, {1, 2}, {2, 2}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testSlideGreaterThanSize() throws UDFException {
    // SIZE=2, SLIDE=3 (gap), 6 rows
    // window0: rows 0,1; row2: gap; window1: rows 3,4; row5: gap
    List<long[]> results = runProcessor(2, 3, 6);
    long[][] expected = {{0, 0}, {0, 1}, {1, 3}, {1, 4}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testOverlappingLargeSize() throws UDFException {
    // SIZE=3, SLIDE=2 (overlapping), 3 rows
    // row0: window 0
    // row1: window 0
    // row2: window 0, 1
    List<long[]> results = runProcessor(3, 2, 3);
    long[][] expected = {{0, 0}, {0, 1}, {0, 2}, {1, 2}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testSingleRow() throws UDFException {
    // SIZE=3, SLIDE=1, 1 row — row0 belongs to window 0 only
    List<long[]> results = runProcessor(3, 1, 1);
    long[][] expected = {{0, 0}};
    assertResultsEqual(expected, results);
  }

  @Test
  public void testGetArgumentsSpecifications() {
    assertEquals(3, function.getArgumentsSpecifications().size());
  }

  @Test
  public void testCreateTableFunctionHandle() {
    assertNotNull(function.createTableFunctionHandle());
  }

  private void assertResultsEqual(long[][] expected, List<long[]> actual) {
    assertEquals("Result count mismatch", expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals("Window index mismatch at position " + i, expected[i][0], actual.get(i)[0]);
      assertEquals("PassThrough index mismatch at position " + i, expected[i][1], actual.get(i)[1]);
    }
  }
}
