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
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class TimeWindowTableFunctionBoundaryTest {

  @Test
  public void testTumbleWindowEndSaturatesAtLongMax() throws UDFException {
    Map<String, Argument> arguments = baseTimeWindowArguments();
    arguments.put("SIZE", new ScalarArgument(Type.INT64, 2L));
    arguments.put("ORIGIN", new ScalarArgument(Type.TIMESTAMP, Long.MAX_VALUE - 1));

    WindowOutput output =
        runProcessor(newProcessor(new TumbleTableFunction(), arguments), 1, Long.MAX_VALUE);

    assertEquals(Collections.singletonList(Long.MAX_VALUE - 1), output.windowStarts);
    assertEquals(Collections.singletonList(Long.MAX_VALUE), output.windowEnds);
    assertEquals(Collections.singletonList(0L), output.passThroughIndexes);
  }

  @Test
  public void testHopWindowComparisonUsesExactEndTime() throws UDFException {
    Map<String, Argument> arguments = baseTimeWindowArguments();
    arguments.put("SIZE", new ScalarArgument(Type.INT64, 2L));
    arguments.put("SLIDE", new ScalarArgument(Type.INT64, 1L));
    arguments.put("ORIGIN", new ScalarArgument(Type.TIMESTAMP, Long.MAX_VALUE - 1));

    WindowOutput output =
        runProcessor(newProcessor(new HOPTableFunction(), arguments), 2, Long.MAX_VALUE);

    assertEquals(Arrays.asList(Long.MAX_VALUE - 1, Long.MAX_VALUE), output.windowStarts);
    assertEquals(Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE), output.windowEnds);
    assertEquals(Arrays.asList(0L, 0L), output.passThroughIndexes);
  }

  @Test
  public void testCumulateWindowEndSaturatesAtLongMax() throws UDFException {
    Map<String, Argument> arguments = baseTimeWindowArguments();
    arguments.put("SIZE", new ScalarArgument(Type.INT64, 4L));
    arguments.put("STEP", new ScalarArgument(Type.INT64, 2L));
    arguments.put("ORIGIN", new ScalarArgument(Type.TIMESTAMP, Long.MAX_VALUE - 3));

    WindowOutput output =
        runProcessor(newProcessor(new CumulateTableFunction(), arguments), 1, Long.MAX_VALUE);

    assertEquals(Collections.singletonList(Long.MAX_VALUE - 3), output.windowStarts);
    assertEquals(Collections.singletonList(Long.MAX_VALUE), output.windowEnds);
    assertEquals(Collections.singletonList(0L), output.passThroughIndexes);
  }

  @Test
  public void testSessionWindowDoesNotSplitWhenGapEndOverflows() throws UDFException {
    Map<String, Argument> arguments = baseTimeWindowArguments();
    arguments.put("GAP", new ScalarArgument(Type.INT64, 2L));

    WindowOutput output =
        runProcessor(
            newProcessor(new SessionTableFunction(), arguments),
            2,
            Long.MAX_VALUE - 1,
            Long.MAX_VALUE);

    assertEquals(Arrays.asList(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1), output.windowStarts);
    assertEquals(Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE), output.windowEnds);
    assertEquals(Arrays.asList(0L, 1L), output.passThroughIndexes);
  }

  private static TableFunctionDataProcessor newProcessor(
      TableFunction tableFunction, Map<String, Argument> arguments) throws UDFException {
    return tableFunction
        .getProcessorProvider(tableFunction.analyze(arguments).getTableFunctionHandle())
        .getDataProcessor();
  }

  private static Map<String, Argument> baseTimeWindowArguments() {
    Map<String, Argument> arguments = new HashMap<>();
    arguments.put(
        "DATA",
        new TableArgument(
            Collections.singletonList(Optional.of("time")),
            Collections.singletonList(Type.TIMESTAMP),
            Collections.emptyList(),
            Collections.emptyList(),
            true));
    arguments.put("TIMECOL", new ScalarArgument(Type.STRING, "time"));
    return arguments;
  }

  private static WindowOutput runProcessor(
      TableFunctionDataProcessor processor, int expectedOutputCount, long... times) {
    ColumnBuilder startBuilder = Mockito.mock(ColumnBuilder.class);
    ColumnBuilder endBuilder = Mockito.mock(ColumnBuilder.class);
    ColumnBuilder passThroughIndexBuilder = Mockito.mock(ColumnBuilder.class);
    List<ColumnBuilder> properColumnBuilders = Arrays.asList(startBuilder, endBuilder);

    for (long time : times) {
      processor.process(recordWithTime(time), properColumnBuilders, passThroughIndexBuilder);
    }
    processor.finish(properColumnBuilders, passThroughIndexBuilder);

    return new WindowOutput(
        captureWrites(startBuilder, expectedOutputCount),
        captureWrites(endBuilder, expectedOutputCount),
        captureWrites(passThroughIndexBuilder, expectedOutputCount));
  }

  private static Record recordWithTime(long time) {
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getLong(0)).thenReturn(time);
    return record;
  }

  private static List<Long> captureWrites(ColumnBuilder builder, int expectedOutputCount) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    Mockito.verify(builder, Mockito.times(expectedOutputCount)).writeLong(captor.capture());
    return captor.getAllValues();
  }

  private static class WindowOutput {
    private final List<Long> windowStarts;
    private final List<Long> windowEnds;
    private final List<Long> passThroughIndexes;

    private WindowOutput(
        List<Long> windowStarts, List<Long> windowEnds, List<Long> passThroughIndexes) {
      this.windowStarts = windowStarts;
      this.windowEnds = windowEnds;
      this.passThroughIndexes = passThroughIndexes;
    }
  }
}
