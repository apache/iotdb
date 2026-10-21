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

package org.apache.iotdb.commons.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.match.QetchAlgorithm;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PatternMatchTableFunctionTest {

  @Test
  public void testRejectEmptyRepetitionGroups() {
    for (String pattern :
        new String[] {
          "1,{,}+,1",
          "1,{}+,1",
          "1,{ , , }*,1",
          "{,}+,1,2",
          "1,2,{,}+",
          "1,{2,{,}+,3}+,1",
          "1,{{,}*}+,1"
        }) {
      String expectedMessage =
          String.format(
              QueryMessages
                  .EXCEPTION_INVALID_PATTERN_ARG_REPETITION_GROUPS_MUST_CONTAIN_AT_LEAST_ONE_NUMERIC_DATA_POINT_E1880CCB,
              pattern);
      assertInvalidPattern(pattern, expectedMessage);
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> new QetchAlgorithm().parsePattern2Automaton(pattern));
      assertEquals(expectedMessage, exception.getMessage());
    }
  }

  @Test
  public void testRejectMalformedPatternsDuringAnalysis() {
    for (String pattern : new String[] {null, "", "1", "abc,def,ghi"}) {
      assertInvalidPattern(
          pattern, String.format(QueryMessages.INVALID_PATTERN_MUST_CONTAIN_DATA_POINTS, pattern));
    }
    for (String pattern : new String[] {"1,{2,3,1", "1,2,3}+,1"}) {
      assertInvalidPattern(
          pattern,
          String.format(
              QueryMessages.EXCEPTION_INVALID_PATTERN_ARG_UNMATCHED_OR_9B5E4AE4, pattern));
    }
    assertInvalidPattern(
        "1,{2,3},1", String.format(QueryMessages.INVALID_PATTERN_MISSING_REPEAT_SIGN, "1,{2,3},1"));
    assertInvalidPattern(
        "1,--2,3",
        String.format(
            QueryMessages.EXCEPTION_INVALID_PATTERN_ARG_INVALID_NUMERIC_DATA_POINT_ARG_12165323,
            "1,--2,3",
            "--2"));
  }

  @Test
  public void testAcceptNonEmptyRepetitionGroups() {
    for (String pattern :
        new String[] {"1,{2,3,2}+,1", "1,{,2,3,2,}*,1", "1,{{2,3,2}+}+,1", "1,{2,{3,2}+,1}+,1"}) {
      assertNotNull(createProvider(pattern).getDataProcessor());
    }
  }

  private void assertInvalidPattern(String pattern, String expectedMessage) {
    UDFException exception =
        assertThrows(
            UDFException.class,
            () -> new PatternMatchTableFunction().analyze(createArguments(pattern)));
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testPartitionStateIsIsolated() {
    assertPartitionStateIsIsolated("1,2,3,2,1");
  }

  @Test
  public void testRegexPartitionStateIsIsolated() {
    assertPartitionStateIsIsolated("1,{2,3,2}+,1");
  }

  @Test
  public void testSingletonPartitions() {
    assertSingletonPartitions("1,2,3,2,1");
  }

  @Test
  public void testRegexSingletonPartitions() {
    assertSingletonPartitions("1,{2,3,2}+,1");
  }

  private void assertSingletonPartitions(String pattern) {
    assertPartitionStateIsIsolated(
        pattern, new double[][] {{1}, {1, 2, 3, 2, 1}, {10}, {10, 20, 30, 20, 10}, {1}});
  }

  private void assertPartitionStateIsIsolated(String pattern) {
    assertPartitionStateIsIsolated(
        pattern, new double[][] {{1, 2, 3, 2, 1}, {10, 20, 30, 20, 10}, {2, 3, 2, 1}});
  }

  private void assertPartitionStateIsIsolated(String pattern, double[][] partitions) {
    TableFunctionProcessorProvider provider = createProvider(pattern);
    for (double[] values : partitions) {
      Column[] expected = processPartition(createProvider(pattern).getDataProcessor(), values);
      Column[] actual = processPartition(provider.getDataProcessor(), values);
      int expectedRows = values.length == 1 ? 0 : values.length;
      for (int column = 0; column < actual.length; column++) {
        assertEquals(expectedRows, expected[column].getPositionCount());
        assertEquals(expectedRows, actual[column].getPositionCount());
      }
      for (int i = 0; i < expectedRows; i++) {
        assertEquals(i, actual[2].getLong(i));
        assertEquals(expected[0].getInt(i), actual[0].getInt(i));
        assertEquals(expected[1].getDouble(i), actual[1].getDouble(i), 1e-9);
      }
    }
  }

  @Test
  public void testCreatingProcessorDoesNotResetAnotherPartition() {
    TableFunctionProcessorProvider provider = createProvider("1,2,3,2,1");
    TableFunctionDataProcessor first = provider.getDataProcessor();
    first.beforeStart();
    List<ColumnBuilder> properBuilders = createProperBuilders();
    ColumnBuilder indexes = new LongColumnBuilder(null, 5);
    first.process(record(1, 1), properBuilders, indexes);
    first.process(record(2, 2), properBuilders, indexes);

    Column[] second =
        processPartition(provider.getDataProcessor(), new double[] {10, 20, 30, 20, 10});

    first.process(record(3, 3), properBuilders, indexes);
    first.process(record(4, 2), properBuilders, indexes);
    first.process(record(5, 1), properBuilders, indexes);
    first.finish(properBuilders, indexes);
    first.beforeDestroy();

    Column[] expected =
        processPartition(
            createProvider("1,2,3,2,1").getDataProcessor(), new double[] {1, 2, 3, 2, 1});
    Column firstIndexes = indexes.build();
    Column firstSimilarities = properBuilders.get(1).build();
    assertEquals(5, second[2].getPositionCount());
    assertEquals(5, firstIndexes.getPositionCount());
    for (int i = 0; i < 5; i++) {
      assertEquals(i, second[2].getLong(i));
      assertEquals(i, firstIndexes.getLong(i));
      assertEquals(expected[1].getDouble(i), firstSimilarities.getDouble(i), 1e-9);
    }
  }

  private TableFunctionProcessorProvider createProvider(String pattern) {
    PatternMatchTableFunction function = new PatternMatchTableFunction();
    return function.getProcessorProvider(
        function.analyze(createArguments(pattern)).getTableFunctionHandle());
  }

  private Map<String, Argument> createArguments(String pattern) {
    Map<String, Argument> arguments = new HashMap<>();
    arguments.put(
        "DATA",
        new TableArgument(
            Arrays.asList(Optional.of("time"), Optional.of("value")),
            Arrays.asList(Type.TIMESTAMP, Type.DOUBLE),
            Collections.emptyList(),
            Collections.singletonList("time"),
            false));
    arguments.put("TIME_COL", new ScalarArgument(Type.STRING, "time"));
    arguments.put("DATA_COL", new ScalarArgument(Type.STRING, "value"));
    arguments.put("PATTERN", new ScalarArgument(Type.STRING, pattern));
    arguments.put("SMOOTH", new ScalarArgument(Type.DOUBLE, 0.0));
    arguments.put("THRESHOLD", new ScalarArgument(Type.DOUBLE, 100.0));
    arguments.put("WIDTH", new ScalarArgument(Type.DOUBLE, 1000.0));
    arguments.put("HEIGHT", new ScalarArgument(Type.DOUBLE, 1000.0));
    arguments.put("SMOOTH_ON_PATTERN", new ScalarArgument(Type.BOOLEAN, false));
    return arguments;
  }

  private Column[] processPartition(TableFunctionDataProcessor processor, double[] values) {
    List<ColumnBuilder> properBuilders = createProperBuilders();
    ColumnBuilder indexes = new LongColumnBuilder(null, values.length);
    processor.beforeStart();
    for (int i = 0; i < values.length; i++) {
      processor.process(record(i + 1, values[i]), properBuilders, indexes);
    }
    processor.finish(properBuilders, indexes);
    processor.beforeDestroy();
    return new Column[] {
      properBuilders.get(0).build(), properBuilders.get(1).build(), indexes.build()
    };
  }

  private List<ColumnBuilder> createProperBuilders() {
    return Arrays.asList(new IntColumnBuilder(null, 5), new DoubleColumnBuilder(null, 5));
  }

  private Record record(long time, double value) {
    Record record = mock(Record.class);
    when(record.getLong(0)).thenReturn(time);
    when(record.getDouble(1)).thenReturn(value);
    return record;
  }
}
