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

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LTTBTableFunctionTest {

  @Test
  public void testTargetCountPreservesEndpointsAndPeak() {
    long[] times = {0, 1, 2, 3, 4, 5};
    double[] values = {0, 1, 8, 2, 0, 0};

    assertArrayEquals(
        new int[] {0, 2, 5}, LTTBTableFunction.selectTargetCount(times, values, 6, 3));
  }

  @Test
  public void testTargetCountReturnsAllShortSeries() {
    long[] times = {0, 1};
    double[] values = {1, 2};

    assertArrayEquals(new int[] {0, 1}, LTTBTableFunction.selectTargetCount(times, values, 2, 3));
  }

  @Test
  public void testTargetCountBreaksAreaTiesByStableInputOrder() {
    long[] times = {0, 1, 2, 3, 4};
    double[] values = {0, 4, 4, 1, 0};

    assertArrayEquals(
        new int[] {0, 1, 4}, LTTBTableFunction.selectTargetCount(times, values, 5, 3));
  }

  @Test
  public void testAnalyzeTargetCountOutputSchema() throws UDFException {
    LTTBTableFunction function = new LTTBTableFunction();
    TableFunctionAnalysis analysis = function.analyze(targetCountArguments(3L));

    assertEquals(
        "window_index", analysis.getProperColumnSchema().get().getFields().get(0).getName().get());
    assertEquals(
        "device", analysis.getProperColumnSchema().get().getFields().get(1).getName().get());
    assertEquals(
        "s1_time", analysis.getProperColumnSchema().get().getFields().get(2).getName().get());
    assertEquals("s1", analysis.getProperColumnSchema().get().getFields().get(3).getName().get());
    assertEquals(
        Arrays.asList(0, 1, 2),
        analysis.getRequiredColumns().get(LTTBTableFunction.DATA_PARAMETER_NAME));
  }

  @Test
  public void testAnalyzeRejectsTargetCountBelowThree() throws UDFException {
    try {
      new LTTBTableFunction().analyze(targetCountArguments(2L));
      fail();
    } catch (SemanticException e) {
      // compare against the constant so the test also passes under -P with-zh-locale
      assertEquals(
          CommonMessages.EXCEPTION_THE_N_ARGUMENT_OF_LTTB_MUST_BE_AT_LEAST_3_29CE2B87,
          e.getMessage());
    }
  }

  @Test
  public void testAnalyzeRejectsNAndSizeTogether() throws UDFException {
    Map<String, Argument> arguments = targetCountArguments(3L);
    arguments.put(LTTBTableFunction.SIZE_PARAMETER_NAME, new ScalarArgument(Type.INT64, 2L));
    try {
      new LTTBTableFunction().analyze(arguments);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          CommonMessages
              .EXCEPTION_EXACTLY_ONE_OF_THE_N_AND_SIZE_ARGUMENTS_MUST_BE_SPECIFIED_FOR_LTTB_54AF0733,
          e.getMessage());
    }
  }

  @Test
  public void testTargetCountKeepsNonFiniteValues() {
    long[] times = {0, 1, 2};
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

    // three eligible points with N => 3: every point must be returned, not discarded
    assertArrayEquals(
        new int[] {0, 1, 2}, LTTBTableFunction.selectTargetCount(times, values, 3, 3));
  }

  /**
   * With {@code N < M} the algorithm computes triangle areas, so a non-finite participant reaches
   * the area comparison. NaN and the infinities cannot form a finite area and {@link
   * java.math.BigDecimal} cannot represent them, so such candidates must be skipped (earliest wins)
   * instead of aborting the query with an unchecked {@code NumberFormatException}.
   */
  @Test
  public void testTargetCountSkipsNonFiniteCandidatesWhenSampling() {
    long[] times = {0, 1, 2, 3, 4, 5};
    double[] values = {1.0, Double.NaN, Double.POSITIVE_INFINITY, 2.0, 3.0, 4.0};

    int[] selected = LTTBTableFunction.selectTargetCount(times, values, 6, 3);

    // The first and last eligible points are always preserved.
    assertEquals(0, selected[0]);
    assertEquals(5, selected[selected.length - 1]);
    assertEquals(3, selected.length);
    for (int index : selected) {
      assertTrue("selected index out of range: " + index, index >= 0 && index < 6);
    }
  }

  /** A bucket whose candidates are all non-finite keeps its earliest point, deterministically. */
  @Test
  public void testTargetCountKeepsEarliestPointWhenAllCandidatesAreNonFinite() {
    long[] times = {0, 1, 2, 3, 4, 5};
    double[] values = {
      Double.NaN,
      Double.NaN,
      Double.POSITIVE_INFINITY,
      Double.NaN,
      Double.NEGATIVE_INFINITY,
      Double.NaN
    };

    assertArrayEquals(
        new int[] {0, 1, 5}, LTTBTableFunction.selectTargetCount(times, values, 6, 3));
  }

  /**
   * {@code C} must never be a non-finite point, because every area of the bucket would then be
   * undefined and the bucket would fall back to its earliest point. A non-finite final point
   * therefore hands {@code C} to the last finite point of the partition instead.
   */
  @Test
  public void testTargetCountNonFiniteFinalPointDoesNotBecomeLookahead() {
    long[] times = {0, 1, 2, 3, 4, 5};
    assertArrayEquals(
        new int[] {0, 2, 5},
        LTTBTableFunction.selectTargetCount(times, new double[] {0, 1, 9, 2, 0, Double.NaN}, 6, 3));
    assertArrayEquals(
        new int[] {0, 2, 5},
        LTTBTableFunction.selectTargetCount(
            times, new double[] {0, 1, 9, 2, 0, Double.POSITIVE_INFINITY}, 6, 3));
    assertArrayEquals(
        new int[] {0, 2, 5},
        LTTBTableFunction.selectTargetCount(
            times, new double[] {0, 1, 9, 2, Double.NaN, Double.NaN}, 6, 3));
    // Control: a finite final point is C of the last bucket and selects the same peak.
    assertArrayEquals(
        new int[] {0, 2, 5},
        LTTBTableFunction.selectTargetCount(times, new double[] {0, 1, 9, 2, 0, 0}, 6, 3));
  }

  /**
   * A next bucket without a finite point is skipped: {@code C} averages the finite points of the
   * next bucket that has one instead of collapsing to the last finite point of the partition.
   */
  @Test
  public void testTargetCountSkipsNonFiniteLookaheadBuckets() {
    long[] times = {0, 1, 2, 3, 4, 5, 6, 7};
    double[] values = {0, 5, 4, Double.NaN, Double.NaN, 10, 10, -20};

    // N => 5: C of the first bucket is (t5 + t6) / 2 = (5.5, 10), so t1 (area 17.5) beats t2
    // (area 2). C of the last bucket is the last finite point t6.
    assertArrayEquals(
        new int[] {0, 1, 3, 6, 7},
        LTTBTableFunction.selectTargetCount(times, values, times.length, 5));
    // N => 4: buckets [1,4) and [4,7); the first one skips its NaN point and picks t1, and the
    // second one scores t5 and t6 against the final point t7.
    assertArrayEquals(
        new int[] {0, 1, 6, 7},
        LTTBTableFunction.selectTargetCount(times, values, times.length, 4));
  }

  @Test
  public void testTimestampUniquenessAcceptsDistinctAndRejectsRepeatedTimestamps() {
    LTTBTableFunction.TimestampUniquenessChecker checker =
        new LTTBTableFunction.TimestampUniquenessChecker();
    checker.check(1L);
    checker.check(2L);
    checker.check(3L);
    try {
      checker.check(3L);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_DOES_NOT_SUPPORT_DUPLICATE_TIMESTAMPS_BUT_TIMESTAMP_ARG_APPEARS_MORE_THAN_ONCE_IN_THE_SAME_PARTITION_7713FEDD,
              3L),
          e.getMessage());
    }
  }

  /** The duplicate check must survive the block boundary: the state outlives one block's rows. */
  @Test
  public void testTimestampUniquenessSpansInputBlocks() {
    LTTBTableFunction.TimestampUniquenessChecker checker =
        new LTTBTableFunction.TimestampUniquenessChecker();
    checker.check(7L); // last row of the first input block
    try {
      checker.check(7L); // first row of the second input block
      fail();
    } catch (SemanticException e) {
      assertEquals(
          String.format(
              CommonMessages
                  .EXCEPTION_LTTB_DOES_NOT_SUPPORT_DUPLICATE_TIMESTAMPS_BUT_TIMESTAMP_ARG_APPEARS_MORE_THAN_ONCE_IN_THE_SAME_PARTITION_7713FEDD,
              7L),
          e.getMessage());
    }
  }

  private static Map<String, Argument> targetCountArguments(long n) {
    Map<String, Argument> arguments = new HashMap<>();
    arguments.put(
        LTTBTableFunction.DATA_PARAMETER_NAME,
        new TableArgument(
            Arrays.asList(Optional.of("time"), Optional.of("device"), Optional.of("s1")),
            Arrays.asList(Type.TIMESTAMP, Type.STRING, Type.DOUBLE),
            Collections.singletonList("device"),
            Collections.singletonList("time"),
            false));
    arguments.put(
        LTTBTableFunction.TIMECOL_PARAMETER_NAME, new ScalarArgument(Type.STRING, "time"));
    arguments.put(LTTBTableFunction.N_PARAMETER_NAME, new ScalarArgument(Type.INT64, n));
    arguments.put(
        LTTBTableFunction.SIZE_PARAMETER_NAME,
        new ScalarArgument(Type.INT64, LTTBTableFunction.UNSPECIFIED_SIZE));
    arguments.put(
        LTTBTableFunction.SLIDE_PARAMETER_NAME,
        new ScalarArgument(Type.INT64, LTTBTableFunction.UNSPECIFIED_SLIDE));
    arguments.put(LTTBTableFunction.ORIGIN_PARAMETER_NAME, new ScalarArgument(Type.TIMESTAMP, 0L));
    arguments.put(
        LTTBTableFunction.WINDOW_MODE_PARAMETER_NAME, new ScalarArgument(Type.BOOLEAN, false));
    return arguments;
  }
}
