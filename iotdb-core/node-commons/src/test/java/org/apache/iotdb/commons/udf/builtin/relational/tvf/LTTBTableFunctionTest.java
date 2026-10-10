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
