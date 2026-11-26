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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.library.util.UdfTestFakes;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UDTFHistogramTest {

  private void feed(
      UDTFHistogram udf,
      UdfTestFakes.RecordingPointCollector collector,
      long[] times,
      double[] values)
      throws Exception {
    for (int i = 0; i < times.length; i++) {
      udf.transform(UdfTestFakes.rowOf(times[i], values[i]), collector);
    }
  }

  private UDTFHistogram createUdf(Map<String, String> attributes) throws Exception {
    UDTFHistogram udf = new UDTFHistogram();
    UDFParameters parameters = UdfTestFakes.parameters(new HashMap<>(attributes));
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, UdfTestFakes.configurations());
    return udf;
  }

  @Test
  public void uniformRangeBinningIsUnchanged() throws Exception {
    UDTFHistogram udf = createUdf(Map.of("count", "10", "min", "0.0", "max", "10.0"));
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(
        udf,
        collector,
        new long[] {1, 2, 3, 4, 5, 6, 7, 8},
        new double[] {0.5, 1, 2, 5, 9, 10, -1, 11});
    udf.terminate(collector);

    // 0.5 and -1 clamp into bucket 0; 1 -> 1; 2 -> 2; 5 -> 5; 9, 10 and 11 -> bucket 9
    assertEquals(2, collector.getInts().get(0L).intValue());
    assertEquals(1, collector.getInts().get(1L).intValue());
    assertEquals(1, collector.getInts().get(2L).intValue());
    assertEquals(1, collector.getInts().get(5L).intValue());
    assertEquals(3, collector.getInts().get(9L).intValue());
    assertEquals(0, collector.getInts().get(3L).intValue());
    assertEquals(0, collector.getInts().get(8L).intValue());
  }

  @Test
  public void overflowingDefaultRangeKeepsEveryBucketReachable() throws Exception {
    UDTFHistogram udf = createUdf(Map.of("count", "10"));
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(
        udf,
        collector,
        new long[] {1, 2, 3},
        new double[] {-Double.MAX_VALUE, 0.0, Double.MAX_VALUE});
    udf.terminate(collector);

    // (max - min) overflows to +Infinity with the default range; the mapping must stay
    // monotonic with the lower bound, the center and the upper bound in distinct buckets
    assertEquals(1, collector.getInts().get(0L).intValue());
    assertEquals(1, collector.getInts().get(5L).intValue());
    assertEquals(1, collector.getInts().get(9L).intValue());
    for (int i = 1; i <= 4; i++) {
      assertEquals(0, collector.getInts().get((long) i).intValue());
    }
    for (int i = 6; i <= 8; i++) {
      assertEquals(0, collector.getInts().get((long) i).intValue());
    }
  }

  @Test
  public void defaultRangeBoundaryKeepsNegativeValuesOnTheirSide() throws Exception {
    // With the default [-MAX, MAX] range and count=2, zero is the boundary between the two
    // buckets: -1 must stay in the lower bucket instead of rounding onto the boundary.
    UDTFHistogram udf = createUdf(Map.of("count", "2"));
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2, 3}, new double[] {-1, 0, 1});
    udf.terminate(collector);

    assertEquals(1, collector.getInts().get(0L).intValue());
    assertEquals(2, collector.getInts().get(1L).intValue());
  }
}
