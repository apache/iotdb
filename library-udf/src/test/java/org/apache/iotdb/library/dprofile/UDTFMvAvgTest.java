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

public class UDTFMvAvgTest {

  private void feed(
      UDTFMvAvg udf, UdfTestFakes.RecordingPointCollector collector, long[] times, double[] values)
      throws Exception {
    for (int i = 0; i < times.length; i++) {
      udf.transform(UdfTestFakes.rowOf(times[i], values[i]), collector);
    }
  }

  private UDTFMvAvg createUdf(int windowSize) throws Exception {
    UDTFMvAvg udf = new UDTFMvAvg();
    UDFParameters parameters =
        UdfTestFakes.parameters(new HashMap<>(Map.of("window", String.valueOf(windowSize))));
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, UdfTestFakes.configurations());
    return udf;
  }

  @Test
  public void movingAverageAveragesTheWholeWindow() throws Exception {
    UDTFMvAvg udf = createUdf(3);
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2, 3, 4, 5}, new double[] {1, 2, 3, 4, 5});
    udf.terminate(collector);

    // window=3 over 1,2,3,4,5: means of {1,2,3}, {2,3,4}, {3,4,5}
    assertEquals(3, collector.getDoubles().size());
    assertEquals(2.0, collector.getDoubles().get(3L), 1e-9);
    assertEquals(3.0, collector.getDoubles().get(4L), 1e-9);
    assertEquals(4.0, collector.getDoubles().get(5L), 1e-9);
  }

  @Test
  public void nonFiniteValuesDoNotAdvanceTheWindow() throws Exception {
    UDTFMvAvg udf = createUdf(2);
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2, 3}, new double[] {1, Double.NaN, 3});
    udf.terminate(collector);

    // the NaN row is skipped, so the first full window is {1,3} at t=3
    assertEquals(1, collector.getDoubles().size());
    assertEquals(2.0, collector.getDoubles().get(3L), 1e-9);
  }

  @Test
  public void evictedValueDoesNotLeakRoundingErrorIntoLaterWindows() throws Exception {
    UDTFMvAvg udf = createUdf(2);
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    double twoPow53 = Math.pow(2, 53);
    feed(udf, collector, new long[] {1, 2, 3, 4, 5}, new double[] {twoPow53, 1, 0, 0, 0});
    udf.terminate(collector);

    // 2^53 + 1 loses the unit; a running sum would then stay 1 too low after evicting 1 and
    // make both [0, 0] windows -0.5.
    assertEquals(0.5, collector.getDoubles().get(3L), 0.0);
    assertEquals(0.0, collector.getDoubles().get(4L), 0.0);
    assertEquals(0.0, collector.getDoubles().get(5L), 0.0);
  }

  @Test
  public void overflowingWindowRecoversForLaterWindows() throws Exception {
    UDTFMvAvg udf = createUdf(2);
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(
        udf,
        collector,
        new long[] {1, 2, 3, 4},
        new double[] {Double.MAX_VALUE, Double.MAX_VALUE, 1, 1});
    udf.terminate(collector);

    // the sum of two Double.MAX_VALUE values overflows, but the mean is still representable
    assertEquals(Double.MAX_VALUE, collector.getDoubles().get(2L), 0.0);
    // and the later [1, 1] window must not inherit an Infinity from the overflow
    assertEquals(1.0, collector.getDoubles().get(4L), 0.0);
  }
}
