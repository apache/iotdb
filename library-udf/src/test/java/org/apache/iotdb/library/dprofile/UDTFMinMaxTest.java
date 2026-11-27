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
import static org.junit.Assert.assertTrue;

public class UDTFMinMaxTest {

  private void feed(
      UDTFMinMax udf, UdfTestFakes.RecordingPointCollector collector, long[] times, double[] values)
      throws Exception {
    for (int i = 0; i < times.length; i++) {
      udf.transform(UdfTestFakes.rowOf(times[i], values[i]), collector);
    }
  }

  private UDTFMinMax createBatchUdf() throws Exception {
    UDTFMinMax udf = new UDTFMinMax();
    UDFParameters parameters = UdfTestFakes.parameters(new HashMap<>(Map.of("compute", "batch")));
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, UdfTestFakes.configurations());
    return udf;
  }

  @Test
  public void nonFiniteRowsKeepTimestampsAligned() throws Exception {
    UDTFMinMax udf = createBatchUdf();
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2, 3}, new double[] {1, Double.NaN, 3});
    udf.terminate(collector);

    // min=1, max=3: (1-1)/2=0.0 at t=1 and (3-1)/2=1.0 at t=3; the NaN row must not
    // shift the timestamp of any following value
    assertEquals(2, collector.getDoubles().size());
    assertEquals(0.0, collector.getDoubles().get(1L), 1e-9);
    assertEquals(1.0, collector.getDoubles().get(3L), 1e-9);
  }

  @Test
  public void allNonFiniteInputProducesNoOutput() throws Exception {
    UDTFMinMax udf = createBatchUdf();
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2}, new double[] {Double.NaN, Double.NaN});
    udf.terminate(collector);

    assertTrue(collector.getDoubles().isEmpty());
  }

  @Test
  public void batchNormalizationWithoutNonFiniteValues() throws Exception {
    UDTFMinMax udf = createBatchUdf();
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {1, 2}, new double[] {5, 15});
    udf.terminate(collector);

    assertEquals(2, collector.getDoubles().size());
    assertEquals(0.0, collector.getDoubles().get(1L), 1e-9);
    assertEquals(1.0, collector.getDoubles().get(2L), 1e-9);
  }
}
