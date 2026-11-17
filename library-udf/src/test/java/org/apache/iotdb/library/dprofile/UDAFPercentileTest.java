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

public class UDAFPercentileTest {

  private UDAFPercentile createUdf() throws Exception {
    UDAFPercentile udf = new UDAFPercentile();
    UDFParameters parameters =
        UdfTestFakes.parameters(new HashMap<>(Map.of("error", "0", "rank", "0.5")));
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, UdfTestFakes.configurations());
    return udf;
  }

  private void feed(
      UDAFPercentile udf,
      UdfTestFakes.RecordingPointCollector collector,
      long[] times,
      double[] values)
      throws Exception {
    for (int i = 0; i < times.length; i++) {
      udf.transform(UdfTestFakes.rowOf(times[i], values[i]), collector);
    }
  }

  @Test
  public void exactPercentileKeepsTimestampOfMatchedValue() throws Exception {
    UDAFPercentile udf = createUdf();
    UdfTestFakes.RecordingPointCollector collector = new UdfTestFakes.RecordingPointCollector();
    feed(udf, collector, new long[] {100, 200, 300, 400, 500}, new double[] {1, 2, 3, 4, 5});
    udf.terminate(collector);

    // nearest-rank percentile of [1..5] at 0.5 is 3, first seen at t=300
    assertEquals(1, collector.getDoubles().size());
    assertEquals(3.0, collector.getDoubles().get(300L), 1e-9);
  }

  @Test
  public void queryInstancesDoNotShareValueTimestampMaps() throws Exception {
    UDAFPercentile queryA = createUdf();
    UdfTestFakes.RecordingPointCollector collectorA = new UdfTestFakes.RecordingPointCollector();
    feed(queryA, collectorA, new long[] {100, 200, 300, 400, 500}, new double[] {1, 2, 3, 4, 5});

    // a second percentile query starts and receives one row: with shared (static) maps
    // this discards the value -> timestamp mapping query A collected
    UDAFPercentile queryB = createUdf();
    UdfTestFakes.RecordingPointCollector collectorB = new UdfTestFakes.RecordingPointCollector();
    feed(queryB, collectorB, new long[] {600}, new double[] {6});

    // query A terminates: percentile of [1..5] is 3, first seen at t=300
    queryA.terminate(collectorA);
    assertEquals(1, collectorA.getDoubles().size());
    assertEquals(3.0, collectorA.getDoubles().get(300L), 1e-9);
  }
}
