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

public class UDTFSegmentTest {

  private UDTFSegment createUdf() throws Exception {
    UDTFSegment udf = new UDTFSegment();
    UDFParameters parameters = UdfTestFakes.parameters(new HashMap<>(Map.of("output", "all")));
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, UdfTestFakes.configurations());
    return udf;
  }

  @Test
  public void twoInstancesKeepTheirOwnPointsWithOutputAll() throws Exception {
    UDTFSegment a = createUdf();
    UdfTestFakes.RecordingPointCollector collectorA = new UdfTestFakes.RecordingPointCollector();
    a.transform(UdfTestFakes.rowOf(1000L, 1.0), collectorA);

    // starting B must neither clear A's buffered point nor share the buffer with A
    UDTFSegment b = createUdf();
    UdfTestFakes.RecordingPointCollector collectorB = new UdfTestFakes.RecordingPointCollector();
    b.transform(UdfTestFakes.rowOf(2000L, 2.0), collectorB);

    a.transform(UdfTestFakes.rowOf(3000L, 3.0), collectorA);

    a.terminate(collectorA);
    b.terminate(collectorB);

    // exactly two points per instance, no point of the other instance
    assertEquals(2, collectorA.getDoubles().size());
    assertEquals(1.0, collectorA.getDoubles().get(1000L), 0.0);
    assertEquals(3.0, collectorA.getDoubles().get(3000L), 0.0);

    assertEquals(1, collectorB.getDoubles().size());
    assertEquals(2.0, collectorB.getDoubles().get(2000L), 0.0);
  }
}
