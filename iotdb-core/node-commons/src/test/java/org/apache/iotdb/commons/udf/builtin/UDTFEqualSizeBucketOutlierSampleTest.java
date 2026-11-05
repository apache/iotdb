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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UDTFEqualSizeBucketOutlierSampleTest {

  @Test
  public void testInt64CosPreservesSmallDifferences() throws Exception {
    assertSelectsCentralSpike("cos");
  }

  @Test
  public void testInt64PrenextdisPreservesSmallDifferences() throws Exception {
    assertSelectsCentralSpike("prenextdis");
  }

  @Test
  public void testInt64StendisPreservesSmallDifferences() throws Exception {
    assertSelectsCentralSpike("stendis");
  }

  private static void assertSelectsCentralSpike(String type) throws Exception {
    long base = 1L << 60;
    Map<String, String> attributes = new HashMap<>();
    attributes.put("type", type);
    attributes.put("number", "1");
    attributes.put("proportion", "0.2");
    UDFParameters parameters =
        new UDFParameters(
            Collections.singletonList("s1"), Collections.singletonList(Type.INT64), attributes);
    UDTFEqualSizeBucketOutlierSample udf = new UDTFEqualSizeBucketOutlierSample();
    udf.validate(new UDFParameterValidator(parameters));
    udf.beforeStart(parameters, new UDTFConfigurations(ZoneId.of("UTC")));

    long[] values = {base, base + 1, base + 10, base + 1, base};
    RowWindow window = Mockito.mock(RowWindow.class);
    Mockito.when(window.windowSize()).thenReturn(values.length);
    for (int i = 0; i < values.length; i++) {
      Row row = Mockito.mock(Row.class);
      Mockito.when(row.getTime()).thenReturn((long) i);
      Mockito.when(row.getLong(0)).thenReturn(values[i]);
      Mockito.when(window.getRow(i)).thenReturn(row);
    }
    PointCollector collector = Mockito.mock(PointCollector.class);
    udf.transform(window, collector);

    Mockito.verify(collector).putLong(2L, base + 10);
    Mockito.verify(collector, Mockito.times(1)).putLong(Mockito.anyLong(), Mockito.anyLong());
  }
}
