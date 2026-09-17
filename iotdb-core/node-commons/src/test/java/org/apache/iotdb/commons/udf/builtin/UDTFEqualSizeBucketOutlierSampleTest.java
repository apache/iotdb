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
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Test;
import org.mockito.InOrder;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UDTFEqualSizeBucketOutlierSampleTest {

  private static final long[] VALUES = {
    1, -1, 3, 11, 9, 1531604122307244742L, -8581625725655917595L, -7162825364312197604L, 0, 1
  };

  @Test
  public void testAvgWithLargeLongValues() throws Exception {
    assertSamples("avg", new long[][] {{7, -8581625725655917595L}, {8, -7162825364312197604L}});
  }

  @Test
  public void testStendisWithLargeLongValues() throws Exception {
    assertSamples("stendis", new long[][] {{7, -8581625725655917595L}, {8, -7162825364312197604L}});
  }

  @Test
  public void testCosWithLargeLongValues() throws Exception {
    // Integer overflow used to select times 4 and 5 instead of the large outliers at 6 and 7.
    assertSamples("cos", new long[][] {{6, 1531604122307244742L}, {7, -8581625725655917595L}});
  }

  @Test
  public void testPrenextdisWithLargeLongValues() throws Exception {
    assertSamples(
        "prenextdis", new long[][] {{6, 1531604122307244742L}, {7, -8581625725655917595L}});
  }

  private void assertSamples(String type, long[][] expected) throws Exception {
    assertSamples(type, VALUES, expected);
  }

  @Test
  public void testStendisWithOverflowingEndpointProduct() throws Exception {
    assertSamples(
        "stendis",
        new long[] {
          4000000000000000000L,
          5000000000000000000L,
          4000000000000000000L,
          2000000000000000000L,
          4000000000000000000L
        },
        new long[][] {{2, 5000000000000000000L}, {4, 2000000000000000000L}});
  }

  @Test
  public void testPrenextdisWithLongMinValue() throws Exception {
    assertSamples(
        "prenextdis",
        new long[] {0, Long.MIN_VALUE, 0, 5, 0},
        new long[][] {{2, Long.MIN_VALUE}, {3, 0}});
  }

  private void assertSamples(String type, long[] values, long[][] expected) throws Exception {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("proportion", "0.1");
    attributes.put("type", type);
    attributes.put("number", "2");
    UDFParameters parameters =
        new UDFParameters(
            Collections.singletonList("root.sg.d1.s1"),
            Collections.singletonList(Type.INT64),
            attributes);
    UDTFEqualSizeBucketOutlierSample function = new UDTFEqualSizeBucketOutlierSample();
    UDTFConfigurations configurations = new UDTFConfigurations(ZoneOffset.UTC);
    function.validate(new UDFParameterValidator(parameters));
    function.beforeStart(parameters, configurations);
    assertEquals(Type.INT64, configurations.getOutputDataType());
    assertEquals(
        20, ((SlidingSizeWindowAccessStrategy) configurations.getAccessStrategy()).getWindowSize());

    // The input points form the final, partially filled bucket.
    RowWindow window = mock(RowWindow.class);
    when(window.windowSize()).thenReturn(values.length);
    for (int i = 0; i < values.length; i++) {
      Row row = mock(Row.class);
      when(row.getTime()).thenReturn(i + 1L);
      when(row.getLong(0)).thenReturn(values[i]);
      when(window.getRow(i)).thenReturn(row);
    }

    PointCollector collector = mock(PointCollector.class);
    function.transform(window, collector);
    function.terminate(collector);

    InOrder order = inOrder(collector);
    for (long[] point : expected) {
      order.verify(collector).putLong(point[0], point[1]);
    }
    verifyNoMoreInteractions(collector);
  }
}
