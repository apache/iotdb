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
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Test;
import org.mockito.InOrder;

import java.time.ZoneOffset;
import java.util.Collections;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UDTFTimeDifferenceTest {

  @Test
  public void testOverflowDoesNotCorruptFollowingDifference() throws Exception {
    assertDifferences(
        new long[] {Long.MIN_VALUE, 0, 1, Long.MAX_VALUE},
        new long[] {Long.MAX_VALUE, 1, Long.MAX_VALUE - 1});
  }

  @Test
  public void testSmallDifferencesNearLongMaxValueRemainExact() throws Exception {
    assertDifferences(
        new long[] {Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE}, new long[] {1, 1});
  }

  private void assertDifferences(long[] times, long[] expected) throws Exception {
    UDTFTimeDifference function = new UDTFTimeDifference();
    function.beforeStart(
        new UDFParameters(
            Collections.singletonList("root.sg.d1.s1"),
            Collections.singletonList(Type.INT64),
            Collections.emptyMap()),
        new UDTFConfigurations(ZoneOffset.UTC));
    PointCollector collector = mock(PointCollector.class);
    for (int i = 0; i < times.length; i++) {
      Row row = mock(Row.class);
      when(row.getTime()).thenReturn(times[i]);
      function.transform(row, collector);
      if (i == 0) {
        verifyNoMoreInteractions(collector);
      }
    }
    function.terminate(collector);

    InOrder order = inOrder(collector);
    for (int i = 0; i < expected.length; i++) {
      order.verify(collector).putLong(times[i + 1], expected[i]);
    }
    verifyNoMoreInteractions(collector);
  }
}
