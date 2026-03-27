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

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Tests {@link UDAFPercentile} exact (error=0) and approximate (GK sketch) paths. */
public class UDAFPercentileTest {

  @Test
  public void exactDoubleDiscreteNearestRankMedian() throws Exception {
    // Sorted 1..5, phi=0.5 -> ceil(5*0.5)-1 = 2 -> value 3.0
    runExactDouble(new double[] {5, 1, 4, 2, 3}, new long[] {50, 10, 40, 20, 30}, "0.5", 3.0, 30L);
  }

  /** User-style check: ordered inputs 1..5, rank=0.5 -> discrete rank 3. */
  @Test
  public void exactSequentialOneToFiveRankHalfReturnsThree() throws Exception {
    runExactDouble(new double[] {1, 2, 3, 4, 5}, new long[] {10, 20, 30, 40, 50}, "0.5", 3.0, 30L);
  }

  @Test
  public void exactDoubleRankOne() throws Exception {
    // n=5, phi=1 -> ceil(5)-1 = 4 -> max value 5
    runExactDouble(new double[] {1, 2, 3, 4, 5}, new long[] {1, 2, 3, 4, 5}, "1", 5.0, 5L);
  }

  @Test
  public void exactIntDiscreteNearestRank() throws Exception {
    UDAFPercentile udf = new UDAFPercentile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("error", "0");
    attrs.put("rank", "0.5");
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.INT32), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    AtomicInteger rowIdx = new AtomicInteger(0);
    int[] values = {5, 1, 4, 2, 3};
    long[] times = {50, 10, 40, 20, 30};
    Mockito.when(row.getInt(0)).thenAnswer(inv -> values[rowIdx.get()]);
    Mockito.when(row.getTime())
        .thenAnswer(
            inv -> {
              long t = times[rowIdx.get()];
              rowIdx.incrementAndGet();
              return t;
            });

    PointCollector collector = Mockito.mock(PointCollector.class);
    for (int i = 0; i < values.length; i++) {
      udf.transform(row, collector);
    }

    Mockito.doAnswer(
            inv -> {
              Assert.assertEquals(3, (int) inv.getArgument(1));
              Assert.assertEquals(30L, (long) inv.getArgument(0));
              return null;
            })
        .when(collector)
        .putInt(Mockito.anyLong(), Mockito.anyInt());

    udf.terminate(collector);
    Mockito.verify(collector, Mockito.times(1)).putInt(Mockito.anyLong(), Mockito.anyInt());
  }

  private void runExactDouble(
      double[] values, long[] times, String rank, double expectedValue, long expectedTime)
      throws Exception {
    UDAFPercentile udf = new UDAFPercentile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("error", "0");
    attrs.put("rank", rank);
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.DOUBLE), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    AtomicInteger rowIdx = new AtomicInteger(0);
    Mockito.when(row.getDouble(0)).thenAnswer(inv -> values[rowIdx.get()]);
    Mockito.when(row.getTime())
        .thenAnswer(
            inv -> {
              long t = times[rowIdx.get()];
              rowIdx.incrementAndGet();
              return t;
            });

    PointCollector collector = Mockito.mock(PointCollector.class);
    for (int i = 0; i < values.length; i++) {
      udf.transform(row, collector);
    }

    Mockito.doAnswer(
            inv -> {
              Assert.assertEquals(expectedValue, (double) inv.getArgument(1), 0.0);
              Assert.assertEquals(expectedTime, (long) inv.getArgument(0));
              return null;
            })
        .when(collector)
        .putDouble(Mockito.anyLong(), Mockito.anyDouble());

    udf.terminate(collector);
    Mockito.verify(collector, Mockito.times(1)).putDouble(Mockito.anyLong(), Mockito.anyDouble());
  }

  @Test
  public void approximateDoubleManyPointsNoCrash() throws Exception {
    UDAFPercentile udf = new UDAFPercentile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("error", "0.01");
    attrs.put("rank", "0.5");
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.DOUBLE), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    java.util.Random rnd = new java.util.Random(42);
    Mockito.when(row.getDataType(0)).thenReturn(Type.DOUBLE);
    Mockito.when(row.getDouble(0)).thenAnswer(inv -> rnd.nextDouble());

    PointCollector collector = Mockito.mock(PointCollector.class);
    final double[] captured = new double[1];
    Mockito.doAnswer(
            inv -> {
              captured[0] = inv.getArgument(1);
              return null;
            })
        .when(collector)
        .putDouble(Mockito.anyLong(), Mockito.anyDouble());

    for (int i = 0; i < 8000; i++) {
      udf.transform(row, collector);
    }

    udf.terminate(collector);
    Assert.assertTrue(captured[0] >= 0.0 && captured[0] <= 1.0);
  }

  /**
   * Approximate path (GK): same five points as exact median; result must stay in sample range (not
   * necessarily 3 — sketch is approximate).
   */
  @Test
  public void approximateOneToFiveRankHalfStaysInSampleRange() throws Exception {
    UDAFPercentile udf = new UDAFPercentile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("error", "0.01");
    attrs.put("rank", "0.5");
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.DOUBLE), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getDataType(0)).thenReturn(Type.DOUBLE);
    double[] values = {1, 2, 3, 4, 5};
    AtomicInteger k = new AtomicInteger(0);
    Mockito.when(row.getDouble(0)).thenAnswer(inv -> values[k.getAndIncrement()]);

    PointCollector collector = Mockito.mock(PointCollector.class);
    final double[] captured = new double[1];
    Mockito.doAnswer(
            inv -> {
              captured[0] = inv.getArgument(1);
              return null;
            })
        .when(collector)
        .putDouble(Mockito.anyLong(), Mockito.anyDouble());

    for (int i = 0; i < values.length; i++) {
      udf.transform(row, collector);
    }
    udf.terminate(collector);

    Assert.assertTrue(captured[0] >= 1.0 && captured[0] <= 5.0);
  }

  /**
   * Approximate path with many points: values 1..N, rank 0.5 should be near (N+1)/2; allow slack
   * for GK error parameter.
   */
  @Test
  public void approximateMedianOfOneToTwoThousandNearMiddle() throws Exception {
    final int n = 2000;
    UDAFPercentile udf = new UDAFPercentile();
    Map<String, String> attrs = new HashMap<>();
    attrs.put("error", "0.01");
    attrs.put("rank", "0.5");
    UDFParameters params =
        new UDFParameters(
            Collections.singletonList("s"), Collections.singletonList(Type.DOUBLE), attrs);
    UDTFConfigurations config = new UDTFConfigurations(ZoneId.systemDefault());
    udf.beforeStart(params, config);

    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getDataType(0)).thenReturn(Type.DOUBLE);
    AtomicInteger seq = new AtomicInteger(0);
    // Approximate path calls Util.getValueAsDouble once per row (no second getDouble / getTime).
    Mockito.when(row.getDouble(0)).thenAnswer(inv -> (double) seq.incrementAndGet());

    PointCollector collector = Mockito.mock(PointCollector.class);
    final double[] captured = new double[1];
    Mockito.doAnswer(
            inv -> {
              captured[0] = inv.getArgument(1);
              return null;
            })
        .when(collector)
        .putDouble(Mockito.anyLong(), Mockito.anyDouble());

    for (int i = 0; i < n; i++) {
      udf.transform(row, collector);
    }
    udf.terminate(collector);

    double expectedMid = (n + 1) / 2.0;
    Assert.assertEquals(expectedMid, captured[0], 250.0);
  }
}
