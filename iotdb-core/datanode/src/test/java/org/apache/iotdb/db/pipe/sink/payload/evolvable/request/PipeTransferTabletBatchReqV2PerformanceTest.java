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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class PipeTransferTabletBatchReqV2PerformanceTest {

  private static final String ENABLED_PROPERTY =
      "iotdb.pipe.tablet.batch.v2.deserialize.perf.enabled";
  private static final String COLUMNS_PROPERTY =
      "iotdb.pipe.tablet.batch.v2.deserialize.perf.columns";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.pipe.tablet.batch.v2.deserialize.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY =
      "iotdb.pipe.tablet.batch.v2.deserialize.perf.iterations";

  private static volatile PipeTransferTabletBatchReqV2 benchmarkBlackhole;

  @Test
  public void singleWideRawTabletDeserializationBenchmark() throws IOException {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true, optionally tune -D%s, -D%s and -D%s.",
            ENABLED_PROPERTY, COLUMNS_PROPERTY, WARMUP_ITERATIONS_PROPERTY, ITERATIONS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));

    final int columnCount = Integer.getInteger(COLUMNS_PROPERTY, 100_000);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 1);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 5);
    Assert.assertTrue(columnCount > 0);
    Assert.assertTrue(warmupIterations >= 0);
    Assert.assertTrue(iterations > 0);

    final PipeTransferTabletBatchReqV2 request = createSingleWideRawTabletBatch(columnCount);
    final PipeTransferTabletBatchReqV2 verificationResult = deserialize(request);
    Assert.assertEquals(1, verificationResult.getTabletReqs().size());
    Assert.assertEquals(
        columnCount, verificationResult.getTabletReqs().get(0).getTablet().getSchemas().size());

    for (int i = 0; i < warmupIterations; ++i) {
      deserialize(request);
    }

    final long[] elapsedNanos = new long[iterations];
    for (int i = 0; i < iterations; ++i) {
      final long startTime = System.nanoTime();
      deserialize(request);
      elapsedNanos[i] = System.nanoTime() - startTime;
    }

    System.out.printf(
        Locale.ROOT,
        "Batch V2 single raw tablet deserialization benchmark: columns=%d, warmups=%d, iterations=%d, median=%.3f ms/op%n",
        columnCount,
        warmupIterations,
        iterations,
        median(elapsedNanos) / 1_000_000.0);
  }

  private static PipeTransferTabletBatchReqV2 createSingleWideRawTabletBatch(final int columnCount)
      throws IOException {
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; ++i) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
    }

    final Tablet tablet = new Tablet("root.sg.d", schemas, 1);
    return PipeTransferTabletBatchReqV2.toTPipeTransferReq(
        Collections.emptyList(),
        Collections.singletonList(serializeTablet(tablet)),
        Collections.emptyList(),
        Collections.singletonList("root.sg"));
  }

  private static ByteBuffer serializeTablet(final Tablet tablet) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      tablet.serialize(outputStream);
      ReadWriteIOUtils.write(false, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private static PipeTransferTabletBatchReqV2 deserialize(
      final PipeTransferTabletBatchReqV2 request) {
    request.body.position(0);
    benchmarkBlackhole = PipeTransferTabletBatchReqV2.fromTPipeTransferReq(request);
    return benchmarkBlackhole;
  }

  private static double median(final long[] values) {
    Arrays.sort(values);
    final int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0;
  }
}
