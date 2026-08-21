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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.pipe.plugin.sink.opcua;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;
import org.apache.iotdb.pipe.plugin.sink.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.pipe.plugin.sink.opcua.server.OpcUaNameSpace.TabletRowConsumer;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Manual benchmark for the OPC UA client-server TsFile paths. Enable it with {@code
 * -Diotdb.opcua.tsfile.last-value.perf.enabled=true}; the remaining properties below tune the
 * generated file and measurement rounds.
 */
public class OpcUaSinkTsFilePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.opcua.tsfile.last-value.perf.enabled";
  private static final String DEVICE_COUNT_PROPERTY =
      "iotdb.opcua.tsfile.last-value.perf.device.count";
  private static final String MEASUREMENT_COUNT_PROPERTY =
      "iotdb.opcua.tsfile.last-value.perf.measurement.count";
  private static final String BLOB_MEASUREMENT_COUNT_PROPERTY =
      "iotdb.opcua.tsfile.last-value.perf.blob.measurement.count";
  private static final String ROW_COUNT_PROPERTY = "iotdb.opcua.tsfile.last-value.perf.row.count";
  private static final String TABLET_ROW_COUNT_PROPERTY =
      "iotdb.opcua.tsfile.last-value.perf.tablet.row.count";
  private static final String WARMUP_ITERATIONS_PROPERTY =
      "iotdb.opcua.tsfile.last-value.perf.warmup.iterations";
  private static final String ITERATIONS_PROPERTY = "iotdb.opcua.tsfile.last-value.perf.iterations";
  private static final String ROUNDS_PROPERTY = "iotdb.opcua.tsfile.last-value.perf.rounds";

  private static volatile long benchmarkBlackhole;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void compareTabletAndMetadataLastValuePaths() throws Exception {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true; use the benchmark-specific properties to tune its workload.",
            ENABLED_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());

    final int deviceCount = Integer.getInteger(DEVICE_COUNT_PROPERTY, 2);
    final int measurementCount = Integer.getInteger(MEASUREMENT_COUNT_PROPERTY, 32);
    final int blobMeasurementCount = Integer.getInteger(BLOB_MEASUREMENT_COUNT_PROPERTY, 1);
    final int rowCount = Integer.getInteger(ROW_COUNT_PROPERTY, 50_000);
    final int tabletRowCount = Integer.getInteger(TABLET_ROW_COUNT_PROPERTY, 1024);
    final int warmupIterations = Integer.getInteger(WARMUP_ITERATIONS_PROPERTY, 1);
    final int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 1);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    assertPositive(
        deviceCount,
        measurementCount,
        rowCount,
        tabletRowCount,
        warmupIterations,
        iterations,
        rounds);
    Assert.assertTrue(blobMeasurementCount >= 0);
    Assert.assertTrue(blobMeasurementCount <= measurementCount);

    final File tsFile = new File(temporaryFolder.getRoot(), "opcua-last-value-performance.tsfile");
    generateAlignedTreeTsFile(
        tsFile, deviceCount, measurementCount, blobMeasurementCount, rowCount, tabletRowCount);

    final boolean originalPipeMemoryManagementEnabled =
        PipeConfig.getInstance().getPipeMemoryManagementEnabled();
    CommonDescriptor.getInstance().getConfig().setPipeMemoryManagementEnabled(false);
    try {
      final Map<String, CapturedLastValue> tabletLastValues =
          captureLastValues(consumer -> transferByTabletPath(tsFile, consumer));
      final Map<String, CapturedLastValue> metadataLastValues =
          captureLastValues(consumer -> transferByMetadataPath(tsFile, consumer));
      Assert.assertEquals((long) deviceCount * measurementCount, tabletLastValues.size());
      Assert.assertEquals(tabletLastValues, metadataLastValues);

      compare(
          tsFile,
          deviceCount,
          measurementCount,
          blobMeasurementCount,
          rowCount,
          warmupIterations,
          iterations,
          rounds);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMemoryManagementEnabled(originalPipeMemoryManagementEnabled);
    }
  }

  private static void compare(
      final File tsFile,
      final int deviceCount,
      final int measurementCount,
      final int blobMeasurementCount,
      final int rowCount,
      final int warmupIterations,
      final int iterations,
      final int rounds) {
    final Runnable tabletPath = () -> benchmark(() -> transferByTabletPath(tsFile, null));
    final Runnable metadataPath = () -> benchmark(() -> transferByMetadataPath(tsFile, null));

    for (int i = 0; i < warmupIterations; ++i) {
      if ((i & 1) == 0) {
        tabletPath.run();
        metadataPath.run();
      } else {
        metadataPath.run();
        tabletPath.run();
      }
    }

    final Measurement[] tabletMeasurements = new Measurement[rounds];
    final Measurement[] metadataMeasurements = new Measurement[rounds];
    for (int round = 0; round < rounds; ++round) {
      if ((round & 1) == 0) {
        tabletMeasurements[round] = ManualPerformanceTestUtils.measure(iterations, tabletPath);
        metadataMeasurements[round] = ManualPerformanceTestUtils.measure(iterations, metadataPath);
      } else {
        metadataMeasurements[round] = ManualPerformanceTestUtils.measure(iterations, metadataPath);
        tabletMeasurements[round] = ManualPerformanceTestUtils.measure(iterations, tabletPath);
      }
    }

    final Summary tabletSummary =
        ManualPerformanceTestUtils.summarize(tabletMeasurements, iterations);
    final Summary metadataSummary =
        ManualPerformanceTestUtils.summarize(metadataMeasurements, iterations);
    final long pointCount = (long) deviceCount * measurementCount * rowCount;
    System.out.printf(
        Locale.ROOT,
        "%nOPC UA TsFile last-value benchmark: file=%.2f MiB, devices=%d, measurements/device=%d, BLOB measurements/device=%d, rows/device=%d, points=%d, warmups=%d, iterations/round=%d, rounds=%d%n",
        tsFile.length() / 1024.0 / 1024.0,
        deviceCount,
        measurementCount,
        blobMeasurementCount,
        rowCount,
        pointCount,
        warmupIterations,
        iterations,
        rounds);
    printSummary("tablet path", tabletSummary);
    printSummary("metadata path", metadataSummary);
    System.out.printf(
        Locale.ROOT,
        "  change: CPU speedup=%.2fx, allocation reduction=%.1f%%, peak-heap reduction=%.1f%%%n",
        ratio(tabletSummary.getCpuNanosPerOperation(), metadataSummary.getCpuNanosPerOperation()),
        reduction(
            tabletSummary.getAllocatedBytesPerOperation(),
            metadataSummary.getAllocatedBytesPerOperation()),
        reduction(tabletSummary.getPeakHeapDeltaBytes(), metadataSummary.getPeakHeapDeltaBytes()));
  }

  private static void transferByTabletPath(
      final File tsFile, final TabletRowConsumer suppliedConsumer) throws Exception {
    final BenchmarkConsumer benchmarkConsumer =
        Objects.isNull(suppliedConsumer) ? new BenchmarkConsumer() : null;
    final TabletRowConsumer consumer =
        Objects.isNull(suppliedConsumer) ? benchmarkConsumer : suppliedConsumer;
    try (final TsFileInsertionEventScanParser parser =
        new TsFileInsertionEventScanParser(
            tsFile,
            new PrefixTreePattern("root"),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            null,
            null,
            false)) {
      for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
        OpcUaNameSpace.transferTabletForClientServerModel(
            tabletWithIsAligned.getLeft(), false, null, consumer);
      }
    }
    if (Objects.nonNull(benchmarkConsumer)) {
      benchmarkBlackhole = benchmarkConsumer.result();
    }
  }

  private static void transferByMetadataPath(
      final File tsFile, final TabletRowConsumer suppliedConsumer) throws Exception {
    final BenchmarkConsumer benchmarkConsumer =
        Objects.isNull(suppliedConsumer) ? new BenchmarkConsumer() : null;
    final TabletRowConsumer consumer =
        Objects.isNull(suppliedConsumer) ? benchmarkConsumer : suppliedConsumer;
    for (final Map.Entry<IDeviceID, List<Pair<IMeasurementSchema, TimeValuePair>>> entry :
        OpcUaSink.readLastValues(tsFile).entrySet()) {
      OpcUaNameSpace.transferLastValues(entry.getKey(), entry.getValue(), false, null, consumer);
    }
    if (Objects.nonNull(benchmarkConsumer)) {
      benchmarkBlackhole = benchmarkConsumer.result();
    }
  }

  private static Map<String, CapturedLastValue> captureLastValues(
      final ThrowingConsumerRunner runner) throws Exception {
    final Map<String, CapturedLastValue> lastValues = new LinkedHashMap<>();
    runner.run(
        (segments, schemas, timestamps, values, sink) -> {
          final String device = String.join(".", segments);
          for (int i = 0; i < schemas.size(); ++i) {
            lastValues.put(
                device + "." + schemas.get(i).getMeasurementName(),
                new CapturedLastValue(timestamps.get(i), values.get(i)));
          }
        });
    return lastValues;
  }

  private static void benchmark(final ThrowingRunnable operation) {
    try {
      operation.run();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  private static void generateAlignedTreeTsFile(
      final File tsFile,
      final int deviceCount,
      final int measurementCount,
      final int blobMeasurementCount,
      final int rowCount,
      final int tabletRowCount)
      throws Exception {
    final List<IMeasurementSchema> schemas = new ArrayList<>(measurementCount);
    for (int measurement = 0; measurement < measurementCount; ++measurement) {
      final TSDataType dataType =
          measurement < blobMeasurementCount ? TSDataType.BLOB : TSDataType.INT64;
      schemas.add(
          new MeasurementSchema(
              "s" + measurement, dataType, TSEncoding.PLAIN, CompressionType.LZ4));
    }

    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      for (int device = 0; device < deviceCount; ++device) {
        final String deviceId = "root.opcua_perf.d" + device;
        writer.registerAlignedTimeseries(new PartialPath(deviceId), schemas);
        final Tablet tablet = new Tablet(deviceId, schemas, tabletRowCount);
        for (int row = 0; row < rowCount; ++row) {
          if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
            writer.writeAligned(tablet);
            tablet.reset();
          }

          final int rowIndex = tablet.getRowSize();
          tablet.addTimestamp(rowIndex, row);
          for (int measurement = 0; measurement < measurementCount; ++measurement) {
            if (measurement < blobMeasurementCount) {
              tablet.addValue(
                  schemas.get(measurement).getMeasurementName(),
                  rowIndex,
                  new Binary(
                      "d" + device + "s" + measurement + "r" + row, TSFileConfig.STRING_CHARSET));
            } else {
              tablet.addValue(
                  rowIndex,
                  measurement,
                  ((long) device * measurementCount + measurement) * rowCount + row);
            }
          }
        }
        if (tablet.getRowSize() > 0) {
          writer.writeAligned(tablet);
        }
      }
    }
  }

  private static void assertPositive(final int... values) {
    for (final int value : values) {
      Assert.assertTrue(value > 0);
    }
  }

  private static void printSummary(final String label, final Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-13s CPU=%.3f ms/file, allocated=%.3f MiB/file, peak heap delta=%.3f MiB%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000_000.0,
        summary.getAllocatedBytesPerOperation() / 1024.0 / 1024.0,
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
  }

  private static double ratio(final double baseline, final double optimized) {
    return optimized == 0 ? Double.POSITIVE_INFINITY : baseline / optimized;
  }

  private static double reduction(final double baseline, final double optimized) {
    return baseline == 0 ? 0 : (baseline - optimized) * 100.0 / baseline;
  }

  @FunctionalInterface
  private interface ThrowingConsumerRunner {
    void run(TabletRowConsumer consumer) throws Exception;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static final class BenchmarkConsumer implements TabletRowConsumer {

    private long hash = 1;
    private long callbackCount;
    private long valueCount;

    @Override
    public void accept(
        final String[] segments,
        final List<IMeasurementSchema> measurementSchemas,
        final List<Long> timestamps,
        final List<Object> values,
        final OpcUaSink sink) {
      ++callbackCount;
      for (final String segment : segments) {
        hash = 31 * hash + Objects.hashCode(segment);
      }
      for (int i = 0; i < measurementSchemas.size(); ++i) {
        hash = 31 * hash + measurementSchemas.get(i).getMeasurementName().hashCode();
        hash = 31 * hash + Long.hashCode(timestamps.get(i));
        hash = 31 * hash + Objects.hashCode(values.get(i));
        ++valueCount;
      }
    }

    private long result() {
      return hash ^ callbackCount ^ valueCount;
    }
  }

  private static final class CapturedLastValue {

    private final long timestamp;
    private final Object value;

    private CapturedLastValue(final long timestamp, final Object value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CapturedLastValue)) {
        return false;
      }
      final CapturedLastValue that = (CapturedLastValue) obj;
      return timestamp == that.timestamp && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, value);
    }
  }
}
