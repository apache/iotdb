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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.protocol.thrift.handler.RPCServiceThriftHandlerMetrics;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Measurement;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils.Summary;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class TabletRpcDeserializationPerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.tablet.rpc.deserialization.perf.enabled";
  private static final String ROUNDS_PROPERTY = "iotdb.tablet.rpc.deserialization.perf.rounds";

  private static final String DECODE_COLUMNS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.decode.columns";
  private static final String DECODE_ROWS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.decode.rows";
  private static final String DECODE_WARMUP_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.decode.warmup.iterations";
  private static final String DECODE_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.decode.iterations";

  private static final String BATCH_TABLETS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.batch.tablets";
  private static final String BATCH_MEASUREMENTS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.batch.measurements";
  private static final String BATCH_ROWS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.batch.rows";
  private static final String BATCH_WARMUP_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.batch.warmup.iterations";
  private static final String BATCH_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.batch.iterations";

  private static final String REUSE_TABLETS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.reuse.tablets";
  private static final String REUSE_MEASUREMENTS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.reuse.measurements";
  private static final String REUSE_WARMUP_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.reuse.warmup.iterations";
  private static final String REUSE_ITERATIONS_PROPERTY =
      "iotdb.tablet.rpc.deserialization.perf.reuse.iterations";

  private static final Runnable NO_OP = () -> {};

  private static volatile Object benchmarkBlackhole;
  private static volatile long benchmarkLongBlackhole;

  @Test
  public void plainTabletDecodeBenchmark() {
    assumeEnabled();

    final int columnCount = Integer.getInteger(DECODE_COLUMNS_PROPERTY, 64);
    final int rowCount = Integer.getInteger(DECODE_ROWS_PROPERTY, 256);
    final int warmupIterations = Integer.getInteger(DECODE_WARMUP_ITERATIONS_PROPERTY, 500);
    final int iterations = Integer.getInteger(DECODE_ITERATIONS_PROPERTY, 1_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    assertPositive(columnCount, rowCount, warmupIterations, iterations, rounds);

    final DecodeScenario scenario = createDecodeScenario(columnCount, rowCount);
    final DecodedTablet legacy = decodeLegacy(scenario);
    final DecodedTablet optimized = decodeOptimized(scenario);
    assertDecodedTabletEquals(legacy, optimized);

    compare(
        String.format(
            Locale.ROOT,
            "PLAIN tablet decode benchmark: columns=%d, rows=%d",
            columnCount,
            rowCount),
        warmupIterations,
        iterations,
        rounds,
        NO_OP,
        () -> consume(decodeLegacy(scenario)),
        NO_OP,
        () -> consume(decodeOptimized(scenario)));
  }

  @Test
  public void batchStatementParsingBenchmark() throws IllegalPathException {
    assumeEnabled();

    final int tabletCount = Integer.getInteger(BATCH_TABLETS_PROPERTY, 32);
    final int measurementCount = Integer.getInteger(BATCH_MEASUREMENTS_PROPERTY, 32);
    final int rowCount = Integer.getInteger(BATCH_ROWS_PROPERTY, 16);
    final int warmupIterations = Integer.getInteger(BATCH_WARMUP_ITERATIONS_PROPERTY, 500);
    final int iterations = Integer.getInteger(BATCH_ITERATIONS_PROPERTY, 1_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    assertPositive(tabletCount, measurementCount, rowCount, warmupIterations, iterations, rounds);

    final TSInsertTabletsReq legacyRequest =
        createInsertTabletsRequest(tabletCount, measurementCount, rowCount);
    final TSInsertTabletsReq optimizedRequest =
        createInsertTabletsRequest(tabletCount, measurementCount, rowCount);
    final InsertMultiTabletsStatement legacy = createLegacyStatement(legacyRequest);
    resetRequestBuffers(optimizedRequest);
    final InsertMultiTabletsStatement optimized =
        StatementGenerator.createStatement(optimizedRequest);
    assertStatementsEqual(legacy, optimized);

    compare(
        String.format(
            Locale.ROOT,
            "Batch tablet statement benchmark: tablets=%d, measurements/tablet=%d, rows/tablet=%d",
            tabletCount,
            measurementCount,
            rowCount),
        warmupIterations,
        iterations,
        rounds,
        () -> resetRequestBuffers(legacyRequest),
        () -> runLegacyStatementParsing(legacyRequest),
        () -> resetRequestBuffers(optimizedRequest),
        () -> runOptimizedStatementParsing(optimizedRequest));
  }

  @Test
  public void measurementCanonicalizationBenchmark() throws MetadataException {
    assumeEnabled();

    final int tabletCount = Integer.getInteger(REUSE_TABLETS_PROPERTY, 64);
    final int measurementCount = Integer.getInteger(REUSE_MEASUREMENTS_PROPERTY, 64);
    final int warmupIterations = Integer.getInteger(REUSE_WARMUP_ITERATIONS_PROPERTY, 500);
    final int iterations = Integer.getInteger(REUSE_ITERATIONS_PROPERTY, 2_000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    assertPositive(tabletCount, measurementCount, warmupIterations, iterations, rounds);

    final MeasurementBatch legacyBatch = createMeasurementBatch(tabletCount, measurementCount);
    final MeasurementBatch optimizedBatch = createMeasurementBatch(tabletCount, measurementCount);
    final List<List<String>> legacy =
        PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(legacyBatch.measurementLists);
    PathUtils.checkIsLegalSingleMeasurementListsAndUpdateInPlace(optimizedBatch.measurementLists);
    Assert.assertEquals(legacy, optimizedBatch.measurementLists);
    assertMeasurementsCanonicalized(optimizedBatch.measurementLists);

    compare(
        String.format(
            Locale.ROOT,
            "Measurement canonicalization benchmark: tablets=%d, measurements/tablet=%d",
            tabletCount,
            measurementCount),
        warmupIterations,
        iterations,
        rounds,
        () -> legacyBatch.reset(),
        () -> runLegacyMeasurementCanonicalization(legacyBatch),
        () -> optimizedBatch.reset(),
        () -> runOptimizedMeasurementCanonicalization(optimizedBatch));
  }

  private static void assumeEnabled() {
    Assume.assumeTrue(
        String.format(
            "Manual performance UT. Enable with -D%s=true; use the benchmark-specific properties to tune its workload.",
            ENABLED_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
    Assume.assumeTrue(
        "Current-thread CPU time and allocation metrics are required.",
        ManualPerformanceTestUtils.enableThreadMetrics());
  }

  private static DecodeScenario createDecodeScenario(int columnCount, int rowCount) {
    final TSDataType[] dataTypes = new TSDataType[columnCount];
    final List<TSEncoding> encodings = new ArrayList<>(columnCount + 1);
    encodings.add(TSEncoding.PLAIN);
    final ByteBuffer timeBuffer = ByteBuffer.allocate(rowCount * Long.BYTES);
    for (int row = 0; row < rowCount; row++) {
      timeBuffer.putLong(row);
    }
    timeBuffer.flip();

    final ByteBuffer valueBuffer = ByteBuffer.allocate(columnCount * rowCount * Long.BYTES);
    for (int column = 0; column < columnCount; column++) {
      dataTypes[column] = TSDataType.INT64;
      encodings.add(TSEncoding.PLAIN);
      for (int row = 0; row < rowCount; row++) {
        valueBuffer.putLong((long) column * rowCount + row);
      }
    }
    valueBuffer.flip();
    return new DecodeScenario(dataTypes, encodings, rowCount, timeBuffer, valueBuffer);
  }

  private static DecodedTablet decodeLegacy(DecodeScenario scenario) {
    final LegacyPlainTabletDecoder decoder =
        new LegacyPlainTabletDecoder(scenario.dataTypes, scenario.encodings, scenario.rowCount);
    return new DecodedTablet(
        decoder.decodeTime(scenario.timeBuffer.duplicate()),
        decoder.decodeValues(scenario.valueBuffer.duplicate()));
  }

  private static DecodedTablet decodeOptimized(DecodeScenario scenario) {
    final TabletDecoder decoder =
        new TabletDecoder(
            CompressionType.UNCOMPRESSED,
            scenario.dataTypes,
            scenario.encodings,
            scenario.rowCount);
    return new DecodedTablet(
        decoder.decodeTime(scenario.timeBuffer.duplicate()),
        decoder.decodeValues(scenario.valueBuffer.duplicate()));
  }

  private static TSInsertTabletsReq createInsertTabletsRequest(
      int tabletCount, int measurementCount, int rowCount) {
    final List<String> prefixPaths = new ArrayList<>(tabletCount);
    final List<List<String>> measurementsList = new ArrayList<>(tabletCount);
    final List<ByteBuffer> valuesList = new ArrayList<>(tabletCount);
    final List<ByteBuffer> timestampsList = new ArrayList<>(tabletCount);
    final List<List<Integer>> typesList = new ArrayList<>(tabletCount);
    final List<Integer> sizeList = new ArrayList<>(tabletCount);
    for (int tablet = 0; tablet < tabletCount; tablet++) {
      prefixPaths.add("root.tablet_rpc_performance.d" + tablet);
      final List<String> measurements = new ArrayList<>(measurementCount);
      final List<Integer> types = new ArrayList<>(measurementCount);
      final ByteBuffer values = ByteBuffer.allocate(measurementCount * rowCount * Long.BYTES);
      for (int measurement = 0; measurement < measurementCount; measurement++) {
        measurements.add("s" + measurement);
        types.add((int) TSDataType.INT64.serialize());
        for (int row = 0; row < rowCount; row++) {
          values.putLong((long) measurement * rowCount + row);
        }
      }
      values.flip();

      final ByteBuffer timestamps = ByteBuffer.allocate(rowCount * Long.BYTES);
      for (int row = 0; row < rowCount; row++) {
        timestamps.putLong(row);
      }
      timestamps.flip();

      measurementsList.add(measurements);
      valuesList.add(values);
      timestampsList.add(timestamps);
      typesList.add(types);
      sizeList.add(rowCount);
    }
    return new TSInsertTabletsReq(
        0L, prefixPaths, measurementsList, valuesList, timestampsList, typesList, sizeList);
  }

  // Keep the origin/master implementation for an in-process, same-JVM comparison.
  private static InsertMultiTabletsStatement createLegacyStatement(TSInsertTabletsReq request)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    final InsertMultiTabletsStatement insertStatement = new InsertMultiTabletsStatement();
    final List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (int i = 0; i < request.prefixPaths.size(); i++) {
      final InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
      insertTabletStatement.setDevicePath(
          DataNodeDevicePathCache.getInstance().getPartialPath(request.prefixPaths.get(i)));
      insertTabletStatement.setMeasurements(request.measurementsList.get(i).toArray(new String[0]));
      final long[] timestamps =
          QueryDataSetUtils.readTimesFromBuffer(
              request.timestampsList.get(i), request.sizeList.get(i));
      if (timestamps.length != 0) {
        TimestampPrecisionUtils.checkTimestampPrecision(timestamps[timestamps.length - 1]);
      }
      insertTabletStatement.setTimes(timestamps);
      insertTabletStatement.setColumns(
          QueryDataSetUtils.readTabletValuesFromBuffer(
              request.valuesList.get(i),
              request.typesList.get(i),
              request.measurementsList.get(i).size(),
              request.sizeList.get(i)));
      insertTabletStatement.setBitMaps(
          QueryDataSetUtils.readBitMapsFromBuffer(
                  request.valuesList.get(i),
                  request.measurementsList.get(i).size(),
                  request.sizeList.get(i))
              .orElse(null));
      insertTabletStatement.setRowCount(request.sizeList.get(i));
      final TSDataType[] dataTypes = new TSDataType[request.typesList.get(i).size()];
      for (int j = 0; j < dataTypes.length; j++) {
        dataTypes[j] = TSDataType.deserialize((byte) request.typesList.get(i).get(j).intValue());
      }
      insertTabletStatement.setDataTypes(dataTypes);
      insertTabletStatement.setAligned(request.isAligned);
      if (!insertTabletStatement.isEmpty()) {
        insertTabletStatementList.add(insertTabletStatement);
      }
    }
    insertStatement.setInsertTabletStatementList(insertTabletStatementList);
    PerformanceOverviewMetrics.getInstance().recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  private static MeasurementBatch createMeasurementBatch(int tabletCount, int measurementCount) {
    final List<List<String>> measurementLists = new ArrayList<>(tabletCount);
    final String[][] originals = new String[tabletCount][measurementCount];
    for (int tablet = 0; tablet < tabletCount; tablet++) {
      final List<String> measurements = new ArrayList<>(measurementCount);
      for (int measurement = 0; measurement < measurementCount; measurement++) {
        final String value = new String("tablet_rpc_performance_s" + measurement);
        originals[tablet][measurement] = value;
        measurements.add(value);
      }
      measurementLists.add(measurements);
    }
    return new MeasurementBatch(measurementLists, originals);
  }

  private static void runLegacyStatementParsing(TSInsertTabletsReq request) {
    try {
      benchmarkBlackhole = createLegacyStatement(request);
    } catch (IllegalPathException e) {
      throw new AssertionError(e);
    }
  }

  private static void runOptimizedStatementParsing(TSInsertTabletsReq request) {
    try {
      benchmarkBlackhole = StatementGenerator.createStatement(request);
    } catch (IllegalPathException e) {
      throw new AssertionError(e);
    }
  }

  private static void runLegacyMeasurementCanonicalization(MeasurementBatch batch) {
    try {
      benchmarkBlackhole =
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(batch.measurementLists);
    } catch (MetadataException e) {
      throw new AssertionError(e);
    }
  }

  private static void runOptimizedMeasurementCanonicalization(MeasurementBatch batch) {
    try {
      PathUtils.checkIsLegalSingleMeasurementListsAndUpdateInPlace(batch.measurementLists);
      benchmarkBlackhole = batch.measurementLists;
    } catch (MetadataException e) {
      throw new AssertionError(e);
    }
  }

  private static void resetRequestBuffers(TSInsertTabletsReq request) {
    request.timestampsList.forEach(ByteBuffer::rewind);
    request.valuesList.forEach(ByteBuffer::rewind);
  }

  private static void consume(DecodedTablet decodedTablet) {
    benchmarkLongBlackhole = decodedTablet.timestamps[decodedTablet.timestamps.length - 1];
    benchmarkBlackhole = decodedTablet.values;
  }

  private static void assertDecodedTabletEquals(DecodedTablet expected, DecodedTablet actual) {
    Assert.assertArrayEquals(expected.timestamps, actual.timestamps);
    Assert.assertEquals(expected.values.left.length, actual.values.left.length);
    for (int i = 0; i < expected.values.left.length; i++) {
      Assert.assertArrayEquals((long[]) expected.values.left[i], (long[]) actual.values.left[i]);
    }
    Assert.assertFalse(expected.values.right.hasRemaining());
    Assert.assertFalse(actual.values.right.hasRemaining());
  }

  private static void assertStatementsEqual(
      InsertMultiTabletsStatement expected, InsertMultiTabletsStatement actual) {
    final List<InsertTabletStatement> expectedTablets = expected.getInsertTabletStatementList();
    final List<InsertTabletStatement> actualTablets = actual.getInsertTabletStatementList();
    Assert.assertEquals(expectedTablets.size(), actualTablets.size());
    for (int i = 0; i < expectedTablets.size(); i++) {
      Assert.assertArrayEquals(
          expectedTablets.get(i).getMeasurements(), actualTablets.get(i).getMeasurements());
      Assert.assertArrayEquals(
          expectedTablets.get(i).getDataTypes(), actualTablets.get(i).getDataTypes());
      Assert.assertArrayEquals(expectedTablets.get(i).getTimes(), actualTablets.get(i).getTimes());
      Assert.assertEquals(expectedTablets.get(i).getRowCount(), actualTablets.get(i).getRowCount());
    }
  }

  private static void assertMeasurementsCanonicalized(List<List<String>> measurementLists) {
    final List<String> firstTablet = measurementLists.get(0);
    for (int tablet = 1; tablet < measurementLists.size(); tablet++) {
      for (int measurement = 0; measurement < firstTablet.size(); measurement++) {
        Assert.assertSame(
            firstTablet.get(measurement), measurementLists.get(tablet).get(measurement));
      }
    }
  }

  private static void assertPositive(int... values) {
    for (int value : values) {
      Assert.assertTrue(value > 0);
    }
  }

  private static void compare(
      String title,
      int warmupIterations,
      int iterations,
      int rounds,
      Runnable legacyBefore,
      Runnable legacyOperation,
      Runnable optimizedBefore,
      Runnable optimizedOperation) {
    for (int i = 0; i < warmupIterations; i++) {
      if ((i & 1) == 0) {
        legacyBefore.run();
        legacyOperation.run();
        optimizedBefore.run();
        optimizedOperation.run();
      } else {
        optimizedBefore.run();
        optimizedOperation.run();
        legacyBefore.run();
        legacyOperation.run();
      }
    }

    final Measurement[] legacyMeasurements = new Measurement[rounds];
    final Measurement[] optimizedMeasurements = new Measurement[rounds];
    for (int round = 0; round < rounds; round++) {
      if ((round & 1) == 0) {
        legacyMeasurements[round] =
            ManualPerformanceTestUtils.measure(iterations, legacyBefore, legacyOperation);
        optimizedMeasurements[round] =
            ManualPerformanceTestUtils.measure(iterations, optimizedBefore, optimizedOperation);
      } else {
        optimizedMeasurements[round] =
            ManualPerformanceTestUtils.measure(iterations, optimizedBefore, optimizedOperation);
        legacyMeasurements[round] =
            ManualPerformanceTestUtils.measure(iterations, legacyBefore, legacyOperation);
      }
    }

    final Summary legacy = ManualPerformanceTestUtils.summarize(legacyMeasurements, iterations);
    final Summary optimized =
        ManualPerformanceTestUtils.summarize(optimizedMeasurements, iterations);
    System.out.printf(
        Locale.ROOT,
        "%s, warmups=%d, iterations/round=%d, rounds=%d%n",
        title,
        warmupIterations,
        iterations,
        rounds);
    printSummary("origin/master", legacy);
    printSummary("optimized", optimized);
    System.out.printf(
        Locale.ROOT,
        "  change: CPU speedup=%.2fx, allocation reduction=%.1f%%, peak-heap reduction=%.1f%%%n",
        ratio(legacy.getCpuNanosPerOperation(), optimized.getCpuNanosPerOperation()),
        reduction(
            legacy.getAllocatedBytesPerOperation(), optimized.getAllocatedBytesPerOperation()),
        reduction(legacy.getPeakHeapDeltaBytes(), optimized.getPeakHeapDeltaBytes()));
  }

  private static void printSummary(String label, Summary summary) {
    System.out.printf(
        Locale.ROOT,
        "  %-13s CPU=%.3f us/op, allocated=%.1f bytes/op, peak heap delta=%.3f MiB%n",
        label,
        summary.getCpuNanosPerOperation() / 1_000.0,
        summary.getAllocatedBytesPerOperation(),
        summary.getPeakHeapDeltaBytes() / 1024.0 / 1024.0);
  }

  private static double ratio(double baseline, double optimized) {
    return optimized == 0 ? Double.POSITIVE_INFINITY : baseline / optimized;
  }

  private static double reduction(double baseline, double optimized) {
    return baseline == 0 ? 0 : (baseline - optimized) * 100.0 / baseline;
  }

  private static final class DecodeScenario {

    private final TSDataType[] dataTypes;
    private final List<TSEncoding> encodings;
    private final int rowCount;
    private final ByteBuffer timeBuffer;
    private final ByteBuffer valueBuffer;

    private DecodeScenario(
        TSDataType[] dataTypes,
        List<TSEncoding> encodings,
        int rowCount,
        ByteBuffer timeBuffer,
        ByteBuffer valueBuffer) {
      this.dataTypes = dataTypes;
      this.encodings = encodings;
      this.rowCount = rowCount;
      this.timeBuffer = timeBuffer;
      this.valueBuffer = valueBuffer;
    }
  }

  private static final class DecodedTablet {

    private final long[] timestamps;
    private final Pair<Object[], ByteBuffer> values;

    private DecodedTablet(long[] timestamps, Pair<Object[], ByteBuffer> values) {
      this.timestamps = timestamps;
      this.values = values;
    }
  }

  private static final class MeasurementBatch {

    private final List<List<String>> measurementLists;
    private final String[][] originals;

    private MeasurementBatch(List<List<String>> measurementLists, String[][] originals) {
      this.measurementLists = measurementLists;
      this.originals = originals;
    }

    private void reset() {
      for (int tablet = 0; tablet < originals.length; tablet++) {
        final List<String> measurements = measurementLists.get(tablet);
        for (int measurement = 0; measurement < originals[tablet].length; measurement++) {
          measurements.set(measurement, originals[tablet][measurement]);
        }
      }
    }
  }

  /** The PLAIN INT64 decoding path from origin/master, kept only for benchmark comparison. */
  private static final class LegacyPlainTabletDecoder {

    private final TSDataType[] dataTypes;
    private final List<TSEncoding> columnEncodings;

    @SuppressWarnings("unused")
    private final IUnCompressor unCompressor;

    private final int rowSize;

    private LegacyPlainTabletDecoder(
        TSDataType[] dataTypes, List<TSEncoding> columnEncodings, int rowSize) {
      this.dataTypes = dataTypes;
      this.columnEncodings = columnEncodings;
      this.unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
      this.rowSize = rowSize;
    }

    private long[] decodeTime(ByteBuffer buffer) {
      final int compressedSize = buffer.remaining();
      RPCServiceThriftHandlerMetrics.getInstance().recordUnCompressionSizeTimer(buffer.remaining());
      RPCServiceThriftHandlerMetrics.getInstance().recordCompressionSizeTimer(compressedSize);
      RPCServiceThriftHandlerMetrics.getInstance().recordMemoryUsage(buffer.remaining());

      final long startDecodeTime = System.nanoTime();
      final Decoder decoder = Decoder.getDecoderByType(columnEncodings.get(0), TSDataType.INT64);
      final long[] timestamps = new long[rowSize];
      for (int row = 0; row < rowSize; row++) {
        timestamps[row] = decoder.readLong(buffer);
      }
      RPCServiceThriftHandlerMetrics.getInstance()
          .recordDecodeLatencyTimer(System.nanoTime() - startDecodeTime);
      return timestamps;
    }

    private Pair<Object[], ByteBuffer> decodeValues(ByteBuffer buffer) {
      final int compressedSize = buffer.remaining();
      RPCServiceThriftHandlerMetrics.getInstance().recordUnCompressionSizeTimer(buffer.remaining());
      RPCServiceThriftHandlerMetrics.getInstance().recordCompressionSizeTimer(compressedSize);

      final long startDecodeTime = System.nanoTime();
      final Object[] columns = new Object[dataTypes.length];
      for (int column = 0; column < dataTypes.length; column++) {
        final Decoder decoder =
            Decoder.getDecoderByType(columnEncodings.get(column + 1), dataTypes[column]);
        final long[] values = new long[rowSize];
        for (int row = 0; row < rowSize; row++) {
          values[row] = decoder.readLong(buffer);
        }
        columns[column] = values;
      }
      RPCServiceThriftHandlerMetrics.getInstance()
          .recordDecodeLatencyTimer(System.nanoTime() - startDecodeTime);
      return new Pair<>(columns, buffer);
    }
  }
}
