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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/** Manual benchmark for comparing flush pipeline and single-thread modes under concurrency. */
public class FlushSubTaskPipelinePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.flush-sub-task.perf.enabled";
  private static final String ROUNDS_PROPERTY = "iotdb.flush-sub-task.perf.rounds";
  private static final String ROWS_PROPERTY = "iotdb.flush-sub-task.perf.rows";
  private static final String COLUMNS_PROPERTY = "iotdb.flush-sub-task.perf.columns";

  /** Compatibility alias for the previous single-row-count benchmark. */
  private static final String WORK_PROPERTY = "iotdb.flush-sub-task.perf.work";

  /**
   * Flush real multi-series INT32 MemTables across row/column sizes and 1-64 concurrent callers in
   * both modes. Defaults are 10,000 and 100,000 rows, 1 and 4 columns, and three measured rounds
   * after one warmup round.
   */
  @Test
  public void benchmarkFlushModesByConcurrency() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean(ENABLED_PROPERTY));
    boolean originalPipeline =
        IoTDBDescriptor.getInstance().getConfig().isEnableFlushSubTaskPipeline();
    EnvironmentUtils.envSetUp();
    try {
      int rounds = Integer.getInteger(ROUNDS_PROPERTY, 3);
      int[] rows = getMatrix(ROWS_PROPERTY, WORK_PROPERTY, new int[] {10_000, 100_000});
      int[] columns = getMatrix(COLUMNS_PROPERTY, null, new int[] {1, 4});
      assertTrue(rounds > 0);
      System.out.printf(
          Locale.ROOT,
          "Flush settings rows=%s columns=%s rounds=%d processors=%d subTaskPool=cached%n",
          formatMatrix(rows),
          formatMatrix(columns),
          rounds,
          Runtime.getRuntime().availableProcessors());
      for (int rowCount : rows) {
        for (int columnCount : columns) {
          for (int concurrency : new int[] {1, 2, 4, 8, 16, 32, 64}) {
            runMatrixCase(rounds, rowCount, columnCount, concurrency);
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableFlushSubTaskPipeline(originalPipeline);
      EnvironmentUtils.cleanEnv();
    }
  }

  private static void runMatrixCase(int rounds, int rows, int columns, int concurrency)
      throws Exception {
    long[] elapsedNanos = new long[2];
    long[] flushNanos = new long[2];
    // Warm both modes, then alternate their order to reduce systematic warmup bias.
    for (int round = -1; round < rounds; round++) {
      for (boolean pipeline :
          round % 2 == 0 ? new boolean[] {false, true} : new boolean[] {true, false}) {
        IoTDBDescriptor.getInstance().getConfig().setEnableFlushSubTaskPipeline(pipeline);
        int mode = pipeline ? 0 : 1;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        long start = System.nanoTime();
        try {
          for (int task = 0; task < concurrency; task++) {
            int currentTask = task;
            int currentRound = round;
            futures.add(
                executor.submit(
                    () ->
                        runFlushWorkload(
                            pipeline, concurrency, currentRound, rows, columns, currentTask)));
          }
          long totalFlushNanos = 0;
          for (Future<Long> future : futures) {
            totalFlushNanos += future.get();
          }
          long batchNanos = System.nanoTime() - start;
          if (round >= 0) {
            elapsedNanos[mode] += batchNanos;
            flushNanos[mode] += totalFlushNanos;
            System.out.printf(
                Locale.ROOT,
                "Flush sample rows=%d columns=%d mode=%s concurrency=%d round=%d batch=%.3f ms flushMean=%.3f ms%n",
                rows,
                columns,
                pipeline ? "pipeline" : "single-thread",
                concurrency,
                round + 1,
                batchNanos / 1_000_000.0,
                totalFlushNanos / (double) concurrency / 1_000_000.0);
          }
        } finally {
          executor.shutdownNow();
          assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        }
      }
    }
    for (int mode = 0; mode < 2; mode++) {
      System.out.printf(
          Locale.ROOT,
          "Flush mode rows=%d columns=%d mode=%s concurrency=%d average=%.3f ms flushMean=%.3f ms pointsPerSecond=%.0f%n",
          rows,
          columns,
          mode == 0 ? "pipeline" : "single-thread",
          concurrency,
          elapsedNanos[mode] / (double) rounds / 1_000_000.0,
          flushNanos[mode] / (double) (rounds * concurrency) / 1_000_000.0,
          (double) rows * columns * concurrency * rounds * 1_000_000_000.0 / elapsedNanos[mode]);
    }
  }

  private static long runFlushWorkload(
      boolean pipeline, int concurrency, int round, int rows, int columns, int taskId) {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.flush_perf", Integer.toString(taskId));
    String filePath =
        TestConstant.OUTPUT_DATA_DIR
            + "flush-performance-"
            + pipeline
            + "-r"
            + rows
            + "-c"
            + columns
            + "-x"
            + concurrency
            + "-n"
            + round
            + "-t"
            + taskId
            + ".tsfile";
    try {
      produceData(memTable, rows, columns);
      try (RestorableTsFileIOWriter writer =
          new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory().getFile(filePath))) {
        long start = System.nanoTime();
        new MemTableFlushTask(memTable, writer, "root.flush_perf", Integer.toString(taskId))
            .syncFlushMemTable();
        return System.nanoTime() - start;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      memTable.release();
    }
  }

  private static void produceData(PrimitiveMemTable memTable, int rows, int columns)
      throws IllegalPathException {
    List<IMeasurementSchema> schemas = new ArrayList<>(columns);
    for (int column = 0; column < columns; column++) {
      schemas.add(
          new MeasurementSchema("s" + column, MemTableTestUtils.dataType0, TSEncoding.PLAIN));
    }
    IDeviceID deviceId = DeviceIDFactory.getInstance().getDeviceID(new PartialPath("d0"));
    for (long row = 1; row <= rows; row++) {
      Object[] values = new Object[columns];
      for (int column = 0; column < columns; column++) {
        values[column] = (int) (row + column);
      }
      memTable.write(deviceId, schemas, row, values);
    }
  }

  private static int[] getMatrix(String property, String fallbackProperty, int[] defaults) {
    String value = System.getProperty(property);
    if (value == null && fallbackProperty != null) {
      value = System.getProperty(fallbackProperty);
    }
    if (value == null) {
      return defaults;
    }
    String[] parts = value.split(",", -1);
    int[] result = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      result[i] = Integer.parseInt(parts[i].trim());
      assertTrue(result[i] > 0);
    }
    // Repeated dimensions would reuse output paths and invalidate independent measurements.
    assertTrue(Arrays.stream(result).distinct().count() == result.length);
    return result;
  }

  private static String formatMatrix(int[] values) {
    return Arrays.toString(values);
  }
}
