/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ManualPerformanceTestUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.sun.management.ThreadMXBean;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AlignedRowsBranchComparisonBenchmarkTest {

  private static final String STORAGE_GROUP = "root.branch_benchmark";
  private static final String DEVICE_PATH = STORAGE_GROUP + ".d0";
  private static final IDeviceID DEVICE_ID = new PlainDeviceID(DEVICE_PATH);
  private static final int EXISTING_COLUMNS = 48;
  private static final int EXISTING_ROWS = PrimitiveArrayManager.ARRAY_SIZE;
  private static final int[] ROW_COUNTS = {128, 1024, 1024, 8192};
  private static final int[] COLUMN_COUNTS = {16, 16, 64, 64};
  private static final int[] ITERATIONS = {976, 122, 30, 3};
  private static final int WARMUP_ROUNDS = 3;
  private static final int MEASUREMENT_ROUNDS = 6;
  private static final ThreadMXBean ALLOCATION_MX_BEAN =
      (ThreadMXBean) ManagementFactory.getThreadMXBean();

  private TsFileProcessor processor;
  private DataRegionInfo dataRegionInfo;
  private File tsFile;
  private IMemTable memTable;
  private long benchmarkBlackhole;

  @Before
  public void setUp() throws Exception {
    Assert.assertTrue(ManualPerformanceTestUtils.enableThreadMetrics());
    EnvironmentUtils.envSetUp();
    tsFile =
        SystemFileFactory.INSTANCE.getFile(TestConstant.getTestTsFilePath(STORAGE_GROUP, 0, 0, 0));
    dataRegionInfo =
        new DataRegionInfo(
            new DataRegionTest.DummyDataRegion(
                TestConstant.OUTPUT_DATA_DIR + "branch-benchmark-info", STORAGE_GROUP));
    processor =
        new TsFileProcessor(
            STORAGE_GROUP,
            tsFile,
            dataRegionInfo,
            ignored -> {},
            (ignored, updateMap, systemFlushTime) -> {},
            true);
    processor.setTsFileProcessorInfo(new TsFileProcessorInfo(dataRegionInfo));
    dataRegionInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(dataRegionInfo, processor);

    List<IMeasurementSchema> schemas = createSchemas(EXISTING_COLUMNS);
    AlignedWritableMemChunk memChunk = new AlignedWritableMemChunk(new ArrayList<>(schemas));
    long[] times = new long[EXISTING_ROWS];
    Object[] columns = new Object[EXISTING_COLUMNS];
    for (int column = 0; column < EXISTING_COLUMNS; column++) {
      int[] values = new int[EXISTING_ROWS];
      for (int row = 0; row < EXISTING_ROWS; row++) {
        times[row] = row;
        values[row] = row + column;
      }
      columns[column] = values;
    }
    memChunk.putAlignedTablet(times, columns, null, 0, EXISTING_ROWS);
    Map<IDeviceID, IWritableMemChunkGroup> map = new HashMap<>();
    map.put(DEVICE_ID, new AlignedWritableMemChunkGroup(memChunk, new ArrayList<>(schemas)));
    memTable = new PrimitiveMemTable(STORAGE_GROUP, "0", map);
    Field workMemTable = TsFileProcessor.class.getDeclaredField("workMemTable");
    workMemTable.setAccessible(true);
    workMemTable.set(processor, memTable);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (processor != null) {
        processor.putMemTableBackAndClose();
      }
    } finally {
      EnvironmentUtils.cleanEnv();
      EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
      if (tsFile != null) {
        tsFile.delete();
        new File(tsFile.getPath() + ".resource").delete();
      }
    }
  }

  @Test
  public void compareAlignedRowsMemoryEstimation() throws Exception {
    for (int scenario = 0; scenario < ROW_COUNTS.length; scenario++) {
      List<InsertRowNode> rows = createRows(ROW_COUNTS[scenario], COLUMN_COUNTS[scenario]);
      for (int round = 0; round < WARMUP_ROUNDS; round++) {
        runUnmeasured(rows, ITERATIONS[scenario]);
      }
      BenchmarkResult[] results = new BenchmarkResult[MEASUREMENT_ROUNDS];
      for (int round = 0; round < MEASUREMENT_ROUNDS; round++) {
        results[round] = measure(rows, ITERATIONS[scenario]);
      }
      BenchmarkResult summary = summarize(results);
      System.out.printf(
          Locale.ROOT,
          "rows=%d columns=%d iterations=%d estimate=%d time=%.1f ns/op allocation=%.1f B/op%n",
          ROW_COUNTS[scenario],
          COLUMN_COUNTS[scenario],
          ITERATIONS[scenario],
          summary.estimate,
          summary.nanosPerOperation,
          summary.allocatedBytesPerOperation);
    }
    Assert.assertTrue(benchmarkBlackhole > 0);
  }

  private BenchmarkResult measure(List<InsertRowNode> rows, int iterations) {
    System.gc();
    System.runFinalization();
    long threadId = Thread.currentThread().getId();
    long allocatedBytesBefore = ALLOCATION_MX_BEAN.getThreadAllocatedBytes(threadId);
    long elapsedNanos = 0;
    long previousCost = 0;
    long estimate = 0;
    for (int iteration = 0; iteration < iterations; iteration++) {
      resetMemoryAccounting(previousCost);
      long startNanos = System.nanoTime();
      try {
        long[] increments =
            processor.benchmarkCheckAlignedMemCostAndAddToTspInfoForRows(rows, new HashSet<>());
        estimate = increments[0];
        benchmarkBlackhole = estimate;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        elapsedNanos += System.nanoTime() - startNanos;
      }
      previousCost = estimate;
    }
    resetMemoryAccounting(previousCost);
    long allocatedBytes =
        ALLOCATION_MX_BEAN.getThreadAllocatedBytes(threadId) - allocatedBytesBefore;
    return new BenchmarkResult(
        (double) elapsedNanos / iterations, (double) allocatedBytes / iterations, estimate);
  }

  private void runUnmeasured(List<InsertRowNode> rows, int iterations) {
    long previousCost = 0;
    for (int iteration = 0; iteration < iterations; iteration++) {
      resetMemoryAccounting(previousCost);
      try {
        previousCost =
            processor.benchmarkCheckAlignedMemCostAndAddToTspInfoForRows(rows, new HashSet<>())[0];
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    resetMemoryAccounting(previousCost);
  }

  private void resetMemoryAccounting(long memTableIncrement) {
    if (memTableIncrement == 0) {
      return;
    }
    memTable.releaseTVListRamCost(memTableIncrement);
    dataRegionInfo.releaseStorageGroupMemCost(memTableIncrement);
    SystemInfo.getInstance().resetStorageGroupStatus(dataRegionInfo);
  }

  private static BenchmarkResult summarize(BenchmarkResult[] results) {
    double[] nanos = new double[results.length];
    double[] allocatedBytes = new double[results.length];
    for (int i = 0; i < results.length; i++) {
      nanos[i] = results[i].nanosPerOperation;
      allocatedBytes[i] = results[i].allocatedBytesPerOperation;
    }
    return new BenchmarkResult(
        median(nanos), median(allocatedBytes), results[results.length - 1].estimate);
  }

  private static double median(double[] values) {
    Arrays.sort(values);
    int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2;
  }

  private static List<IMeasurementSchema> createSchemas(int count) {
    List<IMeasurementSchema> schemas = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
    }
    return schemas;
  }

  private static List<InsertRowNode> createRows(int rowCount, int columnCount)
      throws MetadataException {
    List<InsertRowNode> rows = new ArrayList<>(rowCount);
    for (int row = 0; row < rowCount; row++) {
      String[] measurements = new String[columnCount];
      TSDataType[] dataTypes = new TSDataType[columnCount];
      MeasurementSchema[] schemas = new MeasurementSchema[columnCount];
      Object[] values = new Object[columnCount];
      int offset = row % columnCount;
      for (int column = 0; column < columnCount; column++) {
        int sourceColumn = (column + offset) % columnCount;
        measurements[column] = "s" + sourceColumn;
        dataTypes[column] = TSDataType.INT32;
        schemas[column] = new MeasurementSchema(measurements[column], TSDataType.INT32);
        values[column] = ((row * columnCount + column) & 3) == 0 ? null : row + sourceColumn;
      }
      rows.add(
          new InsertRowNode(
              new PlanNodeId("benchmark"),
              new PartialPath(DEVICE_PATH),
              true,
              measurements,
              dataTypes,
              schemas,
              row,
              values,
              false));
    }
    return rows;
  }

  private static final class BenchmarkResult {

    private final double nanosPerOperation;
    private final double allocatedBytesPerOperation;
    private final long estimate;

    private BenchmarkResult(
        double nanosPerOperation, double allocatedBytesPerOperation, long estimate) {
      this.nanosPerOperation = nanosPerOperation;
      this.allocatedBytesPerOperation = allocatedBytesPerOperation;
      this.estimate = estimate;
    }
  }
}
