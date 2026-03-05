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
package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.ChangePointOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import io.airlift.units.Duration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChangePointOperatorTest {

  private static final String SG_NAME = "root.ChangePointOperatorTest";
  private static final String DEVICE_ID = SG_NAME + ".device0";
  private static final String MEASUREMENT = "sensor0";
  private static final long FLUSH_INTERVAL = 20;

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor;

  @Before
  public void setUp() {
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
  }

  @After
  public void tearDown() throws IOException {
    for (TsFileResource r : seqResources) {
      r.remove();
    }
    for (TsFileResource r : unSeqResources) {
      r.remove();
    }
    seqResources.clear();
    unSeqResources.clear();
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    instanceNotificationExecutor.shutdown();
  }

  /**
   * All values are distinct (0, 1, 2, ..., 99). Every point is a change point, so the operator
   * should output all 100 points.
   */
  @Test
  public void testAllDistinctValues() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(false);
    List<long[]> result = collectResults(operator);

    assertEquals(100, result.size());
    for (int i = 0; i < 100; i++) {
      assertEquals(i, result.get(i)[0]);
      assertEquals(i, result.get(i)[1]);
    }
    operator.close();
  }

  /**
   * All values are the same constant (42). Only the first point should be emitted as a change
   * point.
   */
  @Test
  public void testAllSameValues() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 42;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(false);
    List<long[]> result = collectResults(operator);

    assertEquals(1, result.size());
    assertEquals(0, result.get(0)[0]);
    assertEquals(42, result.get(0)[1]);
    operator.close();
  }

  /**
   * All values are the same constant (42), with statistics optimization enabled. Should produce the
   * same result as without statistics: only the first point.
   */
  @Test
  public void testAllSameValuesWithStatistics() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 42;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(true);
    List<long[]> result = collectResults(operator);

    assertEquals(1, result.size());
    assertEquals(0, result.get(0)[0]);
    assertEquals(42, result.get(0)[1]);
    operator.close();
  }

  /**
   * Values form runs of consecutive duplicates: 10 x value_A, 10 x value_B, 10 x value_C, ... The
   * operator should output only the first point of each run.
   */
  @Test
  public void testConsecutiveDuplicateRuns() throws Exception {
    int runLength = 10;
    int numRuns = 10;
    int totalPoints = runLength * numRuns;
    int[] values = new int[totalPoints];
    for (int run = 0; run < numRuns; run++) {
      for (int j = 0; j < runLength; j++) {
        values[run * runLength + j] = (run + 1) * 100;
      }
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(false);
    List<long[]> result = collectResults(operator);

    assertEquals(numRuns, result.size());
    for (int run = 0; run < numRuns; run++) {
      assertEquals(run * runLength, result.get(run)[0]);
      assertEquals((run + 1) * 100, result.get(run)[1]);
    }
    operator.close();
  }

  /**
   * Same data as testConsecutiveDuplicateRuns but with statistics optimization enabled. The result
   * must be identical.
   */
  @Test
  public void testConsecutiveDuplicateRunsWithStatistics() throws Exception {
    int runLength = 10;
    int numRuns = 10;
    int totalPoints = runLength * numRuns;
    int[] values = new int[totalPoints];
    for (int run = 0; run < numRuns; run++) {
      for (int j = 0; j < runLength; j++) {
        values[run * runLength + j] = (run + 1) * 100;
      }
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(true);
    List<long[]> result = collectResults(operator);

    assertEquals(numRuns, result.size());
    for (int run = 0; run < numRuns; run++) {
      assertEquals(run * runLength, result.get(run)[0]);
      assertEquals((run + 1) * 100, result.get(run)[1]);
    }
    operator.close();
  }

  /**
   * Alternating pattern: each point differs from the previous one (1, 2, 1, 2, ...). All points are
   * change points.
   */
  @Test
  public void testAlternatingValues() throws Exception {
    int[] values = new int[60];
    for (int i = 0; i < 60; i++) {
      values[i] = (i % 2 == 0) ? 1 : 2;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operator = createOperator(false);
    List<long[]> result = collectResults(operator);

    assertEquals(60, result.size());
    for (int i = 0; i < 60; i++) {
      assertEquals(i, result.get(i)[0]);
      assertEquals((i % 2 == 0) ? 1 : 2, result.get(i)[1]);
    }
    operator.close();
  }

  /** A single data point. The operator should emit exactly one change point. */
  @Test
  public void testSinglePoint() throws Exception {
    prepareSeqFile(0, new int[] {99});

    ChangePointOperator operator = createOperator(false);
    List<long[]> result = collectResults(operator);

    assertEquals(1, result.size());
    assertEquals(0, result.get(0)[0]);
    assertEquals(99, result.get(0)[1]);
    operator.close();
  }

  /**
   * Data spans multiple TsFile pages (flush every 20 rows). Each page has a constant value, but
   * value changes across pages. With statistics enabled, entire pages should be skipped or emit a
   * single point.
   *
   * <p>Page 0 (time 0-19): all 100, Page 1 (time 20-39): all 100, Page 2 (time 40-59): all 200
   *
   * <p>Expected: 2 change points: (0, 100) and (40, 200)
   */
  @Test
  public void testStatisticsSkipAcrossPages() throws Exception {
    int[] values = new int[60];
    for (int i = 0; i < 40; i++) {
      values[i] = 100;
    }
    for (int i = 40; i < 60; i++) {
      values[i] = 200;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operatorWithStats = createOperator(true);
    List<long[]> resultWithStats = collectResults(operatorWithStats);
    operatorWithStats.close();

    assertEquals(2, resultWithStats.size());
    assertEquals(0, resultWithStats.get(0)[0]);
    assertEquals(100, resultWithStats.get(0)[1]);
    assertEquals(40, resultWithStats.get(1)[0]);
    assertEquals(200, resultWithStats.get(1)[1]);
  }

  /**
   * Same as testStatisticsSkipAcrossPages but without statistics. Verifies the raw-data path
   * produces the same result.
   */
  @Test
  public void testNoStatisticsAcrossPages() throws Exception {
    int[] values = new int[60];
    for (int i = 0; i < 40; i++) {
      values[i] = 100;
    }
    for (int i = 40; i < 60; i++) {
      values[i] = 200;
    }
    prepareSeqFile(0, values);

    ChangePointOperator operatorNoStats = createOperator(false);
    List<long[]> resultNoStats = collectResults(operatorNoStats);
    operatorNoStats.close();

    assertEquals(2, resultNoStats.size());
    assertEquals(0, resultNoStats.get(0)[0]);
    assertEquals(100, resultNoStats.get(0)[1]);
    assertEquals(40, resultNoStats.get(1)[0]);
    assertEquals(200, resultNoStats.get(1)[1]);
  }

  /**
   * Tests that statistics and non-statistics paths yield identical results for mixed data where
   * some pages are uniform and others are not.
   *
   * <p>Page 0 (0-19): all 5 (uniform), Page 1 (20-39): values 5,6,5,6,... (non-uniform), Page 2
   * (40-59): all 6 (uniform)
   */
  @Test
  public void testStatisticsAndRawPathConsistency() throws Exception {
    int[] values = new int[60];
    for (int i = 0; i < 20; i++) {
      values[i] = 5;
    }
    for (int i = 20; i < 40; i++) {
      values[i] = (i % 2 == 0) ? 5 : 6;
    }
    for (int i = 40; i < 60; i++) {
      values[i] = 6;
    }
    prepareSeqFile(0, values);

    ChangePointOperator opWithStats = createOperator(true);
    List<long[]> resultWithStats = collectResults(opWithStats);
    opWithStats.close();

    tearDownResources();

    // Re-create the same data
    for (int i = 0; i < 20; i++) {
      values[i] = 5;
    }
    for (int i = 20; i < 40; i++) {
      values[i] = (i % 2 == 0) ? 5 : 6;
    }
    for (int i = 40; i < 60; i++) {
      values[i] = 6;
    }
    prepareSeqFile(0, values);

    ChangePointOperator opNoStats = createOperator(false);
    List<long[]> resultNoStats = collectResults(opNoStats);
    opNoStats.close();

    assertEquals(resultNoStats.size(), resultWithStats.size());
    for (int i = 0; i < resultNoStats.size(); i++) {
      assertEquals(
          "Mismatch at index " + i + " timestamp",
          resultNoStats.get(i)[0],
          resultWithStats.get(i)[0]);
      assertEquals(
          "Mismatch at index " + i + " value", resultNoStats.get(i)[1], resultWithStats.get(i)[1]);
    }
  }

  /**
   * Multiple seq files. File 0: all value 10 (time 0-49), File 1: all value 20 (time 50-99). The
   * operator should output 2 change points across files.
   */
  @Test
  public void testMultipleFiles() throws Exception {
    int[] values1 = new int[50];
    for (int i = 0; i < 50; i++) {
      values1[i] = 10;
    }
    prepareSeqFile(0, values1);

    int[] values2 = new int[50];
    for (int i = 0; i < 50; i++) {
      values2[i] = 20;
    }
    prepareSeqFile(50, values2);

    ChangePointOperator operator = createOperator(true);
    List<long[]> result = collectResults(operator);
    operator.close();

    assertEquals(2, result.size());
    assertEquals(0, result.get(0)[0]);
    assertEquals(10, result.get(0)[1]);
    assertEquals(50, result.get(1)[0]);
    assertEquals(20, result.get(1)[1]);
  }

  /**
   * Multiple files where the value does NOT change across file boundary. File 0: all value 10 (time
   * 0-49), File 1: all value 10 (time 50-99). Should output only 1 change point.
   */
  @Test
  public void testMultipleFilesSameValue() throws Exception {
    int[] values1 = new int[50];
    for (int i = 0; i < 50; i++) {
      values1[i] = 10;
    }
    prepareSeqFile(0, values1);

    int[] values2 = new int[50];
    for (int i = 0; i < 50; i++) {
      values2[i] = 10;
    }
    prepareSeqFile(50, values2);

    ChangePointOperator operator = createOperator(true);
    List<long[]> result = collectResults(operator);
    operator.close();

    assertEquals(1, result.size());
    assertEquals(0, result.get(0)[0]);
    assertEquals(10, result.get(0)[1]);
  }

  /** Verifies isFinished() returns true after all data is consumed. */
  @Test
  public void testIsFinished() throws Exception {
    prepareSeqFile(0, new int[] {1, 1, 2, 2, 3});

    ChangePointOperator operator = createOperator(false);
    assertFalse(operator.isFinished());

    while (operator.hasNext()) {
      operator.next();
    }
    assertTrue(operator.isFinished());
    operator.close();
  }

  // ==================== Helper methods ====================

  private ChangePointOperator createOperator(boolean canUseStatistics) throws Exception {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema(MEASUREMENT, TSDataType.INT32));

    List<String> measurementColumnNames = new ArrayList<>();
    measurementColumnNames.add(MEASUREMENT);

    if (!canUseStatistics) {
      measurementSchemas.add(new MeasurementSchema("__dummy__", TSDataType.INT32));
      measurementColumnNames.add("__dummy__");
    }

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    allSensors.add("");

    IDeviceID deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_ID);
    DeviceEntry deviceEntry = new AlignedDeviceEntry(deviceId, new Binary[0]);
    List<DeviceEntry> deviceEntries = new ArrayList<>();
    deviceEntries.add(deviceEntry);

    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    columnSchemas.add(
        new ColumnSchema(
            MEASUREMENT,
            TypeFactory.getType(TSDataType.INT32),
            false,
            TsTableColumnCategory.FIELD));
    int[] columnsIndexArray = new int[] {0, 0};

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId, ChangePointOperator.class.getSimpleName());

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);

    ChangePointOperator.ChangePointOperatorParameter parameter =
        new ChangePointOperator.ChangePointOperatorParameter(
            driverContext.getOperatorContexts().get(0),
            planNodeId,
            columnSchemas,
            columnsIndexArray,
            deviceEntries,
            Ordering.ASC,
            scanOptionsBuilder.build(),
            measurementColumnNames,
            allSensors,
            measurementSchemas,
            0,
            idColumnIndex -> null);

    ChangePointOperator operator = new ChangePointOperator(parameter);

    operator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return operator;
  }

  /** Collects all (timestamp, int_value) pairs from the operator output. */
  private List<long[]> collectResults(ChangePointOperator operator) throws Exception {
    List<long[]> results = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock tsBlock = operator.next();
      if (tsBlock == null) {
        continue;
      }
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        long time = tsBlock.getColumn(0).getLong(i);
        int value = tsBlock.getColumn(1).getInt(i);
        results.add(new long[] {time, value});
      }
    }
    return results;
  }

  /**
   * Creates a sequential TsFile with the given INT32 values starting at timeOffset. Timestamps are
   * timeOffset, timeOffset+1, ..., timeOffset+values.length-1. The file is flushed every {@link
   * #FLUSH_INTERVAL} rows to create multiple pages.
   */
  private void prepareSeqFile(long timeOffset, int[] values) throws Exception {
    int fileIndex = seqResources.size();
    File file = new File(TestConstant.getTestTsFilePath(SG_NAME, 0, 0, fileIndex));
    TsFileResource resource = new TsFileResource(file);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.setMinPlanIndex(fileIndex);
    resource.setMaxPlanIndex(fileIndex);
    resource.setVersion(fileIndex);

    IMeasurementSchema schema =
        new MeasurementSchema(
            MEASUREMENT, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);

    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    TsFileWriter writer = new TsFileWriter(file);
    Map<String, IMeasurementSchema> template = new HashMap<>();
    template.put(schema.getMeasurementName(), schema);
    writer.registerSchemaTemplate("template0", template, true);
    writer.registerDevice(DEVICE_ID, "template0");

    for (int i = 0; i < values.length; i++) {
      long timestamp = timeOffset + i;
      TSRecord record = new TSRecord(DEVICE_ID, timestamp);
      record.addTuple(new IntDataPoint(MEASUREMENT, values[i]));
      writer.writeRecord(record);

      resource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_ID), timestamp);
      resource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_ID), timestamp);

      if ((i + 1) % FLUSH_INTERVAL == 0) {
        writer.flush();
      }
    }
    writer.close();

    seqResources.add(resource);
  }

  private void tearDownResources() throws IOException {
    for (TsFileResource r : seqResources) {
      r.remove();
    }
    for (TsFileResource r : unSeqResources) {
      r.remove();
    }
    seqResources.clear();
    unSeqResources.clear();
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }
}
