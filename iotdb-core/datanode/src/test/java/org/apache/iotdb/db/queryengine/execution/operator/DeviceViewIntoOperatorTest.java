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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewIntoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.rpc.RpcUtils.SUCCESS_STATUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DeviceViewIntoOperatorTest {

  private static final String TEST_SG = "root.test";

  private Map<String, Map<PartialPath, Map<String, InputLocation>>>
      deviceToTargetPathSourceInputLocationMap;
  private Map<String, Map<PartialPath, Map<String, TSDataType>>> deviceToTargetPathDataTypeMap;
  private Map<String, Boolean> targetDeviceToAlignedMap;
  private Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap;
  private Map<String, InputLocation> sourceColumnToInputLocationMap;
  private List<TSDataType> inputColumnTypes;
  private List<Integer> deviceColumnIndex;

  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private DeviceViewIntoOperator operator;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    IoTDBDescriptor.getInstance().getConfig().setSelectIntoInsertTabletPlanRowLimit(4);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(512);

    deviceToTargetPathSourceInputLocationMap = new HashMap<>();
    deviceToTargetPathDataTypeMap = new HashMap<>();
    targetDeviceToAlignedMap = new HashMap<>();
    deviceToSourceTargetPathPairListMap = new HashMap<>();
    sourceColumnToInputLocationMap = new HashMap<>();
    inputColumnTypes = new ArrayList<>();
    deviceColumnIndex = new ArrayList<>();

    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, TEST_SG);
  }

  @After
  public void tearDown() throws Exception {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  private DeviceViewIntoOperator createAndInitOperatorForSingleDevices(int sensorNum) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);

    // Create SeriesScanOperator for each sensor
    List<Operator> scanOperators = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<ColumnMerger> columnMergers = new ArrayList<>();

    for (int i = 0; i < sensorNum; i++) {
      scanOperators.add(
          createSeriesScanOperator(driverContext, i, "device0", i, measurementSchemas.get(i)));
      dataTypes.add(TSDataType.INT32);
      columnMergers.add(new SingleColumnMerger(new InputLocation(i, 0), new AscTimeComparator()));
    }

    FullOuterTimeJoinOperator timeJoinOperator =
        createTimeJoinOperator(driverContext, sensorNum, scanOperators, dataTypes, columnMergers);

    // Prepare data types for SingleDeviceViewOperator (device column + sensor columns)
    inputColumnTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      deviceColumnIndex.add(i + 1);
      inputColumnTypes.add(TSDataType.INT32); // Sensor columns
    }

    SingleDeviceViewOperator singleDeviceViewOperator =
        new SingleDeviceViewOperator(
            addOperatorContext(driverContext, sensorNum + 1, SingleDeviceViewOperator.class),
            TEST_SG + ".device0",
            timeJoinOperator,
            deviceColumnIndex,
            inputColumnTypes);

    return createTestDeviceViewIntoOperator(
        driverContext,
        sensorNum + 2,
        singleDeviceViewOperator,
        inputColumnTypes,
        instanceNotificationExecutor);
  }

  private DeviceViewIntoOperator createAndInitOperatorForMultipleDevices(
      int deviceNum, int sensorNum) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);

    List<IDeviceID> devices = new ArrayList<>();
    List<Operator> deviceOperators = new ArrayList<>();
    List<List<Integer>> deviceColumnIndexes = new ArrayList<>();

    List<Integer> singleDeviceColumnIndex = new ArrayList<>();
    for (int i = 0; i < sensorNum; i++) {
      singleDeviceColumnIndex.add(i + 1);
    }

    int operatorIndex = 0;
    for (int deviceIdx = 0; deviceIdx < deviceNum; deviceIdx++) {
      String device = "device" + deviceIdx;

      List<Operator> scanOperators = new ArrayList<>();
      List<TSDataType> scanDataTypes = new ArrayList<>();
      List<ColumnMerger> columnMergers = new ArrayList<>();

      for (int sensorIdx = 0; sensorIdx < sensorNum; sensorIdx++) {
        scanOperators.add(
            createSeriesScanOperator(
                driverContext,
                operatorIndex,
                device,
                sensorIdx,
                measurementSchemas.get(sensorIdx)));
        scanDataTypes.add(TSDataType.INT32);
        columnMergers.add(
            new SingleColumnMerger(new InputLocation(sensorIdx, 0), new AscTimeComparator()));
        operatorIndex++;
      }

      FullOuterTimeJoinOperator timeJoinOperator =
          createTimeJoinOperator(
              driverContext, operatorIndex, scanOperators, scanDataTypes, columnMergers);
      operatorIndex++;

      devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_SG + "." + device));
      deviceOperators.add(timeJoinOperator);
      deviceColumnIndexes.add(singleDeviceColumnIndex);
    }

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      dataTypes.add(TSDataType.INT32); // Sensor columns
    }

    DeviceViewOperator deviceViewOperator =
        new DeviceViewOperator(
            addOperatorContext(driverContext, operatorIndex, DeviceViewOperator.class),
            devices,
            deviceOperators,
            deviceColumnIndexes,
            dataTypes);

    inputColumnTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      deviceColumnIndex.add(i + 1);
      inputColumnTypes.add(TSDataType.INT32); // Sensor columns
    }

    return createTestDeviceViewIntoOperator(
        driverContext,
        operatorIndex + 1,
        deviceViewOperator,
        inputColumnTypes,
        instanceNotificationExecutor);
  }

  /** Test scenario 1: single device with small amount of data, should return in single TsBlock */
  @Test
  public void testSingleDeviceSmallData() throws Exception {
    prepareDeviceData("device0", 2);
    operator = createAndInitOperatorForSingleDevices(2);

    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      result = operator.next();
    }
    assertNotNull(result);
    assertEquals(2, result.getPositionCount());

    // Verify 4 value columns (device, source, target, count)
    assertEquals(4, result.getValueColumnCount());
    assertTrue(operator.isFinished());
  }

  /** Test scenario 2: Single device with result set exceeds maxTsBlockSize */
  @Test
  public void testSingleDeviceExceedsMaxTsBlockSize() throws Exception {
    prepareDeviceData("device0", 10);
    operator = createAndInitOperatorForSingleDevices(10);

    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      result = operator.next();
    }
    assertNotNull(result);
    assertEquals(10, result.getPositionCount());

    // Verify 4 value columns (device, source, target, count)
    assertEquals(4, result.getValueColumnCount());
    assertTrue(operator.isFinished());
  }

  /** Test scenario 3: Multiple device with small amount of data */
  @Test
  public void testMultipleDeviceSmallData() throws Exception {
    prepareDeviceData("device0", 1);
    prepareDeviceData("device1", 1);
    operator = createAndInitOperatorForMultipleDevices(2, 1);

    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      result = operator.next();
    }
    assertNotNull(result);
    assertEquals(2, result.getPositionCount());

    // Verify 4 value columns (device, source, target, count)
    assertEquals(4, result.getValueColumnCount());
    assertTrue(operator.isFinished());
  }

  /** Test scenario 4: Multiple devices, total size exceeds maxTsBlockSize */
  @Test
  public void testMultipleDevicesExceedsTsBlockSize() throws Exception {
    prepareDeviceData("device0", 2);
    prepareDeviceData("device1", 2);
    prepareDeviceData("device2", 2);
    operator = createAndInitOperatorForMultipleDevices(3, 2);

    int totalRows = 0;
    int totalBatches = 0;
    // Loop through all batches
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock result = operator.next();
      if (result != null && !result.isEmpty()) {
        totalRows += result.getPositionCount();
        totalBatches += 1;
      }
    }

    assertEquals(6, totalRows);
    assertEquals(2, totalBatches);
    assertTrue(operator.isFinished());
  }

  /**
   * Helper method: Prepare test data for specified device
   *
   * @param device Device name
   * @param sensorNum Number of path pairs for this device
   */
  private void prepareDeviceData(String device, int sensorNum) throws Exception {
    String sourceDevicePath = TEST_SG + "." + device;
    String targetDevicePath = TEST_SG + ".new_" + device;

    Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputMap =
        deviceToTargetPathSourceInputLocationMap.computeIfAbsent(
            sourceDevicePath, k -> new HashMap<>());
    Map<PartialPath, Map<String, TSDataType>> targetDataTypeMap =
        deviceToTargetPathDataTypeMap.computeIfAbsent(sourceDevicePath, k -> new HashMap<>());
    List<Pair<String, PartialPath>> pairList =
        deviceToSourceTargetPathPairListMap.computeIfAbsent(
            sourceDevicePath, k -> new ArrayList<>());

    Map<String, InputLocation> columnToInputLocationMap = new HashMap<>();
    Map<String, TSDataType> dataTypeMap = new HashMap<>();

    columnToInputLocationMap.put(ColumnHeaderConstant.DEVICE, new InputLocation(0, 0));
    dataTypeMap.put(ColumnHeaderConstant.DEVICE, TSDataType.TEXT);
    sourceColumnToInputLocationMap.put(ColumnHeaderConstant.DEVICE, new InputLocation(0, 0));

    for (int i = 0; i < sensorNum; i++) {
      String targetMeasurement = "sensor" + i;
      String targetPath = targetDevicePath + "." + targetMeasurement;
      PartialPath targetPartialPath = new PartialPath(targetPath);

      pairList.add(new Pair<>(targetMeasurement, targetPartialPath));
      columnToInputLocationMap.put(targetMeasurement, new InputLocation(0, i + 1));
      dataTypeMap.put(targetMeasurement, TSDataType.INT32);

      sourceColumnToInputLocationMap.put(targetMeasurement, new InputLocation(0, i + 1));
    }
    PartialPath targetDevicePartialPath = new PartialPath(targetDevicePath);
    targetPathToSourceInputMap.put(targetDevicePartialPath, columnToInputLocationMap);
    targetDataTypeMap.put(targetDevicePartialPath, dataTypeMap);

    targetDeviceToAlignedMap.put(targetDevicePath, false);
  }

  private SeriesScanOperator createSeriesScanOperator(
      DriverContext driverContext,
      int index,
      String device,
      int sensorIdx,
      IMeasurementSchema measurementSchema) {
    PlanNodeId planNodeId = new PlanNodeId(String.valueOf(index));
    driverContext.addOperatorContext(index, planNodeId, SeriesScanOperator.class.getSimpleName());

    Set<String> allSensors = new HashSet<>();
    allSensors.add("sensor" + sensorIdx);
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);

    IFullPath measurementPath =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_SG + "." + device), measurementSchema);

    SeriesScanOperator seriesScanOperator =
        new SeriesScanOperator(
            driverContext.getOperatorContexts().get(index),
            planNodeId,
            measurementPath,
            Ordering.ASC,
            scanOptionsBuilder.build());
    seriesScanOperator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanOperator;
  }

  private FullOuterTimeJoinOperator createTimeJoinOperator(
      DriverContext driverContext,
      int index,
      List<Operator> scanOperators,
      List<TSDataType> dataTypes,
      List<ColumnMerger> columnMergers) {
    addOperatorContext(driverContext, index, FullOuterTimeJoinOperator.class);
    return new FullOuterTimeJoinOperator(
        driverContext.getOperatorContexts().get(index),
        scanOperators,
        Ordering.ASC,
        dataTypes,
        columnMergers,
        new AscTimeComparator());
  }

  private OperatorContext addOperatorContext(
      DriverContext driverContext, int index, Class<?> operatorClass) {
    driverContext.addOperatorContext(
        index, new PlanNodeId(String.valueOf(index)), operatorClass.getSimpleName());
    return driverContext.getOperatorContexts().get(index);
  }

  private DeviceViewIntoOperator createTestDeviceViewIntoOperator(
      DriverContext driverContext,
      int index,
      Operator child,
      List<TSDataType> types,
      ExecutorService executor) {
    addOperatorContext(driverContext, index, TestDeviceViewIntoOperator.class);
    return new TestDeviceViewIntoOperator(
        driverContext.getOperatorContexts().get(index),
        child,
        types,
        deviceToTargetPathSourceInputLocationMap,
        deviceToTargetPathDataTypeMap,
        targetDeviceToAlignedMap,
        deviceToSourceTargetPathPairListMap,
        sourceColumnToInputLocationMap,
        executor,
        100);
  }

  /**
   * Test version of DeviceViewIntoOperator that mocks the write operation. Instead of actually
   * executing insertTablets, it returns an immediately completed Future.
   */
  private static class TestDeviceViewIntoOperator extends DeviceViewIntoOperator {

    public TestDeviceViewIntoOperator(
        OperatorContext operatorContext,
        Operator child,
        List<TSDataType> inputColumnTypes,
        Map<String, Map<PartialPath, Map<String, InputLocation>>>
            deviceToTargetPathSourceInputLocationMap,
        Map<String, Map<PartialPath, Map<String, TSDataType>>> deviceToTargetPathDataTypeMap,
        Map<String, Boolean> targetDeviceToAlignedMap,
        Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap,
        Map<String, InputLocation> sourceColumnToInputLocationMap,
        ExecutorService intoOperationExecutor,
        long statementSizePerLine) {
      super(
          operatorContext,
          child,
          inputColumnTypes,
          deviceToTargetPathSourceInputLocationMap,
          deviceToTargetPathDataTypeMap,
          targetDeviceToAlignedMap,
          deviceToSourceTargetPathPairListMap,
          sourceColumnToInputLocationMap,
          intoOperationExecutor,
          statementSizePerLine);
    }

    @Override
    protected void executeInsertMultiTabletsStatement(
        InsertMultiTabletsStatement insertMultiTabletsStatement) {
      // Mock the write operation by setting an immediately completed Future
      writeOperationFuture = immediateFuture(SUCCESS_STATUS);
    }
  }
}
