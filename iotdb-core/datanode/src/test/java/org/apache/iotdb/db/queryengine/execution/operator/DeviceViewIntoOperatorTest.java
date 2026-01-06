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

import io.airlift.units.Duration;
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
import java.util.concurrent.TimeUnit;

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
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    OperatorContext.setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

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
      PlanNodeId planNodeId = new PlanNodeId(String.valueOf(i));
      driverContext.addOperatorContext(i, planNodeId, SeriesScanOperator.class.getSimpleName());

      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor" + i);
      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);

      IFullPath measurementPath =
          new NonAlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_SG + ".device0"),
              measurementSchemas.get(i));

      SeriesScanOperator seriesScanOperator =
          new SeriesScanOperator(
              driverContext.getOperatorContexts().get(i),
              planNodeId,
              measurementPath,
              Ordering.ASC,
              scanOptionsBuilder.build());
      seriesScanOperator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

      scanOperators.add(seriesScanOperator);
      dataTypes.add(TSDataType.INT32);
      columnMergers.add(new SingleColumnMerger(new InputLocation(i, 0), new AscTimeComparator()));
    }

    // Add context for FullOuterTimeJoinOperator
    driverContext.addOperatorContext(
        sensorNum,
        new PlanNodeId(String.valueOf(sensorNum)),
        FullOuterTimeJoinOperator.class.getSimpleName());
    // Add context for SingleDeviceViewOperator
    driverContext.addOperatorContext(
        sensorNum + 1,
        new PlanNodeId(String.valueOf(sensorNum + 1)),
        SingleDeviceViewOperator.class.getSimpleName());
    // Add context for DeviceViewIntoOperator
    driverContext.addOperatorContext(
        sensorNum + 2,
        new PlanNodeId(String.valueOf(sensorNum + 2)),
        TestDeviceViewIntoOperator.class.getSimpleName());

    // Join all sensor scans with FullOuterTimeJoinOperator
    FullOuterTimeJoinOperator timeJoinOperator =
        new FullOuterTimeJoinOperator(
            driverContext.getOperatorContexts().get(sensorNum),
            scanOperators,
            Ordering.ASC,
            dataTypes,
            columnMergers,
            new AscTimeComparator());

    // Prepare data types for SingleDeviceViewOperator (device column + sensor columns)
    inputColumnTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      deviceColumnIndex.add(i + 1);
      inputColumnTypes.add(TSDataType.INT32); // Sensor columns
    }

    // Wrap FullOuterTimeJoinOperator with DeviceViewOperator to add device column
    SingleDeviceViewOperator singleDeviceViewOperator =
        new SingleDeviceViewOperator(
            driverContext.getOperatorContexts().get(sensorNum + 1),
            TEST_SG + ".device0",
            timeJoinOperator,
            deviceColumnIndex,
            inputColumnTypes);

    return new TestDeviceViewIntoOperator(
        driverContext.getOperatorContexts().get(sensorNum + 2),
        singleDeviceViewOperator,
        inputColumnTypes,
        deviceToTargetPathSourceInputLocationMap,
        deviceToTargetPathDataTypeMap,
        targetDeviceToAlignedMap,
        deviceToSourceTargetPathPairListMap,
        sourceColumnToInputLocationMap,
        instanceNotificationExecutor,
        100);
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

    // Prepare device IDs, device operators, and device column indexes
    List<IDeviceID> devices = new ArrayList<>();
    List<Operator> deviceOperators = new ArrayList<>();
    List<List<Integer>> deviceColumnIndexes = new ArrayList<>();

    // Prepare data types for DeviceViewOperator (device column + sensor columns)
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      dataTypes.add(TSDataType.INT32); // Sensor columns
    }

    // Prepare device column index for each device (same for all devices)
    List<Integer> singleDeviceColumnIndex = new ArrayList<>();
    for (int i = 0; i < sensorNum; i++) {
      singleDeviceColumnIndex.add(i + 1);
    }

    int operatorIndex = 0;
    for (int deviceIdx = 0; deviceIdx < deviceNum; deviceIdx++) {
      String device = "device" + deviceIdx;

      // Create SeriesScanOperator for each sensor of this device
      List<Operator> scanOperators = new ArrayList<>();
      List<TSDataType> scanDataTypes = new ArrayList<>();
      List<ColumnMerger> columnMergers = new ArrayList<>();

      for (int sensorIdx = 0; sensorIdx < sensorNum; sensorIdx++) {
        PlanNodeId planNodeId = new PlanNodeId(String.valueOf(operatorIndex));
        driverContext.addOperatorContext(
            operatorIndex, planNodeId, SeriesScanOperator.class.getSimpleName());

        Set<String> allSensors = new HashSet<>();
        allSensors.add("sensor" + sensorIdx);
        SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
        scanOptionsBuilder.withAllSensors(allSensors);

        IFullPath measurementPath =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_SG + "." + device),
                measurementSchemas.get(sensorIdx));

        SeriesScanOperator seriesScanOperator =
            new SeriesScanOperator(
                driverContext.getOperatorContexts().get(operatorIndex),
                planNodeId,
                measurementPath,
                Ordering.ASC,
                scanOptionsBuilder.build());
        seriesScanOperator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

        scanOperators.add(seriesScanOperator);
        scanDataTypes.add(TSDataType.INT32);
        columnMergers.add(new SingleColumnMerger(new InputLocation(sensorIdx, 0), new AscTimeComparator()));
        operatorIndex++;
      }

      // Add context for FullOuterTimeJoinOperator for this device
      int joinOperatorIndex = operatorIndex;
      PlanNodeId joinPlanNodeId = new PlanNodeId(String.valueOf(joinOperatorIndex));
      driverContext.addOperatorContext(
          joinOperatorIndex,
          joinPlanNodeId,
          FullOuterTimeJoinOperator.class.getSimpleName());
      operatorIndex++;

      // Join all sensor scans for this device
      FullOuterTimeJoinOperator timeJoinOperator =
          new FullOuterTimeJoinOperator(
              driverContext.getOperatorContexts().get(joinOperatorIndex),
              scanOperators,
              Ordering.ASC,
              scanDataTypes,
              columnMergers,
              new AscTimeComparator());

      devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_SG + "." + device));
      deviceOperators.add(timeJoinOperator);
      deviceColumnIndexes.add(singleDeviceColumnIndex);
    }

    // Add context for DeviceViewOperator
    int deviceViewOperatorIndex = operatorIndex;
    PlanNodeId deviceViewPlanNodeId = new PlanNodeId(String.valueOf(deviceViewOperatorIndex));
    driverContext.addOperatorContext(
        deviceViewOperatorIndex,
        deviceViewPlanNodeId,
        DeviceViewOperator.class.getSimpleName());
    operatorIndex++;

    // Add context for DeviceViewIntoOperator
    int intoOperatorIndex = operatorIndex;
    PlanNodeId intoPlanNodeId = new PlanNodeId(String.valueOf(intoOperatorIndex));
    driverContext.addOperatorContext(
        intoOperatorIndex,
        intoPlanNodeId,
        TestDeviceViewIntoOperator.class.getSimpleName());

    // Wrap all device join operators with DeviceViewOperator to add device column
    DeviceViewOperator deviceViewOperator =
        new DeviceViewOperator(
            driverContext.getOperatorContexts().get(deviceViewOperatorIndex),
            devices,
            deviceOperators,
            deviceColumnIndexes,
            dataTypes);

    // Prepare inputColumnTypes for DeviceViewIntoOperator
    // DeviceViewOperator output: device column + sensor columns
    inputColumnTypes.add(TSDataType.TEXT); // Device column
    for (int i = 0; i < sensorNum; i++) {
      deviceColumnIndex.add(i + 1);
      inputColumnTypes.add(TSDataType.INT32); // Sensor columns
    }

    return new TestDeviceViewIntoOperator(
        driverContext.getOperatorContexts().get(intoOperatorIndex),
        deviceViewOperator,
        inputColumnTypes,
        deviceToTargetPathSourceInputLocationMap,
        deviceToTargetPathDataTypeMap,
        targetDeviceToAlignedMap,
        deviceToSourceTargetPathPairListMap,
        sourceColumnToInputLocationMap,
        instanceNotificationExecutor,
        100);
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

  /**
   * Test scenario 2: Single device with result set exceeds maxTsBlockSize, should return in batches
   */
  @Test
  public void testSingleDeviceExceedsMaxTsBlockSize() throws Exception {
    prepareDeviceData("device0", 8);
    operator = createAndInitOperatorForSingleDevices(8);

    int totalRows = 0;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock result = operator.next();
      if (result != null && !result.isEmpty()) {
        int rowCount = result.getPositionCount();
        assertTrue(rowCount == 5 || rowCount == 3);
        totalRows += rowCount;
      }
    }

    // Verify all data is returned
    assertEquals(8, totalRows);
    assertTrue(operator.isFinished());
  }

  /**
   * Test scenario 3: Multiple devices, each with small amount of data, should return in single
   * TsBlock
   */
  @Test
  public void testMultipleDevicesSmallData() throws Exception {
    prepareDeviceData("device0", 2);
    prepareDeviceData("device1", 2);
    operator = createAndInitOperatorForMultipleDevices(2, 2);

    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      result = operator.next();
    }
    assertNotNull(result);
    assertEquals(4, result.getPositionCount());

    // Verify 4 value columns (device, source, target, count)
    assertEquals(4, result.getValueColumnCount());
    assertTrue(operator.isFinished());
  }

  /**
   * Test scenario 4: Multiple devices, total size exceeds maxTsBlockSize, should batch across
   * devices
   */
  @Test
  public void testMultipleDevicesExceedsTsBlockSize() throws Exception {
    prepareDeviceData("device0", 6);
    prepareDeviceData("device1", 6);
    prepareDeviceData("device2", 6);
    operator = createAndInitOperatorForMultipleDevices(3, 6);

    int totalRows = 0;
    // Loop through all batches
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock result = operator.next();
      if (result != null && !result.isEmpty()) {
        int rowCount = result.getPositionCount();
        assertTrue(rowCount == 5 || rowCount == 3);
        totalRows += rowCount;
      }
    }

    // Verify all data is returned
    assertEquals(18, totalRows);
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
