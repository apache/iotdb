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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeIntoOperator;
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

public class TreeIntoOperatorTest {

  private static final String TEST_SG = "root.test";

  private List<Pair<String, PartialPath>> sourceTargetPathPairList;
  private Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap;
  private Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap;
  private Map<String, Boolean> targetDeviceToAlignedMap;
  private List<TSDataType> inputColumnTypes;

  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private TreeIntoOperator operator;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(512);

    sourceTargetPathPairList = new ArrayList<>();
    targetPathToSourceInputLocationMap = new HashMap<>();
    targetPathToDataTypeMap = new HashMap<>();
    targetDeviceToAlignedMap = new HashMap<>();
    inputColumnTypes = new ArrayList<>();

    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, TEST_SG);
  }

  @After
  public void tearDown() throws Exception {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  /**
   * Common setup method to create and initialize TreeIntoOperator with SeriesScanOperator as child.
   */
  private TreeIntoOperator createAndInitOperator(int sensorNum) {
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
    // Add context for TreeIntoOperator
    driverContext.addOperatorContext(
        sensorNum + 1,
        new PlanNodeId(String.valueOf(sensorNum + 1)),
        TestTreeIntoOperator.class.getSimpleName());

    // Join all sensor scans with FullOuterTimeJoinOperator
    FullOuterTimeJoinOperator timeJoinOperator =
        new FullOuterTimeJoinOperator(
            driverContext.getOperatorContexts().get(sensorNum),
            scanOperators,
            Ordering.ASC,
            dataTypes,
            columnMergers,
            new AscTimeComparator());

    return new TestTreeIntoOperator(
        driverContext.getOperatorContexts().get(sensorNum + 1),
        timeJoinOperator,
        inputColumnTypes,
        targetPathToSourceInputLocationMap,
        targetPathToDataTypeMap,
        targetDeviceToAlignedMap,
        sourceTargetPathPairList,
        instanceNotificationExecutor,
        100);
  }

  /** Test scenario 1: small amount of data, should return in single TsBlock */
  @Test
  public void testAllResultsInSingleTsBlock() throws Exception {
    prepareSourceTargetPairs(2);
    operator = createAndInitOperator(2);

    TsBlock result = null;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      result = operator.next();
    }
    assertNotNull(result);
    assertEquals(2, result.getPositionCount());

    // Verify 3 value columns (source, target, count)
    assertEquals(3, result.getValueColumnCount());
    assertTrue(operator.isFinished());
  }

  /**
   * Test scenario 2: Result set exceeds maxTsBlockSize, should return in batches Create a large
   * number of path pairs to trigger size limit
   */
  @Test
  public void testResultsExceedMaxTsBlockSize() throws Exception {
    prepareSourceTargetPairs(10);
    operator = createAndInitOperator(10);

    int totalRows = 0;
    // Loop through all batches
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock result = operator.next();
      if (result != null && !result.isEmpty()) {
        int rowCount = result.getPositionCount();
        assertTrue(rowCount == 4 || rowCount == 2);
        totalRows += rowCount;
      }
    }

    // Verify all data is returned
    assertEquals(10, totalRows);
    assertTrue(operator.isFinished());
  }

  private void prepareSourceTargetPairs(int sensorNum) throws Exception {
    String sourceDevicePath = TEST_SG + ".device0";
    String targetDevicePath = TEST_SG + ".new_device0";
    targetDeviceToAlignedMap.put(targetDevicePath, false);
    PartialPath targetDevicePartialPath = new PartialPath(targetDevicePath);

    for (int i = 0; i < sensorNum; i++) {
      String targetMeasurement = "sensor" + i;
      String sourcePath = sourceDevicePath + "." + targetMeasurement;
      String targetPath = targetDevicePath + "." + targetMeasurement;

      sourceTargetPathPairList.add(new Pair<>(sourcePath, new PartialPath(targetPath)));

      Map<String, InputLocation> inputLocationMap =
          targetPathToSourceInputLocationMap.computeIfAbsent(
              targetDevicePartialPath, k -> new HashMap<>());
      // Each sensor comes from a different input location (different SeriesScanOperator)
      inputLocationMap.put(targetMeasurement, new InputLocation(0, i));

      // Prepare targetPathToDataTypeMap
      Map<String, TSDataType> dataTypeMap =
          targetPathToDataTypeMap.computeIfAbsent(targetDevicePartialPath, k -> new HashMap<>());
      dataTypeMap.put(targetMeasurement, TSDataType.INT32);
    }

    // Prepare inputColumnTypes (one column for each sensor)
    for (int i = 0; i < sensorNum; i++) {
      inputColumnTypes.add(TSDataType.INT32);
    }
  }

  /**
   * Test version of TreeIntoOperator that mocks the write operation. Instead of actually executing
   * insertTablets, it returns an immediately completed Future.
   */
  private static class TestTreeIntoOperator extends TreeIntoOperator {

    public TestTreeIntoOperator(
        OperatorContext operatorContext,
        Operator child,
        List<TSDataType> inputColumnTypes,
        Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
        Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
        Map<String, Boolean> targetDeviceToAlignedMap,
        List<Pair<String, PartialPath>> sourceTargetPathPairList,
        ExecutorService intoOperationExecutor,
        long statementSizePerLine) {
      super(
          operatorContext,
          child,
          inputColumnTypes,
          targetPathToSourceInputLocationMap,
          targetPathToDataTypeMap,
          targetDeviceToAlignedMap,
          sourceTargetPathPairList,
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
