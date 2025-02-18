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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DeviceIteratorScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.TableOperatorGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.Operator.NOT_BLOCKED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NonAlignedTreeDeviceViewScanOperatorTreeTest {

  private static final String NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST =
      "root.NonAlignedTreeDeviceViewScanOperatorTreeTest";
  private final TableOperatorGenerator tableOperatorGenerator =
      new TableOperatorGenerator(new TestMatadata());
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas,
        deviceIds,
        seqResources,
        unSeqResources,
        NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testScanWithLimitAndOffset() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode();
      node.setPushDownOffset(500);
      node.setPushDownLimit(500);
      LocalExecutionPlanContext localExecutionPlanContext =
          new LocalExecutionPlanContext(
              new TypeProvider(), fragmentInstanceContext, new DataNodeQueryContext(1));
      operator =
          tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
      ((DataDriverContext) localExecutionPlanContext.getDriverContext())
          .getSourceOperators()
          .forEach(
              sourceOperator ->
                  sourceOperator.initQueryDataSource(
                      new QueryDataSource(seqResources, unSeqResources)));

      int count = 0;
      while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(node.getOutputSymbols().size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(3) instanceof TimeColumn);
        assertTrue(tsBlock.getColumn(4) instanceof RunLengthEncodedColumn);
        count += tsBlock.getPositionCount();
      }
      assertEquals(500, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {

        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanAndPushLimitToEachDevice() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode();
      node.setPushLimitToEachDevice(true);
      node.setPushDownLimit(500);
      LocalExecutionPlanContext localExecutionPlanContext =
          new LocalExecutionPlanContext(
              new TypeProvider(), fragmentInstanceContext, new DataNodeQueryContext(1));
      operator =
          tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
      ((DataDriverContext) localExecutionPlanContext.getDriverContext())
          .getSourceOperators()
          .forEach(
              sourceOperator ->
                  sourceOperator.initQueryDataSource(
                      new QueryDataSource(seqResources, unSeqResources)));

      int count = 0;
      while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(node.getOutputSymbols().size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(3) instanceof TimeColumn);
        assertTrue(tsBlock.getColumn(4) instanceof RunLengthEncodedColumn);
        count += tsBlock.getPositionCount();
      }
      assertEquals(1500, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {
        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicate() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode();
      node.setPushDownPredicate(
          new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN,
              new Symbol("sensor3").toSymbolReference(),
              new LongLiteral("1000")));
      node.getAssignments()
          .put(
              new Symbol("sensor3"),
              new ColumnSchema(
                  "sensor3",
                  TypeFactory.getType(TSDataType.INT32),
                  false,
                  TsTableColumnCategory.FIELD));

      Map<Symbol, Type> symbolTSDataTypeMap = new HashMap<>();
      symbolTSDataTypeMap.put(new Symbol("sensor0"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor1"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor2"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor3"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("time"), TypeFactory.getType(TypeEnum.TIMESTAMP));
      symbolTSDataTypeMap.put(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
      TypeProvider typeProvider = new TypeProvider(symbolTSDataTypeMap);
      LocalExecutionPlanContext localExecutionPlanContext =
          new LocalExecutionPlanContext(
              typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
      operator =
          tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
      ((DataDriverContext) localExecutionPlanContext.getDriverContext())
          .getSourceOperators()
          .forEach(
              sourceOperator ->
                  sourceOperator.initQueryDataSource(
                      new QueryDataSource(seqResources, unSeqResources)));

      int count = 0;
      while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(node.getOutputSymbols().size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(3) instanceof TimeColumn);
        assertTrue(tsBlock.getColumn(4) instanceof RunLengthEncodedColumn);
        count += tsBlock.getPositionCount();
      }
      assertEquals(1320, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {
        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithCannotPushDownPredicate() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode();
      node.setPushDownPredicate(
          new LogicalExpression(
              LogicalExpression.Operator.OR,
              Arrays.asList(
                  new ComparisonExpression(
                      ComparisonExpression.Operator.GREATER_THAN,
                      new Symbol("sensor3").toSymbolReference(),
                      new LongLiteral("1000")),
                  new ComparisonExpression(
                      ComparisonExpression.Operator.GREATER_THAN,
                      new Symbol("sensor1").toSymbolReference(),
                      new LongLiteral("1000")))));

      node.getAssignments()
          .put(
              new Symbol("sensor3"),
              new ColumnSchema(
                  "sensor3",
                  TypeFactory.getType(TSDataType.INT32),
                  false,
                  TsTableColumnCategory.FIELD));

      Map<Symbol, Type> symbolTSDataTypeMap = new HashMap<>();
      symbolTSDataTypeMap.put(new Symbol("sensor0"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor1"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor2"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor3"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("time"), TypeFactory.getType(TypeEnum.TIMESTAMP));
      symbolTSDataTypeMap.put(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
      TypeProvider typeProvider = new TypeProvider(symbolTSDataTypeMap);
      LocalExecutionPlanContext localExecutionPlanContext =
          new LocalExecutionPlanContext(
              typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
      operator =
          tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
      ((DataDriverContext) localExecutionPlanContext.getDriverContext())
          .getSourceOperators()
          .forEach(
              sourceOperator ->
                  sourceOperator.initQueryDataSource(
                      new QueryDataSource(seqResources, unSeqResources)));

      int count = 0;
      while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(node.getOutputSymbols().size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(3) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(4) instanceof BinaryColumn);
        count += tsBlock.getPositionCount();
      }
      assertEquals(1320, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {
        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicateAndPushLimitToEachDevice() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DataDriverContext driverContext = new DataDriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode();
      node.setPushDownPredicate(
          new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN,
              new Symbol("sensor3").toSymbolReference(),
              new LongLiteral("1000")));
      node.getAssignments()
          .put(
              new Symbol("sensor3"),
              new ColumnSchema(
                  "sensor3",
                  TypeFactory.getType(TSDataType.INT32),
                  false,
                  TsTableColumnCategory.FIELD));
      node.setPushLimitToEachDevice(true);
      node.setPushDownLimit(10);

      Map<Symbol, Type> symbolTSDataTypeMap = new HashMap<>();
      symbolTSDataTypeMap.put(new Symbol("sensor0"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor1"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor2"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("sensor3"), TypeFactory.getType(TSDataType.INT32));
      symbolTSDataTypeMap.put(new Symbol("time"), TypeFactory.getType(TypeEnum.TIMESTAMP));
      symbolTSDataTypeMap.put(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
      TypeProvider typeProvider = new TypeProvider(symbolTSDataTypeMap);
      LocalExecutionPlanContext localExecutionPlanContext =
          new LocalExecutionPlanContext(
              typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
      operator =
          tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
      ((DataDriverContext) localExecutionPlanContext.getDriverContext())
          .getSourceOperators()
          .forEach(
              sourceOperator ->
                  sourceOperator.initQueryDataSource(
                      new QueryDataSource(seqResources, unSeqResources)));

      int count = 0;
      while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(node.getOutputSymbols().size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(3) instanceof TimeColumn);
        assertTrue(tsBlock.getColumn(4) instanceof RunLengthEncodedColumn);
        count += tsBlock.getPositionCount();
      }
      assertEquals(30, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {
        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }

  private TreeNonAlignedDeviceViewScanNode getTreeNonAlignedDeviceViewScanNode() {
    List<Symbol> outputSymbols =
        Arrays.asList(
            new Symbol("sensor0"),
            new Symbol("sensor1"),
            new Symbol("sensor2"),
            new Symbol("time"),
            new Symbol("tag1"));

    Map<Symbol, ColumnSchema> assignments = new HashMap<>();
    assignments.put(
        new Symbol("tag1"),
        new ColumnSchema(
            "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    assignments.put(
        new Symbol("time"),
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    assignments.put(
        new Symbol("sensor0"),
        new ColumnSchema(
            "sensor0", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("sensor1"),
        new ColumnSchema(
            "sensor1", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("sensor2"),
        new ColumnSchema(
            "sensor2", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));

    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>();
    idAndAttributeIndexMap.put(new Symbol("tag1"), 0);

    List<DeviceEntry> deviceEntries =
        Arrays.asList(
            new NonAlignedAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device0"),
                Collections.emptyList()),
            new NonAlignedAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device1"),
                Collections.emptyList()),
            new NonAlignedAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device1"),
                Collections.emptyList()));
    Expression timePredicate = null;
    Expression pushDownPredicate = null;
    long pushDownLimit = 0;
    long pushDownOffset = 0;
    boolean pushLimitToEachDevice = false;
    boolean containsNonAlignedDevice = true;
    String treeDBName = NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST;

    Map<String, String> measurementColumnNameMap = new HashMap<>();
    return new TreeNonAlignedDeviceViewScanNode(
        new PlanNodeId("1"),
        new QualifiedObjectName(
            NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST.toLowerCase(), "table1"),
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        Ordering.ASC,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        containsNonAlignedDevice,
        treeDBName,
        measurementColumnNameMap);
  }
}
