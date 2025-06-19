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
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DeviceIteratorScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.TableOperatorGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.Operator.NOT_BLOCKED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NonAlignedTreeDeviceViewScanOperatorTreeTest {

  private static final String NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST =
      "root.NonAlignedTreeDeviceViewScanOperatorTreeTest";
  private static final String tableDbName = "test";
  private static final String tableViewName = "view1";
  private final TableOperatorGenerator tableOperatorGenerator =
      new TableOperatorGenerator(new TestMetadata());
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private final Map<Symbol, ColumnSchema> columnSchemaMap = new HashMap<>();
  private TypeProvider typeProvider;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas,
        deviceIds,
        seqResources,
        unSeqResources,
        NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST);

    columnSchemaMap.put(
        new Symbol("tag1"),
        new ColumnSchema(
            "tag1", TypeFactory.getType(TSDataType.TEXT), false, TsTableColumnCategory.TAG));
    columnSchemaMap.put(
        new Symbol("time"),
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.INT64), false, TsTableColumnCategory.TIME));
    columnSchemaMap.put(
        new Symbol("sensor0"),
        new ColumnSchema(
            "sensor0", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    columnSchemaMap.put(
        new Symbol("sensor1"),
        new ColumnSchema(
            "sensor1", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    columnSchemaMap.put(
        new Symbol("sensor2"),
        new ColumnSchema(
            "sensor2", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    columnSchemaMap.put(
        new Symbol("sensor3"),
        new ColumnSchema(
            "sensor3", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));

    Map<Symbol, Type> symbolTSDataTypeMap = new HashMap<>();
    symbolTSDataTypeMap.put(new Symbol("sensor0"), TypeFactory.getType(TSDataType.INT32));
    symbolTSDataTypeMap.put(new Symbol("sensor1"), TypeFactory.getType(TSDataType.INT32));
    symbolTSDataTypeMap.put(new Symbol("sensor2"), TypeFactory.getType(TSDataType.INT32));
    symbolTSDataTypeMap.put(new Symbol("sensor3"), TypeFactory.getType(TSDataType.INT32));
    symbolTSDataTypeMap.put(new Symbol("time"), TypeFactory.getType(TypeEnum.INT64));
    symbolTSDataTypeMap.put(new Symbol("tag1"), TypeFactory.getType(TSDataType.TEXT));
    typeProvider = new TypeProvider(symbolTSDataTypeMap);

    DataNodeTableCache.getInstance().invalid(tableDbName);

    TsTable tsTable = new TsTable(tableViewName);
    tsTable.addColumnSchema(new TagColumnSchema("id_column", TSDataType.STRING));
    tsTable.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    tsTable.addColumnSchema(new TagColumnSchema("tag1", TSDataType.TEXT));
    tsTable.addColumnSchema(new FieldColumnSchema("sensor0", TSDataType.INT32));
    tsTable.addColumnSchema(new FieldColumnSchema("sensor1", TSDataType.INT32));
    tsTable.addColumnSchema(new FieldColumnSchema("sensor2", TSDataType.INT32));
    tsTable.addColumnSchema(new FieldColumnSchema("sensor3", TSDataType.INT32));
    tsTable.addProp(TsTable.TTL_PROPERTY, Long.MAX_VALUE + "");
    tsTable.addProp(
        TreeViewSchema.TREE_PATH_PATTERN,
        NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".**");
    DataNodeTableCache.getInstance().preUpdateTable(tableDbName, tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable(tableDbName, tableViewName, null);
  }

  @After
  public void tearDown() throws IOException {
    DataNodeTableCache.getInstance().invalid(tableDbName);
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void testScanWithPushDownPredicateAndLimitAndOffset() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownOffset(500);
    node.setPushDownLimit(500);
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor1").toSymbolReference(),
            new LongLiteral("1000")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 500);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicateAndPushLimitToEachDevice() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushLimitToEachDevice(true);
    node.setPushDownLimit(500);
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor1").toSymbolReference(),
            new LongLiteral("1000")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 1320);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithCanPushDownPredicateAndCannotPushDownPredicateAndPushLimitToEachDevice()
      throws Exception {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node =
        getTreeNonAlignedDeviceViewScanNode(
            outputColumnList,
            Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1", "sensor3"));
    node.setPushDownLimit(100);
    node.setPushLimitToEachDevice(true);
    node.setPushDownPredicate(
        new LogicalExpression(
            LogicalExpression.Operator.AND,
            Arrays.asList(
                new LogicalExpression(
                    LogicalExpression.Operator.OR,
                    Arrays.asList(
                        new ComparisonExpression(
                            ComparisonExpression.Operator.GREATER_THAN,
                            new Symbol("sensor0").toSymbolReference(),
                            new LongLiteral("1000")),
                        new ComparisonExpression(
                            ComparisonExpression.Operator.GREATER_THAN,
                            new Symbol("sensor1").toSymbolReference(),
                            new LongLiteral("1000")))),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor2").toSymbolReference(),
                    new LongLiteral("1000")),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor3").toSymbolReference(),
                    new LongLiteral("1000")))));
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 300);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanSingleFieldColumnWithPushLimitToEachDevice1() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0");
    TreeNonAlignedDeviceViewScanNode node =
        getTreeNonAlignedDeviceViewScanNode(outputColumnList, Arrays.asList("sensor0"));
    node.setPushLimitToEachDevice(false);
    node.setPushDownLimit(1);
    node.setPushDownOffset(1);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 1);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicateAndPushLimitToEachDevice1() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node =
        getTreeNonAlignedDeviceViewScanNode(
            outputColumnList,
            Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1", "sensor3"));
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor3").toSymbolReference(),
            new LongLiteral("1000")));
    node.setPushLimitToEachDevice(true);
    node.setPushDownLimit(10);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 30);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithCannotPushDownPredicateAndLimitAndOffset2() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownOffset(500);
    node.setPushDownLimit(500);
    node.setPushDownPredicate(
        new LogicalExpression(
            LogicalExpression.Operator.OR,
            Arrays.asList(
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor1").toSymbolReference(),
                    new LongLiteral("1000")),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor2").toSymbolReference(),
                    new LongLiteral("1000")))));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof LimitOperator);
    try {
      checkResult(operator, outputColumnList, 500);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithLimit() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownLimit(500);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof LimitOperator);
    try {
      checkResult(operator, outputColumnList, 500);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushLimitToEachDevice() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownLimit(500);
    node.setPushLimitToEachDevice(true);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 1500);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithOneFieldColumn() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownLimit(500);
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor0").toSymbolReference(),
            new LongLiteral("1000")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 500);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithOneFieldColumnAndPushLimitToEachDevice() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownLimit(500);
    node.setPushLimitToEachDevice(true);
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor0").toSymbolReference(),
            new LongLiteral("1000")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 1320);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithOffset() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownOffset(1200);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof OffsetOperator);
    try {
      checkResult(operator, outputColumnList, 300);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicateAndOffset1() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownOffset(1200);
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor1").toSymbolReference(),
            new LongLiteral("0")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 300);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicateAndOffset2() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node = getTreeNonAlignedDeviceViewScanNode(outputColumnList);
    node.setPushDownOffset(1200);
    node.setPushDownPredicate(
        new LogicalExpression(
            LogicalExpression.Operator.AND,
            Arrays.asList(
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor1").toSymbolReference(),
                    new LongLiteral("0")),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor2").toSymbolReference(),
                    new LongLiteral("0")))));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    assertTrue(operator instanceof DeviceIteratorScanOperator);
    try {
      checkResult(operator, outputColumnList, 300);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testScanWithPushDownPredicate() throws Exception {
    List<String> outputColumnList = Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1");
    TreeNonAlignedDeviceViewScanNode node =
        getTreeNonAlignedDeviceViewScanNode(
            outputColumnList,
            Arrays.asList("sensor0", "sensor1", "sensor2", "time", "tag1", "sensor3"));
    node.setPushDownPredicate(
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN,
            new Symbol("sensor3").toSymbolReference(),
            new LongLiteral("1000")));
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    Operator operator = getOperator(node, instanceNotificationExecutor);
    try {
      checkResult(operator, outputColumnList, 1320);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      operator.close();
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testUtils() {
    Expression expression =
        new LogicalExpression(
            LogicalExpression.Operator.AND,
            Arrays.asList(
                new LogicalExpression(
                    LogicalExpression.Operator.OR,
                    Arrays.asList(
                        new ComparisonExpression(
                            ComparisonExpression.Operator.GREATER_THAN,
                            new Symbol("sensor0").toSymbolReference(),
                            new LongLiteral("1000")),
                        new ComparisonExpression(
                            ComparisonExpression.Operator.GREATER_THAN,
                            new Symbol("sensor1").toSymbolReference(),
                            new LongLiteral("1000")))),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor2").toSymbolReference(),
                    new LongLiteral("1000")),
                new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    new Symbol("sensor3").toSymbolReference(),
                    new LongLiteral("1000"))));
    List<Expression> conjuncts = IrUtils.extractConjuncts(expression);
    assertEquals(3, conjuncts.size());
    Set<Symbol> symbols = SymbolsExtractor.extractUnique(expression);
    assertEquals(4, symbols.size());
    assertTrue(symbols.contains(new Symbol("sensor0")));
    assertTrue(symbols.contains(new Symbol("sensor1")));
    assertTrue(symbols.contains(new Symbol("sensor2")));
    assertTrue(symbols.contains(new Symbol("sensor3")));
  }

  private Operator getOperator(
      TreeNonAlignedDeviceViewScanNode node, ExecutorService instanceNotificationExecutor) {
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

    LocalExecutionPlanContext localExecutionPlanContext =
        new LocalExecutionPlanContext(
            typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
    Operator operator =
        tableOperatorGenerator.visitTreeNonAlignedDeviceViewScan(node, localExecutionPlanContext);
    ((DataDriverContext) localExecutionPlanContext.getDriverContext())
        .getSourceOperators()
        .forEach(
            sourceOperator ->
                sourceOperator.initQueryDataSource(
                    new QueryDataSource(seqResources, unSeqResources)));
    return operator;
  }

  private void checkResult(Operator operator, List<String> outputColumnList, int expectedCount)
      throws Exception {
    int count = 0;
    while (operator.isBlocked().get() != NOT_BLOCKED && operator.hasNext()) {
      TsBlock tsBlock = operator.next();
      if (tsBlock == null) {
        continue;
      }
      assertEquals(outputColumnList.size(), tsBlock.getValueColumnCount());
      for (int i = 0; i < outputColumnList.size(); i++) {
        Symbol symbol = new Symbol(outputColumnList.get(i));
        assertEquals(
            columnSchemaMap.get(symbol).getType(),
            TypeFactory.getType(tsBlock.getColumn(i).getDataType()));
      }
      count += tsBlock.getPositionCount();
    }
    assertEquals(expectedCount, count);
  }

  private TreeNonAlignedDeviceViewScanNode getTreeNonAlignedDeviceViewScanNode(
      List<String> outputColumns) {
    return getTreeNonAlignedDeviceViewScanNode(outputColumns, outputColumns);
  }

  private TreeNonAlignedDeviceViewScanNode getTreeNonAlignedDeviceViewScanNode(
      List<String> outputColumns, List<String> assignmentColumns) {
    List<Symbol> outputSymbols =
        outputColumns.stream().map(Symbol::new).collect(Collectors.toList());

    Map<Symbol, ColumnSchema> assignments = new HashMap<>();
    for (String assignmentColumn : assignmentColumns) {
      Symbol symbol = new Symbol(assignmentColumn);
      assignments.put(symbol, columnSchemaMap.get(symbol));
    }

    Map<Symbol, Integer> tagAndAttributeIndexMap = new HashMap<>();
    tagAndAttributeIndexMap.put(new Symbol("tag1"), 0);

    List<DeviceEntry> deviceEntries =
        Arrays.asList(
            new NonAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device0"),
                new Binary[0]),
            new NonAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device1"),
                new Binary[0]),
            new NonAlignedDeviceEntry(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    NON_ALIGNED_TREE_DEVICE_VIEW_SCAN_OPERATOR_TREE_TEST + ".device1"),
                new Binary[0]));
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
        new QualifiedObjectName(tableDbName, tableViewName),
        outputSymbols,
        assignments,
        deviceEntries,
        tagAndAttributeIndexMap,
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
