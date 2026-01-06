/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.CteScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CteScanOperatorTest {
  private OperatorContext operatorContext;
  private PlanNodeId planNodeId;
  private CteDataStore cteDataStore;
  private QueryId queryId;
  private CteScanOperator cteScanOperator;

  @Before
  public void setUp() {
    // Set up mock objects
    operatorContext = mock(OperatorContext.class);
    planNodeId = new PlanNodeId("test-plan-node");

    // Create a simple table schema for testing
    TableSchema tableSchema = createTestTableSchema();

    // Create column index mapping
    List<Integer> columnIndex2TsBlockColumnIndexList = Arrays.asList(0, 1, 2);

    // Initialize CteDataStore
    cteDataStore = new CteDataStore(tableSchema, columnIndex2TsBlockColumnIndexList);

    // Add test data to the data store
    List<TsBlock> testData = createTestTsBlocks();
    for (TsBlock tsBlock : testData) {
      cteDataStore.addTsBlock(tsBlock);
    }

    queryId = new QueryId("1");
  }

  @After
  public void tearDown() throws Exception {
    if (cteScanOperator != null) {
      cteScanOperator.close();
    }
  }

  @Test
  public void testConstructor() throws Exception {
    cteScanOperator = new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);
    assertEquals(1, cteDataStore.getCount());
    cteScanOperator.close();
  }

  @Test
  public void testEmptyDataStore() throws Exception {
    // Create empty data store
    TableSchema tableSchema = createTestTableSchema();
    CteDataStore emptyDataStore = new CteDataStore(tableSchema, Arrays.asList(0, 1, 2));

    cteScanOperator = new CteScanOperator(operatorContext, planNodeId, emptyDataStore, queryId);
    // Should not have data
    assertFalse(cteScanOperator.hasNext());

    cteScanOperator.close();
  }

  @Test
  public void testNextWithData() throws Exception {
    cteScanOperator = new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);
    // Should have data
    assertTrue(cteScanOperator.hasNext());
    TsBlock firstBlock = cteScanOperator.next();
    assertNotNull(firstBlock);
    assertEquals(2, firstBlock.getValueColumnCount());
    assertEquals(3, firstBlock.getPositionCount());

    // Should have data
    assertTrue(cteScanOperator.hasNext());
    TsBlock secondBlock = cteScanOperator.next();
    assertNotNull(secondBlock);
    assertEquals(2, secondBlock.getValueColumnCount());
    assertEquals(2, secondBlock.getPositionCount());

    // should return null
    TsBlock thirdBlock = cteScanOperator.next();
    assertNull(thirdBlock);

    cteScanOperator.close();
  }

  @Test
  public void testIsFinished() throws Exception {
    cteScanOperator = new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);

    // Initially not finished
    assertFalse(cteScanOperator.isFinished());
    // Consume all data
    while (cteScanOperator.hasNext()) {
      cteScanOperator.next();
    }
    // Now should be finished
    assertTrue(cteScanOperator.isFinished());

    cteScanOperator.close();
  }

  @Test
  public void testMemory() throws Exception {
    cteScanOperator = new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);

    long maxReturnSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    assertEquals(maxReturnSize, cteScanOperator.calculateMaxPeekMemory());
    assertEquals(maxReturnSize, cteScanOperator.calculateMaxPeekMemory());
    assertEquals(0L, cteScanOperator.calculateRetainedSizeAfterCallingNext());

    cteScanOperator.close();
  }

  @Test
  public void testMultipleCteScanOperators() throws Exception {
    // Test reference counting with multiple operators
    CteScanOperator operator1 =
        new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);
    assertEquals(1, cteDataStore.getCount());
    CteScanOperator operator2 =
        new CteScanOperator(operatorContext, planNodeId, cteDataStore, queryId);
    assertEquals(2, cteDataStore.getCount());

    assertEquals(896, cteDataStore.ramBytesUsed());

    // Both operators should be able to read data
    assertTrue(operator1.hasNext());
    assertTrue(operator2.hasNext());

    // Clean up
    operator1.close();
    operator2.close();
  }

  private TableSchema createTestTableSchema() {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(
        new ColumnSchema("time", TimestampType.TIMESTAMP, false, TsTableColumnCategory.TIME));
    columnSchemas.add(
        new ColumnSchema("name", StringType.STRING, false, TsTableColumnCategory.FIELD));
    columnSchemas.add(
        new ColumnSchema("value", DoubleType.DOUBLE, false, TsTableColumnCategory.FIELD));

    return new TableSchema("test_table", columnSchemas);
  }

  private List<TsBlock> createTestTsBlocks() {
    List<TsBlock> blocks = new ArrayList<>();

    // Create first TsBlock
    blocks.add(
        createTsBlock(
            new long[] {1000L, 2000L, 3000L},
            new String[] {"Alice", "Bob", "Charlie"},
            new double[] {10.5, 20.3, 30.7}));

    // Create second TsBlock
    blocks.add(
        createTsBlock(
            new long[] {4000L, 5000L}, new String[] {"David", "Eve"}, new double[] {40.2, 50.8}));

    return blocks;
  }

  private TsBlock createTsBlock(long[] times, String[] names, double[] values) {
    TsBlockBuilder builder =
        new TsBlockBuilder(ImmutableList.of(TSDataType.STRING, TSDataType.DOUBLE));

    // Time column
    TimeColumnBuilder timeColumn = builder.getTimeColumnBuilder();
    for (long time : times) {
      timeColumn.writeLong(time);
    }

    // Name column
    BinaryColumnBuilder nameColumn = (BinaryColumnBuilder) builder.getColumnBuilder(0);
    for (String name : names) {
      nameColumn.writeBinary(new Binary(name, StandardCharsets.UTF_8));
    }

    // Value column
    DoubleColumnBuilder valueColumn = (DoubleColumnBuilder) builder.getColumnBuilder(1);
    for (double value : values) {
      valueColumn.writeDouble(value);
    }

    builder.declarePositions(times.length);
    return builder.build();
  }
}
