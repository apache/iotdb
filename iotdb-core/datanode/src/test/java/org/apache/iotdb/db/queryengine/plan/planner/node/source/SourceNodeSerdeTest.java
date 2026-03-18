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

package org.apache.iotdb.db.queryengine.plan.planner.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowDiskUsageNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableDiskUsageInformationSchemaTableScanNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SourceNodeSerdeTest {
  @Test
  public void testLastQueryScanNode() throws IllegalPathException {
    LastQueryScanNode node =
        new LastQueryScanNode(
            new PlanNodeId("test"),
            new PartialPath("root.test.d1"),
            true,
            Arrays.asList(0, 1),
            null,
            false,
            null,
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s0", TSDataType.BOOLEAN)));
    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);

    node =
        new LastQueryScanNode(
            new PlanNodeId("test"),
            new PartialPath("root.test.d1"),
            false,
            Arrays.asList(0, 1),
            null,
            false,
            null,
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s0", TSDataType.BOOLEAN)));
    byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
  }

  @Test
  public void testShowDiskUsageNode() throws IllegalPathException {
    ShowDiskUsageNode node =
        new ShowDiskUsageNode(new PlanNodeId("test"), null, new PartialPath("root.test.d1"));

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
    node = new ShowDiskUsageNode(new PlanNodeId("test"), null, new PartialPath("root.test.d1"));
    byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
  }

  @Test
  public void testShowQueriesNode() throws IllegalPathException {
    ShowQueriesNode node = new ShowQueriesNode(new PlanNodeId("test"), null, "root");

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
    node = new ShowQueriesNode(new PlanNodeId("test"), null, "root");
    byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
  }

  @Test
  public void testTableDiskUsageInformationTableScanNode() throws IllegalPathException {
    List<Symbol> symbols = Arrays.asList(new Symbol("database"), new Symbol("size_in_bytes"));
    Map<Symbol, ColumnSchema> assignments = new HashMap<>();
    assignments.put(
        new Symbol("database"),
        new ColumnSchema("database", StringType.getInstance(), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("table_name"),
        new ColumnSchema(
            "table_name", StringType.getInstance(), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("datanode_id"),
        new ColumnSchema("datanode_id", IntType.getInstance(), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("region_id"),
        new ColumnSchema("region_id", IntType.getInstance(), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("time_partition"),
        new ColumnSchema(
            "time_partition", LongType.getInstance(), false, TsTableColumnCategory.FIELD));
    assignments.put(
        new Symbol("size_in_bytes"),
        new ColumnSchema(
            "size_in_bytes", LongType.getInstance(), false, TsTableColumnCategory.FIELD));
    TableDiskUsageInformationSchemaTableScanNode node =
        new TableDiskUsageInformationSchemaTableScanNode(
            new PlanNodeId("test"),
            new QualifiedObjectName("test", "table1"),
            symbols,
            assignments,
            null,
            1,
            1,
            new TRegionReplicaSet(),
            Arrays.asList(1));
    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(
        ((TableDiskUsageInformationSchemaTableScanNode)
                PlanNodeDeserializeHelper.deserialize(byteBuffer))
            .getRegions(),
        node.getRegions());
  }
}
