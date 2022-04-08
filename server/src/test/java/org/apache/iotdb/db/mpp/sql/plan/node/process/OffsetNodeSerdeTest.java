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
package org.apache.iotdb.db.mpp.sql.plan.node.process;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.common.filter.RegexpFilter;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OffsetNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("TestFilterNode"),
            new RegexpFilter(FilterType.EQUAL, new MeasurementPath("s1"), "s1"));

    FillNode fillNode = new FillNode(new PlanNodeId("TestFillNode"), FillPolicy.PREVIOUS);
    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(new PlanNodeId("TestDeviceMergeNode"), OrderBy.TIMESTAMP_ASC);
    List<String> columnNames = new ArrayList<>();
    columnNames.add("s1");
    columnNames.add("s2");
    deviceMergeNode.setColumnNames(columnNames);

    List<PlanNode> planNodes = new ArrayList<>();
    planNodes.add(new ShowDevicesNode(new PlanNodeId("TestShowDevice")));
    List<String> columns = new ArrayList<>();
    columns.add("s1");
    columns.add("s2");
    AggregateNode aggregateNode =
        new AggregateNode(new PlanNodeId("TestAggregateNode"), null, planNodes, columnNames);
    deviceMergeNode.addChild(aggregateNode);
    deviceMergeNode.addChild(new ShowDevicesNode(new PlanNodeId("TestShowDevice")));
    deviceMergeNode.setFilterNullPolicy(FilterNullPolicy.CONTAINS_NULL);
    fillNode.addChild(deviceMergeNode);

    filterNode.addChild(fillNode);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("TestFilterNullNode"), filterNode, FilterNullPolicy.ALL_NULL, null);

    Map<String, String> groupedPathMap = new HashMap<>();
    groupedPathMap.put("s1", "s");
    groupedPathMap.put("s2", "a");
    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            filterNullNode,
            new int[] {1, 3},
            groupedPathMap);

    LimitNode limitNode = new LimitNode(new PlanNodeId("TestLimitNode"), 3, groupByLevelNode);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("TestOffsetNode"), limitNode, 2);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    offsetNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), offsetNode);
  }

  @Test
  public void TestSerializeAndDeserializeForTree1() throws IllegalPathException {
    // create node
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("OffsetNode"), 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("LimitNode"), 100);
    FilterNullNode filterNullNode =
        new FilterNullNode(new PlanNodeId("FilterNullNode"), FilterNullPolicy.CONTAINS_NULL);
    QueryFilter queryFilter = new QueryFilter(FilterType.KW_AND);
    BasicFunctionFilter leftQueryFilter =
        new BasicFunctionFilter(FilterType.GREATERTHAN, new MeasurementPath("root.sg.d1.s2"), "10");
    BasicFunctionFilter rightFilter =
        new BasicFunctionFilter(FilterType.GREATERTHAN, new MeasurementPath("root.sg.d2.s2"), "10");
    queryFilter.addChildOperator(leftQueryFilter);
    queryFilter.addChildOperator(rightFilter);
    FilterNode filterNode = new FilterNode(new PlanNodeId("FilterNode"), queryFilter);

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(
            new PlanNodeId("TimeJoinNode"), OrderBy.TIMESTAMP_DESC, FilterNullPolicy.CONTAINS_NULL);

    SeriesScanNode seriesScanNode1 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode1"), new MeasurementPath("root.sg.d1.s2"));
    seriesScanNode1.setDataRegionReplicaSet(
        new RegionReplicaSet(new DataRegionId(1), new ArrayList<>()));
    seriesScanNode1.setScanOrder(OrderBy.TIMESTAMP_DESC);
    SeriesScanNode seriesScanNode2 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode2"), new MeasurementPath("root.sg.d2.s1"));
    seriesScanNode2.setDataRegionReplicaSet(
        new RegionReplicaSet(new DataRegionId(2), new ArrayList<>()));
    seriesScanNode2.setScanOrder(OrderBy.TIMESTAMP_DESC);
    SeriesScanNode seriesScanNode3 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode3"), new MeasurementPath("root.sg.d2.s2"));
    seriesScanNode3.setDataRegionReplicaSet(
        new RegionReplicaSet(new DataRegionId(3), new ArrayList<>()));
    seriesScanNode3.setScanOrder(OrderBy.TIMESTAMP_DESC);
    seriesScanNode1.setColumnName("root.sg.d1.s2");
    seriesScanNode2.setColumnName("root.sg.d2.s1");
    seriesScanNode3.setColumnName("root.sg.d2.s2");

    // build tree
    timeJoinNode.addChild(seriesScanNode1);
    timeJoinNode.addChild(seriesScanNode2);
    timeJoinNode.addChild(seriesScanNode3);
    filterNode.addChild(timeJoinNode);
    filterNullNode.addChild(filterNode);
    limitNode.addChild(filterNullNode);
    offsetNode.addChild(limitNode);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    offsetNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), offsetNode);
  }
}
