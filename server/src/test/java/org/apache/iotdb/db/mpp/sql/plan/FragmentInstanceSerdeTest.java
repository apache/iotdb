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
package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.sql.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class FragmentInstanceSerdeTest {

  @Test
  public void TestSerializeAndDeserializeForTree1() throws IllegalPathException, IOException {
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            new PlanFragment(new PlanFragmentId("test", -1), constructPlanNodeTree()),
            -1,
            new GroupByFilter(1, 2, 3, 4),
            QueryType.READ);
    RegionReplicaSet regionReplicaSet =
        new RegionReplicaSet(new DataRegionId(1), new ArrayList<>());
    fragmentInstance.setRegionReplicaSet(regionReplicaSet);
    fragmentInstance.setHostEndpoint(new Endpoint("127.0.0.1", 6666));

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();
    FragmentInstance deserializeFragmentInstance = FragmentInstance.deserializeFrom(byteBuffer);
    assertEquals(deserializeFragmentInstance, fragmentInstance);
  }

  @Test
  public void TestSerializeAndDeserializeWithNullFilter() throws IllegalPathException, IOException {
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            new PlanFragment(new PlanFragmentId("test2", 1), constructPlanNodeTree()),
            -1,
            null,
            QueryType.READ);
    RegionReplicaSet regionReplicaSet =
        new RegionReplicaSet(new DataRegionId(1), new ArrayList<>());
    fragmentInstance.setRegionReplicaSet(regionReplicaSet);
    fragmentInstance.setHostEndpoint(new Endpoint("127.0.0.2", 6667));

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();
    FragmentInstance deserializeFragmentInstance = FragmentInstance.deserializeFrom(byteBuffer);
    assertEquals(deserializeFragmentInstance, fragmentInstance);
  }

  private PlanNode constructPlanNodeTree() throws IllegalPathException {
    // create node
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("OffsetNode"), 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("LimitNode"), 100);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("TestFilterNullNode"), null, FilterNullPolicy.ALL_NULL, null);
    IExpression expression =
        BinaryExpression.and(
            new SingleSeriesExpression(
                new MeasurementPath("root.sg.d1.s2"),
                new Gt<Integer>(
                    10, org.apache.iotdb.tsfile.read.filter.factory.FilterType.VALUE_FILTER)),
            new SingleSeriesExpression(
                new MeasurementPath("root.sg.d2.s2"),
                new Gt<Integer>(
                    10, org.apache.iotdb.tsfile.read.filter.factory.FilterType.VALUE_FILTER)));

    FilterNode filterNode = new FilterNode(new PlanNodeId("FilterNode"), expression);

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("TimeJoinNode"), OrderBy.TIMESTAMP_DESC);
    timeJoinNode.setWithoutPolicy(FilterNullPolicy.CONTAINS_NULL);
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

    // build tree
    timeJoinNode.addChild(seriesScanNode1);
    timeJoinNode.addChild(seriesScanNode2);
    timeJoinNode.addChild(seriesScanNode3);
    filterNode.addChild(timeJoinNode);
    filterNullNode.addChild(filterNode);
    limitNode.addChild(filterNullNode);
    offsetNode.addChild(limitNode);

    return offsetNode;
  }
}
