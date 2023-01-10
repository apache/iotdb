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
package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FragmentInstanceSerdeTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SessionInfo sessionInfo =
      new SessionInfo(1, "test", ZoneId.systemDefault().getId());

  @Test
  public void testSerializeAndDeserializeForTree1() throws IllegalPathException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    PlanFragmentId planFragmentId = new PlanFragmentId("test", -1);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            new PlanFragment(planFragmentId, constructPlanNodeTree()),
            planFragmentId.genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.READ,
            config.getQueryTimeoutThreshold(),
            sessionInfo);
    // test FI with StorageExecutor
    TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            ImmutableList.of(dataNodeLocation));
    fragmentInstance.setExecutorAndHost(new StorageExecutor(regionReplicaSet));
    ByteBuffer byteBuffer = fragmentInstance.serializeToByteBuffer();
    FragmentInstance deserializeFragmentInstance = FragmentInstance.deserializeFrom(byteBuffer);
    assertNull(deserializeFragmentInstance.getExecutorType());
    // Because the ExecutorType won't be considered in serialization, we need to set it
    // from original object before comparison.
    deserializeFragmentInstance.setExecutorType(fragmentInstance.getExecutorType());
    assertEquals(deserializeFragmentInstance, fragmentInstance);

    // test FI with QueryExecutor
    fragmentInstance.setExecutorAndHost(new QueryExecutor(dataNodeLocation));
    byteBuffer = fragmentInstance.serializeToByteBuffer();
    deserializeFragmentInstance = FragmentInstance.deserializeFrom(byteBuffer);
    assertNull(deserializeFragmentInstance.getExecutorType());
    deserializeFragmentInstance.setExecutorType(fragmentInstance.getExecutorType());
    assertEquals(deserializeFragmentInstance, fragmentInstance);
  }

  @Test
  public void testSerializeAndDeserializeWithNullFilter() throws IllegalPathException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    PlanFragmentId planFragmentId = new PlanFragmentId("test2", 1);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            new PlanFragment(planFragmentId, constructPlanNodeTree()),
            planFragmentId.genFragmentInstanceId(),
            null,
            QueryType.READ,
            config.getQueryTimeoutThreshold(),
            sessionInfo);
    TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            ImmutableList.of(dataNodeLocation));
    fragmentInstance.setExecutorAndHost(new StorageExecutor(regionReplicaSet));

    ByteBuffer byteBuffer = fragmentInstance.serializeToByteBuffer();
    FragmentInstance deserializeFragmentInstance = FragmentInstance.deserializeFrom(byteBuffer);
    assertNull(deserializeFragmentInstance.getExecutorType());
    deserializeFragmentInstance.setExecutorType(fragmentInstance.getExecutorType());
    assertEquals(deserializeFragmentInstance, fragmentInstance);
  }

  private PlanNode constructPlanNodeTree() throws IllegalPathException {
    // create node
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("OffsetNode"), 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("LimitNode"), 100);

    TimeJoinNode timeJoinNode = new TimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.DESC);
    SeriesScanNode seriesScanNode1 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode1"), new MeasurementPath("root.sg.d1.s2"));
    seriesScanNode1.setScanOrder(Ordering.DESC);
    SeriesScanNode seriesScanNode2 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode2"), new MeasurementPath("root.sg.d2.s1"));
    seriesScanNode2.setScanOrder(Ordering.DESC);
    SeriesScanNode seriesScanNode3 =
        new SeriesScanNode(new PlanNodeId("SeriesScanNode3"), new MeasurementPath("root.sg.d2.s2"));
    seriesScanNode3.setScanOrder(Ordering.DESC);

    // build tree
    timeJoinNode.addChild(seriesScanNode1);
    timeJoinNode.addChild(seriesScanNode2);
    timeJoinNode.addChild(seriesScanNode3);
    limitNode.addChild(timeJoinNode);
    offsetNode.addChild(limitNode);

    return offsetNode;
  }
}
