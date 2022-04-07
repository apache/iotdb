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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExchangeNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() {
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

    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("TestExchangeNode"));
    FragmentSinkNode fragmentSinkNode =
        new FragmentSinkNode(new PlanNodeId("TestFragmentSinkNode"));
    fragmentSinkNode.setDownStream(
        new Endpoint("127.0.0.1", 6666),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    fragmentSinkNode.addChild(new ShowDevicesNode(new PlanNodeId("ss")));
    exchangeNode.setRemoteSourceNode(fragmentSinkNode);
    exchangeNode.addChild(deviceMergeNode);
    exchangeNode.setUpstream(
        new Endpoint("127.0.0.1", 6666),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));

    ByteBuffer byteBuffer = ByteBuffer.allocate(10240);
    exchangeNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), exchangeNode);
  }
}
