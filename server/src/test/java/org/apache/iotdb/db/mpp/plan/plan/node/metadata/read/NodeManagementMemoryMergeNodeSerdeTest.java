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

package org.apache.iotdb.db.mpp.plan.plan.node.metadata.read;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.NodeManagementType;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class NodeManagementMemoryMergeNodeSerdeTest {

  @Test
  public void testChildPathsSerializeAndDeserialize() throws IllegalPathException {
    Set<String> data = new HashSet<>();
    data.add("root.ln");
    data.add("root.abc");
    NodeManagementMemoryMergeNode memorySourceNode =
        new NodeManagementMemoryMergeNode(
            new PlanNodeId("nodeManagementMerge"), data, NodeManagementType.CHILD_PATHS);
    SchemaQueryMergeNode schemaMergeNode = new SchemaQueryMergeNode(new PlanNodeId("schemaMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    NodePathsSchemaScanNode childPathsSchemaScanNode =
        new NodePathsSchemaScanNode(
            new PlanNodeId("childPathsScan"), new PartialPath("root.ln"), -1);
    FragmentSinkNode fragmentSinkNode = new FragmentSinkNode(new PlanNodeId("fragmentSink"));
    fragmentSinkNode.addChild(childPathsSchemaScanNode);
    fragmentSinkNode.setDownStream(
        new TEndPoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    exchangeNode.addChild(schemaMergeNode);
    exchangeNode.setRemoteSourceNode(fragmentSinkNode);
    exchangeNode.setUpstream(
        new TEndPoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    memorySourceNode.addChild(exchangeNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    memorySourceNode.serialize(byteBuffer);
    byteBuffer.flip();
    NodeManagementMemoryMergeNode memorySourceNode1 =
        (NodeManagementMemoryMergeNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(memorySourceNode, memorySourceNode1);
  }

  @Test
  public void testChildNodesSerializeAndDeserialize() throws IllegalPathException {
    Set<String> data = new HashSet<>();
    data.add("ln");
    data.add("abc");
    // TODO
    //    NodeManagementMemoryMergeNode memorySourceNode =
    //        new NodeManagementMemoryMergeNode(
    //            new PlanNodeId("nodeManagementMerge"), data, NodeManagementType.CHILD_NODES);
    //    SchemaQueryMergeNode schemaMergeNode = new SchemaQueryMergeNode(new
    // PlanNodeId("schemaMerge"));
    //    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    //    ChildNodesSchemaScanNode childNodesSchemaScanNode =
    //        new ChildNodesSchemaScanNode(new PlanNodeId("childNodesScan"), new
    // PartialPath("root.ln"));
    //    FragmentSinkNode fragmentSinkNode = new FragmentSinkNode(new PlanNodeId("fragmentSink"));
    //    fragmentSinkNode.addChild(childNodesSchemaScanNode);
    //    fragmentSinkNode.setDownStream(
    //        new TEndPoint("127.0.0.1", 6667),
    //        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
    //        new PlanNodeId("test"));
    //    exchangeNode.addChild(schemaMergeNode);
    //    exchangeNode.setRemoteSourceNode(fragmentSinkNode);
    //    exchangeNode.setUpstream(
    //        new TEndPoint("127.0.0.1", 6667),
    //        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
    //        new PlanNodeId("test"));
    //    memorySourceNode.addChild(exchangeNode);
    //    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    //    memorySourceNode.serialize(byteBuffer);
    //    byteBuffer.flip();
    //    NodeManagementMemoryMergeNode memorySourceNode1 =
    //        (NodeManagementMemoryMergeNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    //    Assert.assertEquals(memorySourceNode, memorySourceNode1);
  }
}
