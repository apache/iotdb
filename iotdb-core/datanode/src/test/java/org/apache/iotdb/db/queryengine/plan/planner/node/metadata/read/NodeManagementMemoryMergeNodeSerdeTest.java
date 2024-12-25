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

package org.apache.iotdb.db.queryengine.plan.planner.node.metadata.read;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NodeManagementMemoryMergeNodeSerdeTest {

  @Test
  public void testNodePathsSerializeAndDeserialize() throws IllegalPathException {
    NodeManagementMemoryMergeNode memorySourceNode = createNodeManagementMemoryMergeNode();
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    memorySourceNode.serialize(byteBuffer);
    byteBuffer.flip();
    NodeManagementMemoryMergeNode memorySourceNode1 =
        (NodeManagementMemoryMergeNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(memorySourceNode, memorySourceNode1);
  }

  @Test
  public void testNodeConvertSerializeAndDeserialize() throws IllegalPathException {
    NodeManagementMemoryMergeNode memorySourceNode = createNodeManagementMemoryMergeNode();
    NodePathsConvertNode node = new NodePathsConvertNode(new PlanNodeId("nodePathConvert"));
    node.addChild(memorySourceNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    NodePathsConvertNode node1 =
        (NodePathsConvertNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(node, node1);
  }

  @Test
  public void testNodeCountSerializeAndDeserialize() throws IllegalPathException {
    NodeManagementMemoryMergeNode memorySourceNode = createNodeManagementMemoryMergeNode();
    NodePathsCountNode node = new NodePathsCountNode(new PlanNodeId("nodePathCount"));
    node.addChild(memorySourceNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    NodePathsCountNode node1 =
        (NodePathsCountNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(node, node1);
  }

  private NodeManagementMemoryMergeNode createNodeManagementMemoryMergeNode()
      throws IllegalPathException {
    Set<TSchemaNode> data = new HashSet<>();
    data.add(new TSchemaNode("root.ln", MNodeType.STORAGE_GROUP.getNodeType()));
    data.add(new TSchemaNode("root.abc", MNodeType.STORAGE_GROUP.getNodeType()));
    NodeManagementMemoryMergeNode memorySourceNode =
        new NodeManagementMemoryMergeNode(new PlanNodeId("nodeManagementMerge"), data);
    SchemaQueryMergeNode schemaMergeNode = new SchemaQueryMergeNode(new PlanNodeId("schemaMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    NodePathsSchemaScanNode childPathsSchemaScanNode =
        new NodePathsSchemaScanNode(
            new PlanNodeId("NodePathsScan"),
            new PartialPath("root.ln"),
            -1,
            SchemaConstant.ALL_MATCH_SCOPE);
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            new PlanNodeId("sink"),
            Collections.singletonList(childPathsSchemaScanNode),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("127.0.0.1", 6667),
                    new FragmentInstanceId(new PlanFragmentId("q", 1), "ds").toThrift(),
                    new PlanNodeId("test").getId())));
    schemaMergeNode.addChild(exchangeNode);
    exchangeNode.addChild(sinkNode);
    exchangeNode.setOutputColumnNames(exchangeNode.getChild().getOutputColumnNames());
    exchangeNode.setUpstream(
        new TEndPoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    memorySourceNode.addChild(exchangeNode);
    return memorySourceNode;
  }
}
