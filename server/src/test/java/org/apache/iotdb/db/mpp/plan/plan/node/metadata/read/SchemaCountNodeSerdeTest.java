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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class SchemaCountNodeSerdeTest {

  @Test
  public void testDevicesCountSerializeAndDeserialize() throws IllegalPathException {
    CountSchemaMergeNode countMergeNode = new CountSchemaMergeNode(new PlanNodeId("countMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    DevicesCountNode devicesCountNode =
        new DevicesCountNode(
            new PlanNodeId("devicesCount"), new PartialPath("root.sg.device0"), true);
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            new PlanNodeId("sink"),
            Collections.singletonList(devicesCountNode),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("127.0.0.1", 6667),
                    new FragmentInstanceId(new PlanFragmentId("q", 1), "ds").toThrift(),
                    new PlanNodeId("test").getId())));
    countMergeNode.addChild(sinkNode);
    exchangeNode.addChild(sinkNode);
    exchangeNode.setOutputColumnNames(exchangeNode.getChild().getOutputColumnNames());
    exchangeNode.setUpstream(
        new TEndPoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    exchangeNode.serialize(byteBuffer);
    byteBuffer.flip();
    ExchangeNode exchangeNode1 = (ExchangeNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(exchangeNode, exchangeNode1);
  }

  @Test
  public void testTimeSeriesCountSerializeAndDeserialize() throws IllegalPathException {
    CountSchemaMergeNode countMergeNode = new CountSchemaMergeNode(new PlanNodeId("countMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    LevelTimeSeriesCountNode levelTimeSeriesCountNode =
        new LevelTimeSeriesCountNode(
            new PlanNodeId("timeseriesCount"),
            new PartialPath("root.sg.device0"),
            true,
            10,
            null,
            null,
            false);
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            new PlanNodeId("sink"),
            Collections.singletonList(levelTimeSeriesCountNode),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("127.0.0.1", 6667),
                    new FragmentInstanceId(new PlanFragmentId("q", 1), "ds").toThrift(),
                    new PlanNodeId("test").getId())));
    countMergeNode.addChild(exchangeNode);
    exchangeNode.addChild(sinkNode);
    exchangeNode.setOutputColumnNames(exchangeNode.getChild().getOutputColumnNames());
    exchangeNode.setUpstream(
        new TEndPoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    exchangeNode.serialize(byteBuffer);
    byteBuffer.flip();
    ExchangeNode exchangeNode1 = (ExchangeNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(exchangeNode, exchangeNode1);
  }
}
