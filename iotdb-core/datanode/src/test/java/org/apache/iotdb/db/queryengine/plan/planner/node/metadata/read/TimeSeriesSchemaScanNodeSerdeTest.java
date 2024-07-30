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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class TimeSeriesSchemaScanNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("offset"), 10);
    LimitNode limitNode = new LimitNode(new PlanNodeId("limit"), 10);
    SchemaQueryMergeNode schemaMergeNode = new SchemaQueryMergeNode(new PlanNodeId("schemaMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    TimeSeriesSchemaScanNode timeSeriesSchemaScanNode =
        new TimeSeriesSchemaScanNode(
            new PlanNodeId("timeSeriesSchemaScan"),
            new PartialPath("root.sg.device0.sensor"),
            null,
            10,
            0,
            false,
            false,
            Collections.emptyMap(),
            SchemaConstant.ALL_MATCH_SCOPE);
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            new PlanNodeId("sink"),
            Collections.singletonList(timeSeriesSchemaScanNode),
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
    offsetNode.addChild(exchangeNode);
    limitNode.addChild(offsetNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    limitNode.serialize(byteBuffer);
    byteBuffer.flip();
    LimitNode limitNode1 = (LimitNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(limitNode, limitNode1);
  }
}
