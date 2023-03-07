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
package org.apache.iotdb.db.mpp.plan.plan.node.process;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ExchangeNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    TimeJoinNode timeJoinNode = new TimeJoinNode(new PlanNodeId("TestTimeJoinNode"), Ordering.ASC);

    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("TestExchangeNode"));
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            new PlanNodeId("sink"),
            Collections.singletonList(timeJoinNode),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("127.0.0.1", 6667),
                    new FragmentInstanceId(new PlanFragmentId("q", 1), "ds").toThrift(),
                    new PlanNodeId("test").getId())));

    exchangeNode.addChild(sinkNode);
    exchangeNode.setOutputColumnNames(exchangeNode.getChild().getOutputColumnNames());
    exchangeNode.setUpstream(
        new TEndPoint("127.0.0.1", 6666),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));

    ByteBuffer byteBuffer = ByteBuffer.allocate(10240);
    exchangeNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), exchangeNode);
  }
}
