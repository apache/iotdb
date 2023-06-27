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

package org.apache.iotdb.db.queryengine.plan.plan.node.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class IdentitySinkNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    DownStreamChannelLocation downStreamChannelLocation =
        new DownStreamChannelLocation(
            new TEndPoint("test", 1), new TFragmentInstanceId("test", 1, "test"), "test");
    IdentitySinkNode identitySinkNode1 =
        new IdentitySinkNode(
            new PlanNodeId("testIdentitySinkNode"),
            Collections.singletonList(downStreamChannelLocation));
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    identitySinkNode1.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(identitySinkNode1, PlanNodeDeserializeHelper.deserialize(byteBuffer));

    IdentitySinkNode identitySinkNode2 =
        new IdentitySinkNode(new PlanNodeId("testIdentitySinkNode"), Collections.emptyList());
    ByteBuffer byteBuffer2 = ByteBuffer.allocate(1024);
    identitySinkNode2.serialize(byteBuffer2);
    byteBuffer2.flip();
    assertEquals(identitySinkNode2, PlanNodeDeserializeHelper.deserialize(byteBuffer2));
  }
}
