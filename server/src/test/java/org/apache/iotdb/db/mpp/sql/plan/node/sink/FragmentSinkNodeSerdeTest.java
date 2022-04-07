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
package org.apache.iotdb.db.mpp.sql.plan.node.sink;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.junit.Test;

public class FragmentSinkNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() {
    FragmentSinkNode fragmentSinkNode = new FragmentSinkNode(new PlanNodeId("TestFragmentSinkNode"));
    fragmentSinkNode.addChild(new ShowDevicesNode(new PlanNodeId("TestShowDevicesNode")));
    fragmentSinkNode.setDownStream(new Endpoint("127.0.0.1", 6666), new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"), new PlanNodeId("test"));

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentSinkNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), fragmentSinkNode);
  }
}
