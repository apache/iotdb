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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class DeviceViewNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    TimeJoinNode timeJoinNode1 = new TimeJoinNode(new PlanNodeId("TestTimeJoinNode"), Ordering.ASC);
    TimeJoinNode timeJoinNode2 = new TimeJoinNode(new PlanNodeId("TestTimeJoinNode"), Ordering.ASC);
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("TestDeviceMergeNode"),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, Ordering.DESC))),
            Arrays.asList("s1", "s2"),
            new HashMap<>());
    deviceViewNode.addChildDeviceNode("root.sg.d1", timeJoinNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", timeJoinNode2);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    deviceViewNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), deviceViewNode);
  }
}
