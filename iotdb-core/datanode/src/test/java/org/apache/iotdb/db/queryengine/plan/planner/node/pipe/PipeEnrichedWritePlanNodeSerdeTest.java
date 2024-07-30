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

package org.apache.iotdb.db.queryengine.plan.planner.node.pipe;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PipeEnrichedWritePlanNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("InternalBatchActivateTemplateNode");
    Map<PartialPath, Pair<Integer, Integer>> map = new HashMap<>();
    map.put(new PartialPath("root.db.d1.s1"), new Pair<>(1, 2));
    InternalBatchActivateTemplateNode internalBatchActivateTemplateNode =
        new InternalBatchActivateTemplateNode(planNodeId, map);
    PipeEnrichedWritePlanNode pipeEnrichedWritePlanNode =
        new PipeEnrichedWritePlanNode(internalBatchActivateTemplateNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    pipeEnrichedWritePlanNode.serialize(byteBuffer);
    byteBuffer.flip();
    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(pipeEnrichedWritePlanNode, deserializedNode);
  }
}
