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

package org.apache.iotdb.db.queryengine.plan.planner.node.write;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteDataNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("DeleteDataNode");
    long startTime = 1;
    long endTime = 10;
    List<MeasurementPath> pathList = new ArrayList<>();
    pathList.add(new MeasurementPath("root.sg.d1.s1"));
    pathList.add(new MeasurementPath("root.sg.d2.*"));
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(planNodeId, pathList, startTime, endTime, MinimumProgressIndex.INSTANCE);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    deleteDataNode.serialize(byteBuffer);
    byteBuffer.flip();

    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertTrue(deserializedNode instanceof DeleteDataNode);
    Assert.assertEquals(planNodeId, deserializedNode.getPlanNodeId());

    deleteDataNode = (DeleteDataNode) deserializedNode;

    Assert.assertEquals(startTime, deleteDataNode.getDeleteStartTime());
    Assert.assertEquals(endTime, deleteDataNode.getDeleteEndTime());

    List<MeasurementPath> deserializedPathList = deleteDataNode.getPathList();
    Assert.assertEquals(pathList.size(), deserializedPathList.size());
    for (int i = 0; i < pathList.size(); i++) {
      Assert.assertEquals(pathList.get(i), deserializedPathList.get(i));
    }
  }
}
