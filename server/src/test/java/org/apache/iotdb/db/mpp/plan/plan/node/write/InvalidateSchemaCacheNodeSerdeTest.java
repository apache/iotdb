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

package org.apache.iotdb.db.mpp.plan.plan.node.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InvalidateSchemaCacheNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class InvalidateSchemaCacheNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("InvalidateSchemaCacheNode");
    QueryId queryId = new QueryId("query");
    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath("root.sg.d1.s1"));
    pathList.add(new PartialPath("root.sg.d2.*"));
    List<String> storageGroups = new ArrayList<>();
    storageGroups.add("root.sg1");
    storageGroups.add("root.sg2");
    InvalidateSchemaCacheNode invalidateSchemaCacheNode =
        new InvalidateSchemaCacheNode(planNodeId, queryId, pathList, storageGroups);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    invalidateSchemaCacheNode.serialize(byteBuffer);
    byteBuffer.flip();

    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertTrue(deserializedNode instanceof InvalidateSchemaCacheNode);
    Assert.assertEquals(planNodeId, deserializedNode.getPlanNodeId());

    invalidateSchemaCacheNode = (InvalidateSchemaCacheNode) deserializedNode;

    Assert.assertEquals(queryId, invalidateSchemaCacheNode.getQueryId());

    List<PartialPath> deserializedPathList = invalidateSchemaCacheNode.getPathList();
    Assert.assertEquals(pathList.size(), deserializedPathList.size());
    for (int i = 0; i < pathList.size(); i++) {
      Assert.assertEquals(pathList.get(i), deserializedPathList.get(i));
    }

    List<String> deserializedStorageGroups = invalidateSchemaCacheNode.getStorageGroups();
    Assert.assertEquals(storageGroups.size(), deserializedStorageGroups.size());
    for (int i = 0; i < storageGroups.size(); i++) {
      Assert.assertEquals(storageGroups.get(i), deserializedStorageGroups.get(i));
    }
  }
}
