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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class PipeEnrichedNonWritePlanNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    PlanNodeId planNodeId = new PlanNodeId("DeleteTimeSeriesNode");
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.d1.s1"));
    patternTree.appendPathPattern(new PartialPath("root.sg.d2.*"));
    patternTree.constructTree();
    DeleteTimeSeriesNode deleteTimeSeriesNode = new DeleteTimeSeriesNode(planNodeId, patternTree);
    PipeEnrichedNonWritePlanNode pipeEnrichedNonWritePlanNode =
        new PipeEnrichedNonWritePlanNode(deleteTimeSeriesNode);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    pipeEnrichedNonWritePlanNode.serialize(byteBuffer);
    byteBuffer.flip();

    PlanNode deserializedNode = PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(pipeEnrichedNonWritePlanNode, deserializedNode);
  }
}
