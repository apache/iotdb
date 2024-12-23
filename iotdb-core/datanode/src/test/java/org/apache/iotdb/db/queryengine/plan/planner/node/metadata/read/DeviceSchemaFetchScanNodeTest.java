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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DeviceSchemaFetchScanNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DeviceSchemaFetchScanNodeTest {

  @Test
  public void testSerialization() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.**.*"));
    DeviceSchemaFetchScanNode deviceSchemaFetchScanNodeTest =
        new DeviceSchemaFetchScanNode(
            new PlanNodeId("0"), new PartialPath("root.sg"), patternTree, patternTree);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
    deviceSchemaFetchScanNodeTest.serialize(byteBuffer);
    byteBuffer.flip();
    DeviceSchemaFetchScanNode recoveredNode =
        (DeviceSchemaFetchScanNode) PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals("root.sg", recoveredNode.getStorageGroup().getFullPath());
    Assert.assertEquals(
        "root.sg.**.*", recoveredNode.getPatternTree().getAllPathPatterns().get(0).getFullPath());
    Assert.assertEquals(
        "root.sg.**.*",
        recoveredNode.getAuthorityScope().getAllPathPatterns().get(0).getFullPath());
  }
}
