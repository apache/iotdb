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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SeriesSchemaFetchScanNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class SchemaFetchMergeNodeTest {

  @Test
  public void testSerialization() throws IllegalPathException {
    SchemaFetchMergeNode schemaFetchMergeNode =
        new SchemaFetchMergeNode(new PlanNodeId("0"), Arrays.asList("root.db1", "root.db2"));
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.**.*"));
    SeriesSchemaFetchScanNode seriesSchemaFetchScanNode =
        new SeriesSchemaFetchScanNode(
            new PlanNodeId("0"),
            new PartialPath("root.sg"),
            patternTree,
            Collections.emptyMap(),
            true,
            false,
            true,
            false);
    schemaFetchMergeNode.addChild(seriesSchemaFetchScanNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
    schemaFetchMergeNode.serialize(byteBuffer);
    byteBuffer.flip();
    SchemaFetchMergeNode recoveredNode =
        (SchemaFetchMergeNode) PlanNodeType.deserialize(byteBuffer);
    Assert.assertEquals(schemaFetchMergeNode, recoveredNode);
  }
}
