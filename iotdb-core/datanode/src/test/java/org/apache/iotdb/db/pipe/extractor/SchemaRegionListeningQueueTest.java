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

package org.apache.iotdb.db.pipe.extractor;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class SchemaRegionListeningQueueTest {
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() {
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).open();
    PipeDataNodeAgent.runtime().notifySchemaLeaderReady(new SchemaRegionId(0));
  }

  @AfterClass
  public static void cleanup() throws IOException {
    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).close();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException, AuthException, IllegalPathException {
    final CreateTimeSeriesNode node1 =
        new CreateTimeSeriesNode(
            new PlanNodeId("CreateTimeSeriesNode"),
            new PartialPath("root.db.d1.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            "alias");

    final PipeEnrichedWritePlanNode node2 =
        new PipeEnrichedWritePlanNode(
            new ActivateTemplateNode(
                new PlanNodeId("ActivateTemplateNode"), new PartialPath("root.sg.d1.s1"), 2, 1));

    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).tryListenToNode(node1);
    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).tryListenToNode(node2);

    // tryListenToSnapshots() cannot be tested here since we cannot operate the reference count of
    // the original or deserialized events

    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).createSnapshot(snapshotDir);
    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).close();

    PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).loadSnapshot(snapshotDir);
    Assert.assertTrue(PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).isOpened());
    final ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr =
        PipeDataNodeAgent.runtime().schemaListener(new SchemaRegionId(0)).newIterator(0);

    final Event event1 = itr.next(0);
    Assert.assertEquals(node1, ((PipeSchemaRegionWritePlanEvent) event1).getPlanNode());

    final Event event2 = itr.next(0);
    Assert.assertEquals(
        node2.getWritePlanNode(), ((PipeSchemaRegionWritePlanEvent) event2).getPlanNode());
    Assert.assertTrue(((PipeSchemaRegionWritePlanEvent) event2).isGeneratedByPipe());

    Assert.assertNull(itr.next(0));
  }
}
