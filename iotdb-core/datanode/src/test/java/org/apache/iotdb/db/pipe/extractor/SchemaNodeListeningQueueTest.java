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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.db.pipe.event.common.schema.PipeWritePlanNodeEvent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaNodeListeningQueue;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWriteSchemaNode;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class SchemaNodeListeningQueueTest {
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() {
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    SchemaNodeListeningQueue.getInstance(0).close();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException, AuthException, IllegalPathException {
    SchemaNodeListeningQueue.getInstance(0).open();
    SchemaNodeListeningQueue.getInstance(0).notifyLeaderReady();

    CreateTimeSeriesNode node1 =
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

    PipeEnrichedWriteSchemaNode node2 =
        new PipeEnrichedWriteSchemaNode(
            new ActivateTemplateNode(
                new PlanNodeId("ActivateTemplateNode"), new PartialPath("root.sg.d1.s1"), 2, 1));

    SchemaNodeListeningQueue.getInstance(0).tryListenToNode(node1);
    SchemaNodeListeningQueue.getInstance(0).tryListenToNode(node2);

    // tryListenToSnapshots() cannot be tested here since we cannot operate the reference count of
    // the original or deserialized events

    SchemaNodeListeningQueue.getInstance(0).createSnapshot(snapshotDir);
    SchemaNodeListeningQueue.getInstance(0).close();

    SchemaNodeListeningQueue.getInstance(0).loadSnapshot(snapshotDir);
    Assert.assertTrue(SchemaNodeListeningQueue.getInstance(0).isOpened());
    ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr =
        SchemaNodeListeningQueue.getInstance(0).newIterator(0);

    Event event1 = itr.next(0);
    Assert.assertEquals(node1, ((PipeWritePlanNodeEvent) event1).getPlanNode());

    Event event2 = itr.next(0);
    Assert.assertEquals(
        node2.getWriteSchemaNode(), ((PipeWritePlanNodeEvent) event2).getPlanNode());
    Assert.assertTrue(((PipeWritePlanNodeEvent) event2).isGeneratedByPipe());

    Assert.assertNull(itr.next(0));
  }
}
