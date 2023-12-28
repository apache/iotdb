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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.pipe.datastructure.AbstractSerializableListeningQueue;
import org.apache.iotdb.commons.pipe.datastructure.LinkedQueueSerializerType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaNodeListeningQueue extends AbstractSerializableListeningQueue<PlanNode> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaNodeListeningQueue.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

  private SchemaNodeListeningQueue() {
    super(LinkedQueueSerializerType.PLAIN);
  }

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToNode(PlanNode node) {
    if (queue.hasAnyIterators() && PipeSchemaNodeFilter.shouldBeListenedByQueue(node)) {
      super.listenToElement(node);
    }
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  @Override
  protected ByteBuffer serializeToByteBuffer(PlanNode node) {
    return node.serializeToByteBuffer();
  }

  @Override
  protected PlanNode deserializeFromByteBuffer(ByteBuffer byteBuffer) {
    return PlanNodeType.deserialize(byteBuffer);
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public boolean createSnapshot(File snapshotDir) {
    try {
      return super.serializeToFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
    } catch (IOException e) {
      LOGGER.warn("Take snapshot error: {}", e.getMessage());
      return false;
    }
  }

  public void loadSnapshot(File snapshotDir) {
    try {
      super.deserializeFromFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
    } catch (IOException e) {
      LOGGER.error("Failed to load snapshot {}", e.getMessage());
    }
  }

  /////////////////////////// Singleton ///////////////////////////

  public static SchemaNodeListeningQueue getInstance(int regionId) {
    return SchemaNodeListeningQueue.InstanceHolder.getOrCreateInstance(regionId);
  }

  private static class InstanceHolder {

    private static final Map<Integer, SchemaNodeListeningQueue> INSTANCE_MAP =
        new ConcurrentHashMap<>();

    public static SchemaNodeListeningQueue getOrCreateInstance(int key) {
      return INSTANCE_MAP.computeIfAbsent(key, k -> new SchemaNodeListeningQueue());
    }

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
