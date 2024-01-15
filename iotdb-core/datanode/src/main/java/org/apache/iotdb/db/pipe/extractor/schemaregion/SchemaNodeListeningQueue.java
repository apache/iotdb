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

import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaSerializableEventType;
import org.apache.iotdb.db.pipe.event.common.schema.PipeWriteSchemaPlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedConfigSchemaNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWriteSchemaNode;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaNodeListeningQueue extends AbstractPipeListeningQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaNodeListeningQueue.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

  private SchemaNodeListeningQueue() {
    super();
  }

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToNode(PlanNode node) {
    if (PipeSchemaNodeFilter.shouldBeListenedByQueue(node)) {
      PipeWriteSchemaPlanEvent event;
      switch (node.getType()) {
        case PIPE_ENRICHED_WRITE_SCHEMA:
          event =
              new PipeWriteSchemaPlanEvent(
                  ((PipeEnrichedWriteSchemaNode) node).getWriteSchemaNode(), true);
          break;
        case PIPE_ENRICHED_CONFIG_SCHEMA:
          event =
              new PipeWriteSchemaPlanEvent(
                  ((PipeEnrichedConfigSchemaNode) node).getConfigSchemaNode(), true);
          break;
        default:
          event = new PipeWriteSchemaPlanEvent(node, false);
      }
      if (super.tryListenToElement(event)) {
        event.increaseReferenceCount(SchemaNodeListeningQueue.class.getName());
      }
    }
  }

  public void tryListenToSnapshot(List<String> snapshotPaths) {
    List<PipeSnapshotEvent> events = new ArrayList<>();
    for (String snapshotPath : snapshotPaths) {
      PipeSchemaRegionSnapshotEvent event = new PipeSchemaRegionSnapshotEvent(snapshotPath);
      event.increaseReferenceCount(SchemaNodeListeningQueue.class.getName());
      events.add(event);
    }
    super.listenToSnapshots(events);
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  @Override
  protected ByteBuffer serializeToByteBuffer(Event event) {
    return ((SerializableEvent) event).serializeToByteBuffer();
  }

  @Override
  protected Event deserializeFromByteBuffer(ByteBuffer byteBuffer) {
    try {
      SerializableEvent result = PipeSchemaSerializableEventType.deserialize(byteBuffer);
      ((EnrichedEvent) result).increaseReferenceCount(SchemaNodeListeningQueue.class.getName());
      return result;
    } catch (IOException e) {
      LOGGER.error("Failed to load snapshot from byteBuffer {}.", byteBuffer);
    }
    return null;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public boolean createSnapshot(File snapshotDir) {
    try {
      return super.serializeToFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
    } catch (IOException e) {
      LOGGER.warn("Take snapshot error: {}", e.getMessage());
      return false;
    }
  }

  public void loadSnapshot(File snapshotDir) {
    try {
      super.deserializeFromFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
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
