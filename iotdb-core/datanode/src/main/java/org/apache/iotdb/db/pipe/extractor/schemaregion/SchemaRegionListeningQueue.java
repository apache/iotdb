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

import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaSerializableEventType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class SchemaRegionListeningQueue extends AbstractPipeListeningQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionListeningQueue.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_schema_region_listening_queue.bin";

  /////////////////////////////// Function ///////////////////////////////

  public synchronized void tryListenToNode(final PlanNode node) {
    if (SchemaRegionListeningFilter.shouldPlanBeListened(node)) {
      final PipeSchemaRegionWritePlanEvent event;
      switch (node.getType()) {
        case PIPE_ENRICHED_WRITE:
          event =
              new PipeSchemaRegionWritePlanEvent(
                  ((PipeEnrichedWritePlanNode) node).getWritePlanNode(), true);
          break;
        case PIPE_ENRICHED_NON_WRITE:
          event =
              new PipeSchemaRegionWritePlanEvent(
                  ((PipeEnrichedNonWritePlanNode) node).getNonWritePlanNode(), true);
          break;
        default:
          event = new PipeSchemaRegionWritePlanEvent(node, false);
      }
      tryListen(event);
    }
  }

  public synchronized void tryListenToSnapshot(
      final String mTreeSnapshotPath, final String tLogPath, final String databaseName) {
    tryListen(
        Objects.nonNull(mTreeSnapshotPath)
            ? Collections.singletonList(
                new PipeSchemaRegionSnapshotEvent(mTreeSnapshotPath, tLogPath, databaseName))
            : Collections.emptyList());
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  @Override
  protected ByteBuffer serializeToByteBuffer(final Event event) {
    return ((SerializableEvent) event).serializeToByteBuffer();
  }

  @Override
  protected Event deserializeFromByteBuffer(final ByteBuffer byteBuffer) {
    try {
      final SerializableEvent result = PipeSchemaSerializableEventType.deserialize(byteBuffer);
      // We assume the caller of this method will put the deserialize result into a queue,
      // so we increase the reference count here.
      ((EnrichedEvent) result).increaseReferenceCount(SchemaRegionListeningQueue.class.getName());
      return result;
    } catch (final IOException e) {
      LOGGER.error("Failed to load snapshot from byteBuffer {}.", byteBuffer);
    }
    return null;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public synchronized boolean createSnapshot(final File snapshotDir) {
    try {
      return super.serializeToFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
    } catch (final Exception e) {
      LOGGER.warn("Take snapshot error: {}", e.getMessage());
      return false;
    }
  }

  public synchronized void loadSnapshot(final File snapshotDir) {
    try {
      super.deserializeFromFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
    } catch (final Exception e) {
      LOGGER.error("Failed to load snapshot {}", e.getMessage());
    }
  }
}
