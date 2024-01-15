/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.transfer.extractor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigSerializableEventType;
import org.apache.iotdb.confignode.manager.pipe.event.PipeWriteConfigPlanEvent;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ConfigPlanListeningQueue extends AbstractPipeListeningQueue
    implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanListeningQueue.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

  private int referenceCount = 0;

  private ConfigPlanListeningQueue() {
    super();
  }

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToPlan(ConfigPhysicalPlan plan, boolean isGeneratedByPipe) {
    if (PipeConfigPlanFilter.shouldBeListenedByQueue(plan)) {
      PipeWriteConfigPlanEvent event;
      switch (plan.getType()) {
        case PipeEnriched:
          tryListenToPlan(((PipeEnrichedPlan) plan).getInnerPlan(), true);
          return;
        case UnsetTemplate:
          try {
            event =
                new PipeWriteConfigPlanEvent(
                    new PipeUnsetSchemaTemplatePlan(
                        ConfigNode.getInstance()
                            .getConfigManager()
                            .getClusterSchemaManager()
                            .getTemplate(((UnsetSchemaTemplatePlan) plan).getTemplateId())
                            .getName(),
                        ((UnsetSchemaTemplatePlan) plan).getPath().getFullPath()),
                    isGeneratedByPipe);
          } catch (MetadataException e) {
            LOGGER.warn("Failed to collect UnsetTemplatePlan because ", e);
            return;
          }
          break;
        case CreateDatabase:
          // Do not transfer system database plan
          if (((DatabaseSchemaPlan) plan)
              .getSchema()
              .getName()
              .equals(SchemaConstant.SYSTEM_DATABASE)) {
            return;
          }
          event = new PipeWriteConfigPlanEvent(plan, isGeneratedByPipe);
          break;
        default:
          event = new PipeWriteConfigPlanEvent(plan, isGeneratedByPipe);
      }
      if (super.tryListenToElement(event)) {
        event.increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
      }
    }
  }

  public void tryListenToSnapshots(List<String> snapshotPaths) {
    List<PipeSnapshotEvent> events = new ArrayList<>();
    for (String snapshotPath : snapshotPaths) {
      PipeConfigRegionSnapshotEvent event = new PipeConfigRegionSnapshotEvent(snapshotPath);
      event.increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
      events.add(event);
    }
    super.listenToSnapshots(events);
  }

  /////////////////////////////// Reference count ///////////////////////////////

  // ConfigNodeQueue does not need extra protection of consensus, because the
  // reference count is handled under consensus layer.
  public void increaseReferenceCountForListeningPipe(PipeParameters parameters)
      throws IllegalPathException {
    if (!PipeConfigPlanFilter.getPipeListenSet(parameters).isEmpty()) {
      referenceCount++;
      if (referenceCount == 1) {
        open();
      }
    }
  }

  public void decreaseReferenceCountForListeningPipe(PipeParameters parameters)
      throws IllegalPathException, IOException {
    if (!PipeConfigPlanFilter.getPipeListenSet(parameters).isEmpty()) {
      referenceCount--;
      if (referenceCount == 0) {
        close();
      }
    }
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////
  @Override
  protected ByteBuffer serializeToByteBuffer(Event event) {
    return ((SerializableEvent) event).serializeToByteBuffer();
  }

  @Override
  protected Event deserializeFromByteBuffer(ByteBuffer byteBuffer) {
    try {
      SerializableEvent result = PipeConfigSerializableEventType.deserialize(byteBuffer);
      ((EnrichedEvent) result).increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
      return result;
    } catch (IOException e) {
      LOGGER.error("Failed to load snapshot from byteBuffer {}.", byteBuffer);
    }
    return null;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return super.serializeToFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    super.deserializeFromFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
  }

  /////////////////////////////// INSTANCE ///////////////////////////////

  public static ConfigPlanListeningQueue getInstance() {
    return ConfigPlanListeningQueueHolder.INSTANCE;
  }

  private static class ConfigPlanListeningQueueHolder {

    private static final ConfigPlanListeningQueue INSTANCE = new ConfigPlanListeningQueue();

    private ConfigPlanListeningQueueHolder() {
      // empty constructor
    }
  }
}
