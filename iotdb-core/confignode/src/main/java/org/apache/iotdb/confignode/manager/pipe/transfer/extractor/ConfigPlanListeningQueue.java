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
import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigSerializableEventType;
import org.apache.iotdb.confignode.manager.pipe.event.PipeWriteConfigPlanEvent;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class ConfigPlanListeningQueue extends AbstractPipeListeningQueue
    implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanListeningQueue.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

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
          event =
              new PipeWriteConfigPlanEvent(
                  new PipeUnsetSchemaTemplatePlan(
                      ClusterTemplateManager.getInstance()
                          .getTemplate(((UnsetSchemaTemplatePlan) plan).getTemplateId())
                          .getName(),
                      ((UnsetSchemaTemplatePlan) plan).getPath().getFullPath()),
                  isGeneratedByPipe);
          break;
        default:
          event = new PipeWriteConfigPlanEvent(plan, isGeneratedByPipe);
      }
      event.increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
      super.listenToElement(event);
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
    if (needToRecord(parameters)) {
      increaseReferenceCount();
    }
  }

  public void decreaseReferenceCountForListeningPipe(PipeParameters parameters)
      throws IllegalPathException, IOException {
    if (needToRecord(parameters)) {
      decreaseReferenceCount();
    }
  }

  private boolean needToRecord(PipeParameters parameters) throws IllegalPathException {
    return !PipeConfigPlanFilter.getPipeListenSet(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                EXTRACTOR_EXCLUSION_DEFAULT_VALUE))
        .isEmpty();
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
    return super.serializeToFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    super.deserializeFromFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
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
