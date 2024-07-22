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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.auth.user.LocalFileUserAccessor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigSerializableEventType;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConfigRegionListeningQueue extends AbstractPipeListeningQueue
    implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionListeningQueue.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_config_region_listening_queue.bin";

  /////////////////////////////// Function ///////////////////////////////

  public synchronized void tryListenToPlan(
      final ConfigPhysicalPlan plan, final boolean isGeneratedByPipe) {
    if (ConfigRegionListeningFilter.shouldPlanBeListened(plan)) {
      final PipeConfigRegionWritePlanEvent event;
      switch (plan.getType()) {
        case PipeEnriched:
          tryListenToPlan(((PipeEnrichedPlan) plan).getInnerPlan(), true);
          return;
        case UnsetTemplate:
          // Different clusters have different template ids, so we need to
          // convert template id to template name, in order to make the event
          // in receiver side executable.
          try {
            event =
                new PipeConfigRegionWritePlanEvent(
                    new PipeUnsetSchemaTemplatePlan(
                        ConfigNode.getInstance()
                            .getConfigManager()
                            .getClusterSchemaManager()
                            .getTemplate(((UnsetSchemaTemplatePlan) plan).getTemplateId())
                            .getName(),
                        ((UnsetSchemaTemplatePlan) plan).getPath().getFullPath()),
                    isGeneratedByPipe);
          } catch (MetadataException e) {
            LOGGER.warn("Failed to collect UnsetTemplatePlan", e);
            return;
          }
          break;
        default:
          event = new PipeConfigRegionWritePlanEvent(plan, isGeneratedByPipe);
      }
      tryListen(event);
    }
  }

  public synchronized void tryListenToSnapshots(
      final List<Pair<Pair<Path, Path>, CNSnapshotFileType>> snapshotPathInfoList) {
    final List<PipeSnapshotEvent> events = new ArrayList<>();
    for (final Pair<Pair<Path, Path>, CNSnapshotFileType> snapshotPathInfo : snapshotPathInfoList) {
      final Path snapshotPath = snapshotPathInfo.getLeft().getLeft();
      final CNSnapshotFileType type = snapshotPathInfo.getRight();
      // Filter empty and superuser snapshots
      if (snapshotPath.toFile().length() == 0
          || type == CNSnapshotFileType.USER
              && snapshotPath
                  .toFile()
                  .getName()
                  .equals(AuthorityChecker.SUPER_USER + IoTDBConstant.PROFILE_SUFFIX)
          || type == CNSnapshotFileType.USER_ROLE
              && snapshotPath
                  .toFile()
                  .getName()
                  .equals(
                      AuthorityChecker.SUPER_USER
                          + LocalFileUserAccessor.ROLE_SUFFIX
                          + IoTDBConstant.PROFILE_SUFFIX)) {
        continue;
      }
      final Path templateFilePath = snapshotPathInfo.getLeft().getRight();
      events.add(
          new PipeConfigRegionSnapshotEvent(
              snapshotPath.toString(),
              Objects.nonNull(templateFilePath) && templateFilePath.toFile().length() > 0
                  ? templateFilePath.toString()
                  : null,
              snapshotPathInfo.getRight()));
    }
    tryListen(events);
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////

  @Override
  protected ByteBuffer serializeToByteBuffer(final Event event) {
    return ((SerializableEvent) event).serializeToByteBuffer();
  }

  @Override
  protected Event deserializeFromByteBuffer(final ByteBuffer byteBuffer) {
    try {
      final SerializableEvent result = PipeConfigSerializableEventType.deserialize(byteBuffer);
      // We assume the caller of this method will put the deserialize result into a queue,
      // so we increase the reference count here.
      ((EnrichedEvent) result).increaseReferenceCount(ConfigRegionListeningQueue.class.getName());
      return result;
    } catch (final IOException e) {
      LOGGER.error("Failed to load snapshot from byteBuffer {}.", byteBuffer);
    }
    return null;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public synchronized boolean processTakeSnapshot(final File snapshotDir)
      throws TException, IOException {
    return super.serializeToFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
  }

  @Override
  public synchronized void processLoadSnapshot(final File snapshotDir)
      throws TException, IOException {
    super.deserializeFromFile(new File(snapshotDir, SNAPSHOT_FILE_NAME));
  }
}
