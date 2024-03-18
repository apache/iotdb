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

package org.apache.iotdb.confignode.manager.pipe.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.manager.pipe.resource.snapshot.PipeConfigNodeSnapshotResourceManager;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeConfigRegionSnapshotEvent extends PipeSnapshotEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigRegionSnapshotEvent.class);
  private String snapshotPath;
  private static final Set<Short> CONFIG_PHYSICAL_PLAN_TRANSFER_TYPES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConfigPhysicalPlanType.CreateDatabase.getPlanType(),
                  ConfigPhysicalPlanType.CreateUser.getPlanType(),
                  ConfigPhysicalPlanType.CreateRole.getPlanType(),
                  ConfigPhysicalPlanType.CreateSchemaTemplate.getPlanType(),
                  ConfigPhysicalPlanType.CommitSetSchemaTemplate.getPlanType(),
                  ConfigPhysicalPlanType.GrantUser.getPlanType(),
                  ConfigPhysicalPlanType.GrantRole.getPlanType(),
                  ConfigPhysicalPlanType.SetTTL.getPlanType())));

  public PipeConfigRegionSnapshotEvent() {
    // Used for deserialization
    this(null);
  }

  public PipeConfigRegionSnapshotEvent(String snapshotPath) {
    this(snapshotPath, null, null, null);
  }

  public PipeConfigRegionSnapshotEvent(
      String snapshotPath, String pipeName, PipeTaskMeta pipeTaskMeta, PipePattern pattern) {
    super(pipeName, pipeTaskMeta, pattern, PipeConfigNodeSnapshotResourceManager.getInstance());
    this.snapshotPath = snapshotPath;
  }

  public File getSnapshot() {
    return new File(snapshotPath);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      snapshotPath = resourceManager.increaseSnapshotReference(snapshotPath);
      return true;
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for snapshot %s error. Holder Message: %s",
              snapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      resourceManager.decreaseSnapshotReference(snapshotPath);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for snapshot %s error. Holder Message: %s",
              snapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      long startTime,
      long endTime) {
    return new PipeConfigRegionSnapshotEvent(snapshotPath, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer result =
        ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + snapshotPath.getBytes().length);
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_SNAPSHOT.getType(), result);
    ReadWriteIOUtils.write(snapshotPath, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(ByteBuffer buffer) {
    snapshotPath = ReadWriteIOUtils.readString(buffer);
  }

  /////////////////////////////// Type parsing ///////////////////////////////

  public static boolean needTransferSnapshot(Set<ConfigPhysicalPlanType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(CONFIG_PHYSICAL_PLAN_TRANSFER_TYPES);
    types.retainAll(
        listenedTypeSet.stream()
            .map(ConfigPhysicalPlanType::getPlanType)
            .collect(Collectors.toSet()));
    return !types.isEmpty();
  }

  public void confineTransferredTypes(Set<ConfigPhysicalPlanType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(CONFIG_PHYSICAL_PLAN_TRANSFER_TYPES);
    types.retainAll(
        listenedTypeSet.stream()
            .map(ConfigPhysicalPlanType::getPlanType)
            .collect(Collectors.toSet()));
    transferredTypes = types;
  }

  public static Set<ConfigPhysicalPlanType> getConfigPhysicalPlanTypeSet(String sealTypes) {
    final Set<Short> types = new HashSet<>(CONFIG_PHYSICAL_PLAN_TRANSFER_TYPES);
    types.retainAll(
        Arrays.stream(sealTypes.split(",")).map(Short::valueOf).collect(Collectors.toSet()));
    return types.stream()
        .map(ConfigPhysicalPlanType::convertToConfigPhysicalPlanType)
        .collect(Collectors.toSet());
  }
}
