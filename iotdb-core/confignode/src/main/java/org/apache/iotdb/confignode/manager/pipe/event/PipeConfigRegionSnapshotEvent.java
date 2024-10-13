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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.commons.pipe.resource.snapshot.PipeSnapshotResourceManager;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PipeConfigRegionSnapshotEvent extends PipeSnapshotEvent
    implements ReferenceTrackableEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigRegionSnapshotEvent.class);
  private String snapshotPath;
  // This will only be filled in when the snapshot is a schema info file.
  private String templateFilePath;
  private static final Map<CNSnapshotFileType, Set<Short>>
      SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP = new EnumMap<>(CNSnapshotFileType.class);
  private CNSnapshotFileType fileType;

  static {
    SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.put(
        CNSnapshotFileType.ROLE,
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    ConfigPhysicalPlanType.CreateRole.getPlanType(),
                    ConfigPhysicalPlanType.GrantRole.getPlanType()))));
    SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.put(
        CNSnapshotFileType.USER,
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    ConfigPhysicalPlanType.CreateUserWithRawPassword.getPlanType(),
                    ConfigPhysicalPlanType.GrantUser.getPlanType()))));
    SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.put(
        CNSnapshotFileType.USER_ROLE,
        Collections.singleton(ConfigPhysicalPlanType.GrantRoleToUser.getPlanType()));
    SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.put(
        CNSnapshotFileType.SCHEMA,
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    ConfigPhysicalPlanType.CreateDatabase.getPlanType(),
                    ConfigPhysicalPlanType.CreateSchemaTemplate.getPlanType(),
                    ConfigPhysicalPlanType.CommitSetSchemaTemplate.getPlanType()))));
    SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.put(
        CNSnapshotFileType.TTL, Collections.singleton(ConfigPhysicalPlanType.SetTTL.getPlanType()));
  }

  public PipeConfigRegionSnapshotEvent() {
    // Used for deserialization
    this(null, null, null);
  }

  public PipeConfigRegionSnapshotEvent(
      final String snapshotPath, final String templateFilePath, final CNSnapshotFileType type) {
    this(snapshotPath, templateFilePath, type, null, 0, null, null, null);
  }

  public PipeConfigRegionSnapshotEvent(
      final String snapshotPath,
      final String templateFilePath,
      final CNSnapshotFileType type,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        PipeConfigNodeResourceManager.snapshot());
    this.snapshotPath = snapshotPath;
    this.templateFilePath = Objects.nonNull(templateFilePath) ? templateFilePath : "";
    this.fileType = type;
  }

  public File getSnapshotFile() {
    return new File(snapshotPath);
  }

  public File getTemplateFile() {
    return !templateFilePath.isEmpty() ? new File(templateFilePath) : null;
  }

  public CNSnapshotFileType getFileType() {
    return fileType;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    try {
      snapshotPath = resourceManager.increaseSnapshotReference(snapshotPath);
      if (!templateFilePath.isEmpty()) {
        templateFilePath = resourceManager.increaseSnapshotReference(templateFilePath);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for snapshot %s error. Holder Message: %s",
              snapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    try {
      resourceManager.decreaseSnapshotReference(snapshotPath);
      if (!templateFilePath.isEmpty()) {
        resourceManager.decreaseSnapshotReference(templateFilePath);
      }
      return true;
    } catch (final Exception e) {
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
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return new PipeConfigRegionSnapshotEvent(
        snapshotPath,
        templateFilePath,
        fileType,
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    final ByteBuffer result =
        ByteBuffer.allocate(
            2 * Byte.BYTES
                + 2 * Integer.BYTES
                + snapshotPath.getBytes().length
                + templateFilePath.getBytes().length);
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_SNAPSHOT.getType(), result);
    ReadWriteIOUtils.write(fileType.getType(), result);
    ReadWriteIOUtils.write(snapshotPath, result);
    ReadWriteIOUtils.write(templateFilePath, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(final ByteBuffer buffer) {
    fileType = CNSnapshotFileType.deserialize(ReadWriteIOUtils.readByte(buffer));
    snapshotPath = ReadWriteIOUtils.readString(buffer);
    templateFilePath = ReadWriteIOUtils.readString(buffer);
  }

  /////////////////////////////// Type parsing ///////////////////////////////

  public static boolean needTransferSnapshot(final Set<ConfigPhysicalPlanType> listenedTypeSet) {
    final Set<Short> types =
        SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    types.retainAll(
        listenedTypeSet.stream()
            .map(ConfigPhysicalPlanType::getPlanType)
            .collect(Collectors.toSet()));
    return !types.isEmpty();
  }

  public void confineTransferredTypes(final Set<ConfigPhysicalPlanType> listenedTypeSet) {
    final Set<Short> types =
        new HashSet<>(SNAPSHOT_FILE_TYPE_2_CONFIG_PHYSICAL_PLAN_TYPE_MAP.get(fileType));
    types.retainAll(
        listenedTypeSet.stream()
            .map(ConfigPhysicalPlanType::getPlanType)
            .collect(Collectors.toSet()));
    transferredTypes = types;
  }

  public static Set<ConfigPhysicalPlanType> getConfigPhysicalPlanTypeSet(final String sealTypes) {
    return sealTypes.isEmpty()
        ? Collections.emptySet()
        : Arrays.stream(sealTypes.split(","))
            .map(
                typeValue ->
                    ConfigPhysicalPlanType.convertToConfigPhysicalPlanType(
                        Short.parseShort(typeValue)))
            .collect(Collectors.toSet());
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeConfigRegionSnapshotEvent{snapshotPath=%s, templateFilePath=%s, fileType=%s}",
            snapshotPath, templateFilePath, fileType)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeConfigRegionSnapshotEvent{snapshotPath=%s, templateFilePath=%s, fileType=%s}",
            snapshotPath, templateFilePath, fileType)
        + " - "
        + super.coreReportMessage();
  }

  /////////////////////////// ReferenceTrackableEvent ///////////////////////////

  @Override
  protected void trackResource() {
    PipeConfigNodeResourceManager.ref().trackPipeEventResource(this, eventResourceBuilder());
  }

  @Override
  public PipeEventResource eventResourceBuilder() {
    return new PipeConfigRegionSnapshotEventResource(
        this.isReleased,
        this.referenceCount,
        this.resourceManager,
        this.snapshotPath,
        this.templateFilePath);
  }

  private static class PipeConfigRegionSnapshotEventResource extends PipeEventResource {

    private final PipeSnapshotResourceManager resourceManager;
    private final String snapshotPath;
    private final String templateFilePath;

    private PipeConfigRegionSnapshotEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final PipeSnapshotResourceManager resourceManager,
        final String snapshotPath,
        final String templateFilePath) {
      super(isReleased, referenceCount);
      this.resourceManager = resourceManager;
      this.snapshotPath = snapshotPath;
      this.templateFilePath = templateFilePath;
    }

    @Override
    protected void finalizeResource() {
      try {
        resourceManager.decreaseSnapshotReference(snapshotPath);
        if (!templateFilePath.isEmpty()) {
          resourceManager.decreaseSnapshotReference(templateFilePath);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            String.format("Decrease reference count for snapshot %s error.", snapshotPath), e);
      }
    }
  }
}
