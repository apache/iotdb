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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeSchemaRegionSnapshotEvent extends PipeSnapshotEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSchemaRegionSnapshotEvent.class);
  private String mTreeSnapshotPath;
  private String tagLogSnapshotPath;
  private String databaseName;

  private static final Map<Short, StatementType> PLAN_NODE_2_STATEMENT_TYPE_MAP = new HashMap<>();

  static {
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_TIME_SERIES.getNodeType(), StatementType.CREATE_TIME_SERIES);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_ALIGNED_TIME_SERIES.getNodeType(),
        StatementType.CREATE_ALIGNED_TIME_SERIES);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.ACTIVATE_TEMPLATE.getNodeType(), StatementType.ACTIVATE_TEMPLATE);
    PLAN_NODE_2_STATEMENT_TYPE_MAP.put(
        PlanNodeType.CREATE_LOGICAL_VIEW.getNodeType(), StatementType.CREATE_LOGICAL_VIEW);
  }

  public PipeSchemaRegionSnapshotEvent() {
    // Used for deserialization
    this(null, null, null);
  }

  public PipeSchemaRegionSnapshotEvent(
      String mTreeSnapshotPath, String tagLogSnapshotPath, String databaseName) {
    this(mTreeSnapshotPath, tagLogSnapshotPath, databaseName, null, null, null);
  }

  public PipeSchemaRegionSnapshotEvent(
      String mTreeSnapshotPath,
      String tagLogSnapshotPath,
      String databaseName,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern) {
    super(pipeName, pipeTaskMeta, pattern, PipeResourceManager.snapshot());
    this.mTreeSnapshotPath = mTreeSnapshotPath;
    this.tagLogSnapshotPath = Objects.nonNull(tagLogSnapshotPath) ? tagLogSnapshotPath : "";
    this.databaseName = databaseName;
  }

  public File getMTreeSnapshotFile() {
    return new File(mTreeSnapshotPath);
  }

  public File getTagLogSnapshotFile() {
    return !tagLogSnapshotPath.isEmpty() ? new File(tagLogSnapshotPath) : null;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      mTreeSnapshotPath = resourceManager.increaseSnapshotReference(mTreeSnapshotPath);
      if (!tagLogSnapshotPath.isEmpty()) {
        tagLogSnapshotPath = resourceManager.increaseSnapshotReference(tagLogSnapshotPath);
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tagLogSnapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      resourceManager.decreaseSnapshotReference(mTreeSnapshotPath);
      if (!tagLogSnapshotPath.isEmpty()) {
        resourceManager.decreaseSnapshotReference(tagLogSnapshotPath);
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tagLogSnapshotPath, holderMessage),
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
    return new PipeSchemaRegionSnapshotEvent(
        mTreeSnapshotPath, tagLogSnapshotPath, databaseName, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer result =
        ByteBuffer.allocate(
            Byte.BYTES
                + 3 * Integer.BYTES
                + mTreeSnapshotPath.getBytes().length
                + tagLogSnapshotPath.getBytes().length
                + databaseName.getBytes().length);
    ReadWriteIOUtils.write(PipeSchemaSerializableEventType.SCHEMA_SNAPSHOT.getType(), result);
    ReadWriteIOUtils.write(mTreeSnapshotPath, result);
    ReadWriteIOUtils.write(tagLogSnapshotPath, result);
    ReadWriteIOUtils.write(databaseName, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(ByteBuffer buffer) {
    mTreeSnapshotPath = ReadWriteIOUtils.readString(buffer);
    tagLogSnapshotPath = ReadWriteIOUtils.readString(buffer);
    databaseName = ReadWriteIOUtils.readString(buffer);
  }

  /////////////////////////////// Type parsing ///////////////////////////////

  public static boolean needTransferSnapshot(Set<PlanNodeType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(PLAN_NODE_2_STATEMENT_TYPE_MAP.keySet());
    types.retainAll(
        listenedTypeSet.stream().map(PlanNodeType::getNodeType).collect(Collectors.toSet()));
    return !types.isEmpty();
  }

  public void confineTransferredTypes(Set<PlanNodeType> listenedTypeSet) {
    final Set<Short> types = new HashSet<>(PLAN_NODE_2_STATEMENT_TYPE_MAP.keySet());
    types.retainAll(
        listenedTypeSet.stream().map(PlanNodeType::getNodeType).collect(Collectors.toSet()));
    transferredTypes = types;
  }

  public static Set<StatementType> getStatementTypeSet(String sealTypes) {
    Map<Short, StatementType> statementTypeMap = new HashMap<>(PLAN_NODE_2_STATEMENT_TYPE_MAP);
    statementTypeMap
        .keySet()
        .retainAll(
            Arrays.stream(sealTypes.split(",")).map(Short::valueOf).collect(Collectors.toSet()));
    return new HashSet<>(statementTypeMap.values());
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeSchemaRegionSnapshotEvent{mTreeSnapshotPath=%s, tagLogSnapshotPath=%s, databaseName=%s}",
            mTreeSnapshotPath, tagLogSnapshotPath, databaseName)
        + " - "
        + super.toString();
  }
}
