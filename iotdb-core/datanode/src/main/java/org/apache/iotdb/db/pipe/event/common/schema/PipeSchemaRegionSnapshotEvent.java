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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeSchemaRegionSnapshotEvent extends PipeSnapshotEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSchemaRegionSnapshotEvent.class);
  private String mTreeSnapshotPath;
  private String tLogPath;
  private String databaseName;

  public PipeSchemaRegionSnapshotEvent() {
    // Used for deserialization
    this(null, null, null);
  }

  public PipeSchemaRegionSnapshotEvent(
      String mTreeSnapshotPath, String tLogPath, String databaseName) {
    this(mTreeSnapshotPath, tLogPath, databaseName, null, null, null);
  }

  public PipeSchemaRegionSnapshotEvent(
      String mTreeSnapshotPath,
      String tLogPath,
      String databaseName,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern) {
    super(pipeName, pipeTaskMeta, pattern, PipeResourceManager.snapshot());
    this.mTreeSnapshotPath = mTreeSnapshotPath;
    this.tLogPath = Objects.nonNull(tLogPath) ? tLogPath : "";
    this.databaseName = databaseName;
  }

  public File getMTreeSnapshotFile() {
    return new File(mTreeSnapshotPath);
  }

  public File getTLogFile() {
    return !tLogPath.isEmpty() ? new File(tLogPath) : null;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      mTreeSnapshotPath = resourceManager.increaseSnapshotReference(mTreeSnapshotPath);
      tLogPath = resourceManager.increaseSnapshotReference(tLogPath);
      return true;
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tLogPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      resourceManager.decreaseSnapshotReference(mTreeSnapshotPath);
      resourceManager.decreaseSnapshotReference(tLogPath);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for mTree snapshot %s or tLog %s error. Holder Message: %s",
              mTreeSnapshotPath, tLogPath, holderMessage),
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
        mTreeSnapshotPath, tLogPath, databaseName, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer result =
        ByteBuffer.allocate(
            Byte.BYTES
                + 3 * Integer.BYTES
                + mTreeSnapshotPath.getBytes().length
                + tLogPath.getBytes().length
                + databaseName.getBytes().length);
    ReadWriteIOUtils.write(PipeSchemaSerializableEventType.SCHEMA_SNAPSHOT.getType(), result);
    ReadWriteIOUtils.write(mTreeSnapshotPath, result);
    ReadWriteIOUtils.write(tLogPath, result);
    ReadWriteIOUtils.write(databaseName, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(ByteBuffer buffer) {
    mTreeSnapshotPath = ReadWriteIOUtils.readString(buffer);
    tLogPath = ReadWriteIOUtils.readString(buffer);
    databaseName = ReadWriteIOUtils.readString(buffer);
  }
}
