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

package org.apache.iotdb.commons.pipe.task.meta;

import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SchemaProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeExceptionType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeMeta {

  private final AtomicReference<PipeStatus> status = new AtomicReference<>(PipeStatus.STOPPED);

  /**
   * The Integers are for recording the {@link Integer}, which varies in:
   *
   * <p>1. {@link DataRegionId} Used to store progress for schema transmission on DataRegion,
   * usually updated since the data synchronization is basic function of pipe.
   *
   * <p>2. {@link SchemaRegionId} Used to store {@link SchemaProgressIndex} for schema transmission
   * on SchemaRegion, always {@link MinimumProgressIndex} if the pipe has no relation to schema
   * synchronization.
   *
   * <p>3. {@link ConfigRegionId} Used to store {@link SchemaProgressIndex} for schema transmission
   * on ConfigNode, always {@link MinimumProgressIndex} if the pipe has no relation to schema
   * synchronization.
   *
   * <p>
   *
   * <p>Notes:
   *
   * <p>- The {@link ConfigRegionId#getId()} is always {@link Integer#MIN_VALUE} since the original
   * id 0 clashes with the start of {@link SchemaRegionId#getId()} and {@link DataRegionId#getId()}
   *
   * <p>- The {@link ConfigRegionId#getId()} and {@link SchemaRegionId#getId()}'s {@link
   * PipeTaskMeta}s engender nothing if the pipe has nothing to do with metadata.
   *
   * <p>- The {@link ConfigRegionId}s and {@link SchemaRegionId}s will not exist for the pipes
   * recovered from log with previous versions, and this is guaranteed to be seen as if they exist
   * but do not spark schema transmission.
   */
  private final Map<Integer, PipeTaskMeta> consensusGroupId2TaskMetaMap;

  /**
   * Stores the newest exceptions encountered group by dataNodes.
   *
   * <p>The exceptions are all instances of:
   *
   * <p>1. {@link PipeRuntimeCriticalException}, to record the failure of pushing pipeMeta, and will
   * result in the halt of pipe execution.
   *
   * <p>2. {@link PipeRuntimeConnectorCriticalException}, to record the exception reported by other
   * pipes sharing the same connector, and will stop the pipe likewise.
   */
  private final Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMap =
      new ConcurrentHashMap<>();

  private final AtomicLong exceptionsClearTime = new AtomicLong(Long.MIN_VALUE);

  private final AtomicBoolean shouldBeRunning = new AtomicBoolean(false);

  public PipeRuntimeMeta() {
    consensusGroupId2TaskMetaMap = new ConcurrentHashMap<>();
  }

  public PipeRuntimeMeta(Map<Integer, PipeTaskMeta> consensusGroupId2TaskMetaMap) {
    this.consensusGroupId2TaskMetaMap = consensusGroupId2TaskMetaMap;
  }

  public AtomicReference<PipeStatus> getStatus() {
    return status;
  }

  public Map<Integer, PipeTaskMeta> getConsensusGroupId2TaskMetaMap() {
    return consensusGroupId2TaskMetaMap;
  }

  public Map<Integer, PipeRuntimeException> getDataNodeId2PipeRuntimeExceptionMap() {
    return dataNodeId2PipeRuntimeExceptionMap;
  }

  public long getExceptionsClearTime() {
    return exceptionsClearTime.get();
  }

  public void setExceptionsClearTime(long exceptionsClearTime) {
    if (exceptionsClearTime > this.getExceptionsClearTime()) {
      this.exceptionsClearTime.set(exceptionsClearTime);
    }
  }

  public boolean getShouldBeRunning() {
    return shouldBeRunning.get();
  }

  public void setShouldBeRunning(boolean shouldBeRunning) {
    this.shouldBeRunning.set(shouldBeRunning);
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    PipeRuntimeMetaVersion.VERSION_2.serialize(outputStream);

    ReadWriteIOUtils.write(status.get().getType(), outputStream);

    // Avoid concurrent modification
    final Map<Integer, PipeTaskMeta> consensusGroupId2TaskMetaMapView =
        new HashMap<>(consensusGroupId2TaskMetaMap);
    ReadWriteIOUtils.write(consensusGroupId2TaskMetaMapView.size(), outputStream);
    for (Map.Entry<Integer, PipeTaskMeta> entry : consensusGroupId2TaskMetaMapView.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    // Avoid concurrent modification
    final Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMapView =
        new HashMap<>(dataNodeId2PipeRuntimeExceptionMap);
    ReadWriteIOUtils.write(dataNodeId2PipeRuntimeExceptionMapView.size(), outputStream);
    for (Map.Entry<Integer, PipeRuntimeException> entry :
        dataNodeId2PipeRuntimeExceptionMapView.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    ReadWriteIOUtils.write(exceptionsClearTime.get(), outputStream);
    ReadWriteIOUtils.write(shouldBeRunning.get(), outputStream);
  }

  public static PipeRuntimeMeta deserialize(InputStream inputStream) throws IOException {
    final byte pipeRuntimeVersionByte = ReadWriteIOUtils.readByte(inputStream);
    final PipeRuntimeMetaVersion pipeRuntimeMetaVersion =
        PipeRuntimeMetaVersion.deserialize(pipeRuntimeVersionByte);
    switch (pipeRuntimeMetaVersion) {
      case VERSION_1:
        return deserializeVersion1(inputStream, pipeRuntimeVersionByte);
      case VERSION_2:
        return deserializeVersion2(inputStream);
      default:
        throw new UnsupportedOperationException(
            "Unknown pipe runtime meta version: " + pipeRuntimeMetaVersion.getVersion());
    }
  }

  private static PipeRuntimeMeta deserializeVersion1(InputStream inputStream, byte pipeStatusByte)
      throws IOException {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(pipeStatusByte));

    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          ReadWriteIOUtils.readInt(inputStream),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_1, inputStream));
    }

    return pipeRuntimeMeta;
  }

  private static PipeRuntimeMeta deserializeVersion2(InputStream inputStream) throws IOException {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(inputStream)));

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          ReadWriteIOUtils.readInt(inputStream),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(inputStream),
          PipeRuntimeExceptionType.deserializeFrom(PipeRuntimeMetaVersion.VERSION_2, inputStream));
    }

    pipeRuntimeMeta.exceptionsClearTime.set(ReadWriteIOUtils.readLong(inputStream));
    pipeRuntimeMeta.shouldBeRunning.set(ReadWriteIOUtils.readBool(inputStream));

    return pipeRuntimeMeta;
  }

  public static PipeRuntimeMeta deserialize(ByteBuffer byteBuffer) {
    final byte pipeRuntimeVersionByte = ReadWriteIOUtils.readByte(byteBuffer);
    final PipeRuntimeMetaVersion pipeRuntimeMetaVersion =
        PipeRuntimeMetaVersion.deserialize(pipeRuntimeVersionByte);
    switch (pipeRuntimeMetaVersion) {
      case VERSION_1:
        return deserializeVersion1(byteBuffer, pipeRuntimeVersionByte);
      case VERSION_2:
        return deserializeVersion2(byteBuffer);
      default:
        throw new UnsupportedOperationException(
            "Unknown pipe runtime meta version: " + pipeRuntimeMetaVersion.getVersion());
    }
  }

  private static PipeRuntimeMeta deserializeVersion1(
      ByteBuffer byteBuffer, byte pipeRuntimeVersionByte) {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(pipeRuntimeVersionByte));

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          ReadWriteIOUtils.readInt(byteBuffer),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_1, byteBuffer));
    }

    return pipeRuntimeMeta;
  }

  public static PipeRuntimeMeta deserializeVersion2(ByteBuffer byteBuffer) {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer)));

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          ReadWriteIOUtils.readInt(byteBuffer),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(byteBuffer),
          PipeRuntimeExceptionType.deserializeFrom(PipeRuntimeMetaVersion.VERSION_2, byteBuffer));
    }

    pipeRuntimeMeta.exceptionsClearTime.set(ReadWriteIOUtils.readLong(byteBuffer));
    pipeRuntimeMeta.shouldBeRunning.set(ReadWriteIOUtils.readBool(byteBuffer));

    return pipeRuntimeMeta;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeRuntimeMeta that = (PipeRuntimeMeta) o;
    return Objects.equals(status.get().getType(), that.status.get().getType())
        && consensusGroupId2TaskMetaMap.equals(that.consensusGroupId2TaskMetaMap)
        && dataNodeId2PipeRuntimeExceptionMap.equals(that.dataNodeId2PipeRuntimeExceptionMap)
        && exceptionsClearTime.get() == that.exceptionsClearTime.get()
        && shouldBeRunning.get() == that.shouldBeRunning.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        status.get(),
        consensusGroupId2TaskMetaMap,
        dataNodeId2PipeRuntimeExceptionMap,
        exceptionsClearTime.get(),
        shouldBeRunning.get());
  }

  @Override
  public String toString() {
    return "PipeRuntimeMeta{"
        + "status="
        + status.get()
        + ", consensusGroupId2TaskMetaMap="
        + consensusGroupId2TaskMetaMap
        + ", dataNodeId2PipeRuntimeExceptionMap="
        + dataNodeId2PipeRuntimeExceptionMap
        + ", exceptionsClearTime="
        + exceptionsClearTime.get()
        + ", shouldBeRunning="
        + shouldBeRunning.get()
        + "}";
  }
}
