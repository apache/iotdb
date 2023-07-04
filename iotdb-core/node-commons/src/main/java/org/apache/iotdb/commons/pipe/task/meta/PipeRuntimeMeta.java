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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeExceptionType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeMeta {

  private final AtomicReference<PipeStatus> status;
  private final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupId2TaskMetaMap;

  /**
   * Stores the exceptions encountered during pushing pipeMeta to DataNodes. The exceptions are all
   * instances of PipeRuntimeCriticalException, so that the failure of pushing pipeMeta will result
   * in the halt of transferring data.
   */
  private final Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMap;

  public PipeRuntimeMeta() {
    status = new AtomicReference<>(PipeStatus.STOPPED);
    consensusGroupId2TaskMetaMap = new ConcurrentHashMap<>();
    dataNodeId2PipeRuntimeExceptionMap = new ConcurrentHashMap<>();
  }

  public PipeRuntimeMeta(Map<TConsensusGroupId, PipeTaskMeta> consensusGroupId2TaskMetaMap) {
    status = new AtomicReference<>(PipeStatus.STOPPED);
    this.consensusGroupId2TaskMetaMap = consensusGroupId2TaskMetaMap;
    dataNodeId2PipeRuntimeExceptionMap = new ConcurrentHashMap<>();
  }

  public AtomicReference<PipeStatus> getStatus() {
    return status;
  }

  public Map<TConsensusGroupId, PipeTaskMeta> getConsensusGroupId2TaskMetaMap() {
    return consensusGroupId2TaskMetaMap;
  }

  public Map<Integer, PipeRuntimeException> getDataNodeId2PipeRuntimeExceptionMap() {
    return dataNodeId2PipeRuntimeExceptionMap;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(status.get().getType(), outputStream);

    ReadWriteIOUtils.write(consensusGroupId2TaskMetaMap.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> entry :
        consensusGroupId2TaskMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    ReadWriteIOUtils.write(dataNodeId2PipeRuntimeExceptionMap.size(), outputStream);
    for (Map.Entry<Integer, PipeRuntimeException> entry :
        dataNodeId2PipeRuntimeExceptionMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(status.get().getType(), outputStream);

    ReadWriteIOUtils.write(consensusGroupId2TaskMetaMap.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> entry :
        consensusGroupId2TaskMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    ReadWriteIOUtils.write(dataNodeId2PipeRuntimeExceptionMap.size(), outputStream);
    for (Map.Entry<Integer, PipeRuntimeException> entry :
        dataNodeId2PipeRuntimeExceptionMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static PipeRuntimeMeta deserialize(InputStream inputStream) throws IOException {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(inputStream)));

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(inputStream)),
          PipeTaskMeta.deserialize(inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(inputStream),
          PipeRuntimeExceptionType.deserializeFrom(inputStream));
    }

    return pipeRuntimeMeta;
  }

  public static PipeRuntimeMeta deserialize(ByteBuffer byteBuffer) {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer)));

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          PipeTaskMeta.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(byteBuffer),
          PipeRuntimeExceptionType.deserializeFrom(byteBuffer));
    }

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
        && dataNodeId2PipeRuntimeExceptionMap.equals(that.dataNodeId2PipeRuntimeExceptionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, consensusGroupId2TaskMetaMap, dataNodeId2PipeRuntimeExceptionMap);
  }

  @Override
  public String toString() {
    return "PipeRuntimeMeta{"
        + "status="
        + status
        + ", consensusGroupId2TaskMetaMap="
        + consensusGroupId2TaskMetaMap
        + ", dataNodeId2PipeMetaExceptionMap="
        + dataNodeId2PipeRuntimeExceptionMap
        + '}';
  }
}
