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
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeMeta {

  private final AtomicReference<PipeStatus> status;
  private final List<String> exceptionMessages;
  private final Map<TConsensusGroupId, PipeConsensusGroupTaskMeta> consensusGroupIdToTaskMetaMap;

  public PipeRuntimeMeta() {
    status = new AtomicReference<>(PipeStatus.STOPPED);
    exceptionMessages = new LinkedList<>();
    consensusGroupIdToTaskMetaMap = new ConcurrentHashMap<>();
  }

  public AtomicReference<PipeStatus> getStatus() {
    return status;
  }

  public List<String> getExceptionMessages() {
    return exceptionMessages;
  }

  public Map<TConsensusGroupId, PipeConsensusGroupTaskMeta> getConsensusGroupIdToTaskMetaMap() {
    return consensusGroupIdToTaskMetaMap;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(status.get().getType(), outputStream);

    // ignore exception messages

    ReadWriteIOUtils.write(consensusGroupIdToTaskMetaMap.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, PipeConsensusGroupTaskMeta> entry :
        consensusGroupIdToTaskMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static PipeRuntimeMeta deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  public static PipeRuntimeMeta deserialize(ByteBuffer byteBuffer) throws IOException {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer)));

    // ignore exception messages

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.consensusGroupIdToTaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          PipeConsensusGroupTaskMeta.deserialize(byteBuffer));
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
    return status.equals(that.status)
        && exceptionMessages.equals(that.exceptionMessages)
        && consensusGroupIdToTaskMetaMap.equals(that.consensusGroupIdToTaskMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, exceptionMessages, consensusGroupIdToTaskMetaMap);
  }

  @Override
  public String toString() {
    return "PipeRuntimeMeta{"
        + "status="
        + status
        + ", exceptionMessages="
        + exceptionMessages
        + ", consensusGroupIdToTaskMetaMap="
        + consensusGroupIdToTaskMetaMap
        + '}';
  }
}
