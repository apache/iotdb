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
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeTaskMeta {

  private String pipeName;

  private long createTime;

  private PipeStatus status;

  private Map<String, String> collectorAttributes = new HashMap<>();

  private Map<String, String> processorAttributes = new HashMap<>();

  private Map<String, String> connectorAttributes = new HashMap<>();

  private final List<String> messages = new ArrayList<>();

  private Map<TConsensusGroupId, DataRegionPipeTaskMeta> dataRegionPipeTasks = new HashMap<>();

  private PipeTaskMeta() {}

  public PipeTaskMeta(
      String pipeName,
      long createTime,
      PipeStatus status,
      Map<String, String> collectorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes,
      Map<TConsensusGroupId, DataRegionPipeTaskMeta> dataRegionPipeTasks) {
    this.pipeName = pipeName.toUpperCase();
    this.createTime = createTime;
    this.status = status;
    this.collectorAttributes = collectorAttributes;
    this.processorAttributes = processorAttributes;
    this.connectorAttributes = connectorAttributes;
    this.dataRegionPipeTasks = dataRegionPipeTasks;
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreateTime() {
    return createTime;
  }

  public Map<String, String> getCollectorAttributes() {
    return collectorAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return collectorAttributes;
  }

  public Map<String, String> getConnectorAttributes() {
    return collectorAttributes;
  }

  public PipeStatus getStatus() {
    return status;
  }

  public List<String> getMessages() {
    return messages;
  }

  public DataRegionPipeTaskMeta getDataRegionPipeTask(int regionGroup) {
    return dataRegionPipeTasks.get(regionGroup);
  }

  public Set<TConsensusGroupId> getRegionGroups() {
    return dataRegionPipeTasks.keySet();
  }

  public void setStatus(PipeStatus status) {
    this.status = status;
  }

  public void addMessage(String message) {
    messages.add(message);
  }

  public void addDataRegionPipeTask(
      TConsensusGroupId id, DataRegionPipeTaskMeta dataRegionPipeTaskMeta) {
    this.dataRegionPipeTasks.put(id, dataRegionPipeTaskMeta);
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeName, outputStream);
    ReadWriteIOUtils.write(createTime, outputStream);
    ReadWriteIOUtils.write(status.getType(), outputStream);
    outputStream.writeInt(collectorAttributes.size());
    for (Map.Entry<String, String> entry : collectorAttributes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    outputStream.writeInt(processorAttributes.size());
    for (Map.Entry<String, String> entry : processorAttributes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    outputStream.writeInt(connectorAttributes.size());
    for (Map.Entry<String, String> entry : connectorAttributes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    outputStream.writeInt(dataRegionPipeTasks.size());
    for (Map.Entry<TConsensusGroupId, DataRegionPipeTaskMeta> entry :
        dataRegionPipeTasks.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    PipeTaskMeta pipeTaskMeta = new PipeTaskMeta();
    pipeTaskMeta.pipeName = ReadWriteIOUtils.readString(byteBuffer);
    pipeTaskMeta.createTime = ReadWriteIOUtils.readLong(byteBuffer);
    pipeTaskMeta.status = PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer));
    int size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeTaskMeta.collectorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeTaskMeta.processorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeTaskMeta.connectorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeTaskMeta.dataRegionPipeTasks.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          DataRegionPipeTaskMeta.deserialize(byteBuffer));
    }
    return pipeTaskMeta;
  }

  public static PipeTaskMeta deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTaskMeta that = (PipeTaskMeta) obj;
    return pipeName.equals(that.pipeName)
        && createTime == that.createTime
        && status.equals(that.status)
        && collectorAttributes.equals(that.collectorAttributes)
        && processorAttributes.equals(that.processorAttributes)
        && connectorAttributes.equals(that.connectorAttributes)
        && dataRegionPipeTasks.equals(that.dataRegionPipeTasks);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }

  @Override
  public String toString() {
    return "PipeTaskMeta{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", createTime='"
        + createTime
        + '\''
        + ", status='"
        + status
        + '\''
        + ", collectorAttributes='"
        + collectorAttributes
        + '\''
        + ", processorAttributes='"
        + processorAttributes
        + '\''
        + ", connectorAttributes='"
        + connectorAttributes
        + '\''
        + ", DataRegionPipeTasks='"
        + dataRegionPipeTasks
        + '\''
        + '}';
  }
}
