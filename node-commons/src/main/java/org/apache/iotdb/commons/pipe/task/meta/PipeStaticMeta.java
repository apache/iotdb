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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PipeStaticMeta {

  private String pipeName;
  private long createTime;

  private Map<String, String> collectorAttributes = new HashMap<>();
  private Map<String, String> processorAttributes = new HashMap<>();
  private Map<String, String> connectorAttributes = new HashMap<>();

  private PipeStaticMeta() {}

  public PipeStaticMeta(
      String pipeName,
      long createTime,
      Map<String, String> collectorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes) {
    this.pipeName = pipeName.toUpperCase();
    this.createTime = createTime;
    this.collectorAttributes = collectorAttributes;
    this.processorAttributes = processorAttributes;
    this.connectorAttributes = connectorAttributes;
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
    return processorAttributes;
  }

  public Map<String, String> getConnectorAttributes() {
    return connectorAttributes;
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
  }

  public static PipeStaticMeta deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  public static PipeStaticMeta deserialize(ByteBuffer byteBuffer) {
    final PipeStaticMeta pipeStaticMeta = new PipeStaticMeta();

    pipeStaticMeta.pipeName = ReadWriteIOUtils.readString(byteBuffer);
    pipeStaticMeta.createTime = ReadWriteIOUtils.readLong(byteBuffer);

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeStaticMeta.collectorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeStaticMeta.processorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      pipeStaticMeta.connectorAttributes.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }

    return pipeStaticMeta;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeStaticMeta that = (PipeStaticMeta) obj;
    return pipeName.equals(that.pipeName)
        && createTime == that.createTime
        && collectorAttributes.equals(that.collectorAttributes)
        && processorAttributes.equals(that.processorAttributes)
        && connectorAttributes.equals(that.connectorAttributes);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }

  @Override
  public String toString() {
    return "PipeStaticMeta{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", createTime="
        + createTime
        + ", collectorAttributes="
        + collectorAttributes
        + ", processorAttributes="
        + processorAttributes
        + ", connectorAttributes="
        + connectorAttributes
        + '}';
  }
}
