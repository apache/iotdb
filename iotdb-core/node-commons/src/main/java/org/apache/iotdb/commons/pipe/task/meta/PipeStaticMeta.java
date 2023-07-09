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

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeStaticMeta {

  private String pipeName;
  private long creationTime;

  private PipeParameters extractorParameters;
  private PipeParameters processorParameters;
  private PipeParameters connectorParameters;

  private PipeStaticMeta() {
    // Empty constructor
  }

  public PipeStaticMeta(
      String pipeName,
      long creationTime,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    extractorParameters = new PipeParameters(extractorAttributes);
    processorParameters = new PipeParameters(processorAttributes);
    connectorParameters = new PipeParameters(connectorAttributes);
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public PipeParameters getExtractorParameters() {
    return extractorParameters;
  }

  public PipeParameters getProcessorParameters() {
    return processorParameters;
  }

  public PipeParameters getConnectorParameters() {
    return connectorParameters;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(extractorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : extractorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(processorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : processorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(connectorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : connectorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(extractorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : extractorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(processorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : processorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(connectorParameters.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : connectorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static PipeStaticMeta deserialize(InputStream inputStream) throws IOException {
    final PipeStaticMeta pipeStaticMeta = new PipeStaticMeta();

    pipeStaticMeta.pipeName = ReadWriteIOUtils.readString(inputStream);
    pipeStaticMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    pipeStaticMeta.extractorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.processorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.connectorParameters = new PipeParameters(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      pipeStaticMeta.extractorParameters.getAttribute().put(key, value);
    }
    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      pipeStaticMeta.processorParameters.getAttribute().put(key, value);
    }
    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      pipeStaticMeta.connectorParameters.getAttribute().put(key, value);
    }

    return pipeStaticMeta;
  }

  public static PipeStaticMeta deserialize(ByteBuffer byteBuffer) {
    final PipeStaticMeta pipeStaticMeta = new PipeStaticMeta();

    pipeStaticMeta.pipeName = ReadWriteIOUtils.readString(byteBuffer);
    pipeStaticMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    pipeStaticMeta.extractorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.processorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.connectorParameters = new PipeParameters(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      pipeStaticMeta.extractorParameters.getAttribute().put(key, value);
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      pipeStaticMeta.processorParameters.getAttribute().put(key, value);
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      pipeStaticMeta.connectorParameters.getAttribute().put(key, value);
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
        && creationTime == that.creationTime
        && extractorParameters.equals(that.extractorParameters)
        && processorParameters.equals(that.processorParameters)
        && connectorParameters.equals(that.connectorParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pipeName, creationTime, extractorParameters, processorParameters, connectorParameters);
  }

  @Override
  public String toString() {
    return "PipeStaticMeta{"
        + "pipeName='"
        + pipeName
        + "', creationTime="
        + creationTime
        + ", extractorParameters="
        + extractorParameters.getAttribute()
        + ", processorParameters="
        + processorParameters.getAttribute()
        + ", connectorParameters="
        + connectorParameters.getAttribute()
        + "}";
  }
}
