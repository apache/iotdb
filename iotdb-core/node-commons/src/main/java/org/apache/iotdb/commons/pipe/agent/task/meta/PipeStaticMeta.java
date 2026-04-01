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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeStaticMeta {

  private String pipeName;
  private long creationTime;

  private PipeParameters sourceParameters;
  private PipeParameters processorParameters;
  private PipeParameters sinkParameters;

  private PipeStaticMeta() {
    // Empty constructor
  }

  public PipeStaticMeta(
      final String pipeName,
      final long creationTime,
      final Map<String, String> extractorAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> connectorAttributes) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    sourceParameters = new PipeParameters(extractorAttributes);
    processorParameters = new PipeParameters(processorAttributes);
    sinkParameters = new PipeParameters(connectorAttributes);
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public PipeParameters getSourceParameters() {
    return sourceParameters;
  }

  public PipeParameters getProcessorParameters() {
    return processorParameters;
  }

  public PipeParameters getSinkParameters() {
    return sinkParameters;
  }

  public PipeType getPipeType() {
    return PipeType.getPipeType(pipeName);
  }

  public boolean isSourceExternal() {
    return !BuiltinPipePlugin.BUILTIN_SOURCES.contains(
        sourceParameters
            .getStringOrDefault(
                Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase());
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipeName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(sourceParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : sourceParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(processorParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : processorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(sinkParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : sinkParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static PipeStaticMeta deserialize(final InputStream inputStream) throws IOException {
    final PipeStaticMeta pipeStaticMeta = new PipeStaticMeta();

    pipeStaticMeta.pipeName = ReadWriteIOUtils.readString(inputStream);
    pipeStaticMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    pipeStaticMeta.sourceParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.processorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.sinkParameters = new PipeParameters(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      pipeStaticMeta.sourceParameters.getAttribute().put(key, value);
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
      pipeStaticMeta.sinkParameters.getAttribute().put(key, value);
    }

    return pipeStaticMeta;
  }

  public static PipeStaticMeta deserialize(final ByteBuffer byteBuffer) {
    final PipeStaticMeta pipeStaticMeta = new PipeStaticMeta();

    pipeStaticMeta.pipeName = ReadWriteIOUtils.readString(byteBuffer);
    pipeStaticMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    pipeStaticMeta.sourceParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.processorParameters = new PipeParameters(new HashMap<>());
    pipeStaticMeta.sinkParameters = new PipeParameters(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      pipeStaticMeta.sourceParameters.getAttribute().put(key, value);
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
      pipeStaticMeta.sinkParameters.getAttribute().put(key, value);
    }

    return pipeStaticMeta;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeStaticMeta that = (PipeStaticMeta) obj;
    return pipeName.equals(that.pipeName)
        && creationTime == that.creationTime
        && sourceParameters.equals(that.sourceParameters)
        && processorParameters.equals(that.processorParameters)
        && sinkParameters.equals(that.sinkParameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pipeName, creationTime, sourceParameters, processorParameters, sinkParameters);
  }

  @Override
  public String toString() {
    return "PipeStaticMeta{"
        + "pipeName='"
        + pipeName
        + "', creationTime="
        + creationTime
        + ", sourceParameters="
        + sourceParameters
        + ", processorParameters="
        + processorParameters
        + ", sinkParameters="
        + sinkParameters
        + "}";
  }

  /////////////////////////////////  Pipe Name  /////////////////////////////////

  public static final String SYSTEM_PIPE_PREFIX = "__";
  public static final String SUBSCRIPTION_PIPE_PREFIX = SYSTEM_PIPE_PREFIX + "subscription.";
  public static final String CONSENSUS_PIPE_PREFIX = SYSTEM_PIPE_PREFIX + "consensus.";

  public static String generateSubscriptionPipeName(
      final String topicName, final String consumerGroupId) {
    return SUBSCRIPTION_PIPE_PREFIX + topicName + "_" + consumerGroupId;
  }

  public static boolean isSubscriptionPipe(final String pipeName) {
    return Objects.nonNull(pipeName) && pipeName.startsWith(SUBSCRIPTION_PIPE_PREFIX);
  }

  /////////////////////////////////  Tree & Table Isolation  /////////////////////////////////

  public boolean visibleUnder(final boolean isTableModel) {
    final Visibility visibility =
        VisibilityUtils.calculateFromExtractorParameters(sourceParameters);
    return VisibilityUtils.isCompatible(visibility, isTableModel);
  }
}
