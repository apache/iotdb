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

import java.util.Arrays;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeStaticMeta {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeStaticMeta.class);

  private String pipeName;
  private long creationTime;

  private PipeParameters extractorParameters;
  private PipeParameters processorParameters;
  private PipeParameters connectorParameters;

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

  public PipeType getPipeType() {
    return PipeType.getPipeType(pipeName);
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

    ReadWriteIOUtils.write(extractorParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : extractorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(processorParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : processorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
    ReadWriteIOUtils.write(connectorParameters.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : connectorParameters.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static PipeStaticMeta deserialize(final InputStream inputStream) throws IOException {
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

  public static PipeStaticMeta deserialize(final ByteBuffer byteBuffer) {
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
        + extractorParameters
        + ", processorParameters="
        + processorParameters
        + ", connectorParameters="
        + connectorParameters
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

  /////////////////////////////////  Tree & Table Isolation  /////////////////////////////////

  public static final String TREE_PIPE_PREFIX = SYSTEM_PIPE_PREFIX + "tree.";
  public static final String TABLE_PIPE_PREFIX = SYSTEM_PIPE_PREFIX + "table.";

  public String encodeIsolation(final String pipeName) {
    final PipeVisibility visibility = calculateVisibility();
    switch (visibility) {
      case BOTH:
        return pipeName;
      case TREE_ONLY:
        return SYSTEM_PIPE_PREFIX + TREE_PIPE_PREFIX + pipeName;
      case TABLE_ONLY:
        return SYSTEM_PIPE_PREFIX + TABLE_PIPE_PREFIX + pipeName;
      default:
        return pipeName;
    }
  }

  public String decodeIsolation(final String pipeName) {
    final PipeVisibility visibility = calculateVisibility();
    switch (visibility) {
      case BOTH:
        return pipeName;
      case TREE_ONLY:
        // "__.tree.xxx"
        return pipeName.substring(8);
      case TABLE_ONLY:
        // "__.table.xxx"
        return pipeName.substring(9);
      default:
        return pipeName;
    }
  }

  public boolean visibleUnder(final boolean isTableModel) {
    final PipeVisibility visibility = calculateVisibility();
    if (Objects.equals(PipeVisibility.BOTH, visibility)) {
      return true;
    }
    return isTableModel ?
        Objects.equals(PipeVisibility.TABLE_ONLY, visibility) :
        Objects.equals(PipeVisibility.TREE_ONLY, visibility);
  }

  private PipeVisibility calculateVisibility() {
    // visible under all model when 'mode.double-living' is set to true
    final boolean isDoubleLiving =
        extractorParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeExtractorConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeExtractorConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    if (isDoubleLiving) {
      return PipeVisibility.BOTH;
    }

    final boolean isTreeDialect =
        extractorParameters
            .getStringOrDefault(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
            .equals(SystemConstant.SQL_DIALECT_TREE_VALUE);
    final Boolean _isCaptureTree =
        extractorParameters.getBooleanByKeys(
            PipeExtractorConstant.EXTRACTOR_CAPTURE_TREE_KEY,
            PipeExtractorConstant.SOURCE_CAPTURE_TREE_KEY);
    final boolean isCaptureTree = Objects.nonNull(_isCaptureTree) ? _isCaptureTree : isTreeDialect;
    final Boolean _isCaptureTable =
        extractorParameters.getBooleanByKeys(
            PipeExtractorConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
            PipeExtractorConstant.SOURCE_CAPTURE_TABLE_KEY);
    final boolean isCaptureTable =
        Objects.nonNull(_isCaptureTable) ? _isCaptureTable : !isTreeDialect;

    // visible under specific tree or table model <-> actually capture tree or table data
    if (isCaptureTree && isCaptureTable) {
      return PipeVisibility.BOTH;
    }
    if (isCaptureTree) {
      return PipeVisibility.TREE_ONLY;
    }
    if (isCaptureTable) {
      return PipeVisibility.TABLE_ONLY;
    }

    // UNREACHABLE CODE
    LOGGER.error("BROKEN INVARIANT: DETECT INVISIBLE PIPE {}", this);
    return PipeVisibility.NONE;
  }
}
