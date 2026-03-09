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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AlterPipe extends PipeStatement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlterPipe.class);

  private final String pipeName;
  private final boolean ifExistsCondition;
  private final Map<String, String> extractorAttributes;
  private final Map<String, String> processorAttributes;
  private final Map<String, String> connectorAttributes;
  private final boolean isReplaceAllExtractorAttributes;
  private final boolean isReplaceAllProcessorAttributes;
  private final boolean isReplaceAllConnectorAttributes;

  public AlterPipe(
      final String pipeName,
      final boolean ifExistsCondition,
      final Map<String, String> extractorAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> connectorAttributes,
      final boolean isReplaceAllExtractorAttributes,
      final boolean isReplaceAllProcessorAttributes,
      final boolean isReplaceAllConnectorAttributes) {
    this.pipeName = requireNonNull(pipeName);
    this.ifExistsCondition = ifExistsCondition;
    this.extractorAttributes = requireNonNull(extractorAttributes);
    this.processorAttributes = requireNonNull(processorAttributes);
    this.connectorAttributes = requireNonNull(connectorAttributes);
    this.isReplaceAllExtractorAttributes = isReplaceAllExtractorAttributes;
    this.isReplaceAllProcessorAttributes = isReplaceAllProcessorAttributes;
    this.isReplaceAllConnectorAttributes = isReplaceAllConnectorAttributes;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasIfExistsCondition() {
    return ifExistsCondition;
  }

  public Map<String, String> getExtractorAttributes() {
    return extractorAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return processorAttributes;
  }

  public Map<String, String> getConnectorAttributes() {
    return connectorAttributes;
  }

  public boolean isReplaceAllExtractorAttributes() {
    return isReplaceAllExtractorAttributes;
  }

  public boolean isReplaceAllProcessorAttributes() {
    return isReplaceAllProcessorAttributes;
  }

  public boolean isReplaceAllConnectorAttributes() {
    return isReplaceAllConnectorAttributes;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterPipe(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pipeName,
        ifExistsCondition,
        extractorAttributes,
        processorAttributes,
        connectorAttributes,
        isReplaceAllExtractorAttributes,
        isReplaceAllProcessorAttributes,
        isReplaceAllConnectorAttributes);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final AlterPipe that = (AlterPipe) obj;
    return Objects.equals(this.pipeName, that.pipeName)
        && Objects.equals(this.ifExistsCondition, that.ifExistsCondition)
        && Objects.equals(this.extractorAttributes, that.extractorAttributes)
        && Objects.equals(this.processorAttributes, that.processorAttributes)
        && Objects.equals(this.connectorAttributes, that.connectorAttributes)
        && Objects.equals(
            this.isReplaceAllExtractorAttributes, that.isReplaceAllExtractorAttributes)
        && Objects.equals(
            this.isReplaceAllProcessorAttributes, that.isReplaceAllProcessorAttributes)
        && Objects.equals(
            this.isReplaceAllConnectorAttributes, that.isReplaceAllConnectorAttributes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("ifExistsCondition", ifExistsCondition)
        .add("extractorAttributes", extractorAttributes)
        .add("processorAttributes", processorAttributes)
        .add("connectorAttributes", connectorAttributes)
        .add("isReplaceAllExtractorAttributes", isReplaceAllExtractorAttributes)
        .add("isReplaceAllProcessorAttributes", isReplaceAllProcessorAttributes)
        .add("isReplaceAllConnectorAttributes", isReplaceAllConnectorAttributes)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(pipeName);
    size += RamUsageEstimator.sizeOfMap(extractorAttributes);
    size += RamUsageEstimator.sizeOfMap(processorAttributes);
    size += RamUsageEstimator.sizeOfMap(connectorAttributes);
    return size;
  }
}
