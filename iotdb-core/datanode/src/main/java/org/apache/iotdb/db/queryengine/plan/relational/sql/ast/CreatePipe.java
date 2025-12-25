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

public class CreatePipe extends PipeStatement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreatePipe.class);

  private final String pipeName;
  private final boolean ifNotExistsCondition;
  private final Map<String, String> sourceAttributes;
  private final Map<String, String> processorAttributes;
  private final Map<String, String> sinkAttributes;

  public CreatePipe(
      final String pipeName,
      final boolean ifNotExistsCondition,
      final Map<String, String> sourceAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> sinkAttributes) {
    this.pipeName = requireNonNull(pipeName, "pipe name can not be null");
    this.ifNotExistsCondition = ifNotExistsCondition;
    this.sourceAttributes =
        requireNonNull(sourceAttributes, "extractor/source attributes can not be null");
    this.processorAttributes =
        requireNonNull(processorAttributes, "processor attributes can not be null");
    this.sinkAttributes = requireNonNull(sinkAttributes, "connector attributes can not be null");
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public Map<String, String> getSourceAttributes() {
    return sourceAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return processorAttributes;
  }

  public Map<String, String> getSinkAttributes() {
    return sinkAttributes;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreatePipe(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pipeName, ifNotExistsCondition, sourceAttributes, processorAttributes, sinkAttributes);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreatePipe other = (CreatePipe) obj;
    return Objects.equals(pipeName, other.pipeName)
        && Objects.equals(ifNotExistsCondition, other.ifNotExistsCondition)
        && Objects.equals(sourceAttributes, other.sourceAttributes)
        && Objects.equals(processorAttributes, other.processorAttributes)
        && Objects.equals(sinkAttributes, other.sinkAttributes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("ifNotExistsCondition", ifNotExistsCondition)
        .add("extractorAttributes", sourceAttributes)
        .add("processorAttributes", processorAttributes)
        .add("connectorAttributes", sinkAttributes)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(pipeName);
    size += RamUsageEstimator.sizeOfMap(sourceAttributes);
    size += RamUsageEstimator.sizeOfMap(processorAttributes);
    size += RamUsageEstimator.sizeOfMap(sinkAttributes);
    return size;
  }
}
