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

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreatePipe extends PipeStatement {

  private final String pipeName;
  private final boolean ifNotExistsCondition;
  private final Map<String, String> extractorAttributes;
  private final Map<String, String> processorAttributes;
  private final Map<String, String> connectorAttributes;

  public CreatePipe(
      final String pipeName,
      final boolean ifNotExistsCondition,
      final Map<String, String> extractorAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> connectorAttributes) {
    this.pipeName = requireNonNull(pipeName, "pipe name can not be null");
    this.ifNotExistsCondition = ifNotExistsCondition;
    this.extractorAttributes =
        requireNonNull(extractorAttributes, "extractor/source attributes can not be null");
    this.processorAttributes =
        requireNonNull(processorAttributes, "processor attributes can not be null");
    this.connectorAttributes =
        requireNonNull(connectorAttributes, "connector attributes can not be null");
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
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

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreatePipe(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pipeName,
        ifNotExistsCondition,
        extractorAttributes,
        processorAttributes,
        connectorAttributes);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreatePipe other = (CreatePipe) obj;
    return Objects.equals(pipeName, other.pipeName)
        && Objects.equals(ifNotExistsCondition, other.ifNotExistsCondition)
        && Objects.equals(extractorAttributes, other.extractorAttributes)
        && Objects.equals(processorAttributes, other.processorAttributes)
        && Objects.equals(connectorAttributes, other.connectorAttributes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("ifNotExistsCondition", ifNotExistsCondition)
        .add("extractorAttributes", extractorAttributes)
        .add("processorAttributes", processorAttributes)
        .add("connectorAttributes", connectorAttributes)
        .toString();
  }
}
