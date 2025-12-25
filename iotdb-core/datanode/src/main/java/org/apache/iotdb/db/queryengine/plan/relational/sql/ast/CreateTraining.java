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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateTraining extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreateTraining.class);

  private final String modelId;
  private final String targetSql;

  private Map<String, String> parameters;
  private String existingModelId = null;

  public CreateTraining(String modelId, String targetSql) {
    super(null);
    this.modelId = modelId;
    this.targetSql = targetSql;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTraining(this, context);
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public void setExistingModelId(String existingModelId) {
    this.existingModelId = existingModelId;
  }

  public String getModelId() {
    return modelId;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getExistingModelId() {
    return existingModelId;
  }

  public String getTargetSql() {
    return targetSql;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateTraining that = (CreateTraining) o;
    return Objects.equals(modelId, that.modelId)
        && Objects.equals(targetSql, that.targetSql)
        && Objects.equals(parameters, that.parameters)
        && Objects.equals(existingModelId, that.existingModelId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(modelId, targetSql, parameters, existingModelId);
  }

  @Override
  public String toString() {
    return "CreateTraining{"
        + "modelId='"
        + modelId
        + '\''
        + ", targetSql='"
        + targetSql
        + '\''
        + ", parameters="
        + parameters
        + ", existingModelId='"
        + existingModelId
        + '\''
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(modelId);
    size += RamUsageEstimator.sizeOf(targetSql);
    size += RamUsageEstimator.sizeOf(existingModelId);
    size += RamUsageEstimator.sizeOfMap(parameters);
    return size;
  }
}
