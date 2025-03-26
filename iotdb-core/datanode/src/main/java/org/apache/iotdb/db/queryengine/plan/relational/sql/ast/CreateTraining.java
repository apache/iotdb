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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateTraining extends Statement {

  private final String modelId;
  private final String modelType;

  private Map<String, String> parameters;
  private String existingModelId = null;

  private List<QualifiedName> targetTables;
  private List<String> targetDbs;

  private List<List<Long>> targetTimeRanges;
  private boolean useAllData = false;

  public CreateTraining(String modelId, String modelType) {
    super(null);
    this.modelId = modelId;
    this.modelType = modelType;
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

  public void setTargetDbs(List<String> targetDbs) {
    this.targetDbs = targetDbs;
  }

  public void setTargetTables(List<QualifiedName> targetTables) {
    this.targetTables = targetTables;
  }

  public void setUseAllData(boolean useAllData) {
    this.useAllData = useAllData;
  }

  public List<String> getTargetDbs() {
    return targetDbs;
  }

  public List<QualifiedName> getTargetTables() {
    return targetTables;
  }

  public String getModelId() {
    return modelId;
  }

  public String getModelType() {
    return modelType;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getExistingModelId() {
    return existingModelId;
  }

  public boolean isUseAllData() {
    return useAllData;
  }

  public void setTargetTimeRanges(List<List<Long>> targetTimeRanges) {
    this.targetTimeRanges = targetTimeRanges;
  }

  public List<List<Long>> getTargetTimeRanges() {
    return targetTimeRanges;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        modelId, modelType, existingModelId, parameters, targetTimeRanges, useAllData);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CreateTraining)) {
      return false;
    }
    CreateTraining createTraining = (CreateTraining) obj;
    return modelId.equals(createTraining.modelId)
        && modelType.equals(createTraining.modelType)
        && Objects.equals(existingModelId, createTraining.existingModelId)
        && Objects.equals(parameters, createTraining.parameters)
        && Objects.equals(targetTimeRanges, createTraining.targetTimeRanges)
        && useAllData == createTraining.useAllData;
  }

  @Override
  public String toString() {
    return "CreateTraining{"
        + "modelId='"
        + modelId
        + '\''
        + ", modelType='"
        + modelType
        + '\''
        + ", parameters="
        + parameters
        + ", existingModelId='"
        + existingModelId
        + '\''
        + ", targetTables="
        + targetTables
        + ", targetDbs="
        + targetDbs
        + ", targetTimeRanges="
        + targetTimeRanges
        + ", useAllData="
        + useAllData
        + '}';
  }
}
