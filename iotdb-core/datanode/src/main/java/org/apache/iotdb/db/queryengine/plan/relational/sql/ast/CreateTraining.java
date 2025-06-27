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
  public int hashCode() {
    return Objects.hash(modelId, targetSql, existingModelId, parameters);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CreateTraining)) {
      return false;
    }
    CreateTraining createTraining = (CreateTraining) obj;
    return modelId.equals(createTraining.modelId)
        && Objects.equals(existingModelId, createTraining.existingModelId)
        && Objects.equals(parameters, createTraining.parameters)
        && Objects.equals(targetSql, createTraining.targetSql);
  }

  @Override
  public String toString() {
    return "CreateTraining{"
        + "modelId='"
        + modelId
        + '\''
        + ", parameters="
        + parameters
        + ", existingModelId='"
        + existingModelId
        + '\''
        + ", targetSql='"
        + targetSql
        + '}';
  }
}
