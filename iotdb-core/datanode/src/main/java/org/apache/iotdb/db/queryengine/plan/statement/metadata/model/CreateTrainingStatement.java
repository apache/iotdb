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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.model;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.List;
import java.util.Map;

public class CreateTrainingStatement extends Statement implements IConfigStatement {

  String modelId;
  String modelType;

  Map<String, String> parameters;
  String existingModelId = null;

  List<PartialPath> targetPathPatterns;

  public CreateTrainingStatement(String modelId, String modelType) {
    this.modelId = modelId;
    this.modelType = modelType;
  }

  public void setTargetPathPatterns(List<PartialPath> targetPathPatterns) {
    this.targetPathPatterns = targetPathPatterns;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getExistingModelId() {
    return existingModelId;
  }

  public List<PartialPath> getTargetPathPatterns() {
    return targetPathPatterns;
  }

  public String getModelId() {
    return modelId;
  }

  public String getModelType() {
    return modelType;
  }

  public void setExistingModelId(String existingModelId) {
    this.existingModelId = existingModelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public void setModelType(String modelType) {
    this.modelType = modelType;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return null;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return targetPathPatterns;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTraining(this, context);
  }
}
