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

import org.apache.iotdb.db.queryengine.plan.relational.type.ModelType;

import java.util.List;
import java.util.Map;

public class CreateModel extends Statement {

  String modelId;
  String curDatabase;
  ModelType modelType;

  Map<String, String> parameters;
  String existingModelId = null;

  List<Table> targetTables;
  List<String> targetDbs;
  boolean useAllData = false;

  public CreateModel(String modelId, ModelType modelType) {
    super(null);
    this.modelId = modelId;
    this.modelType = modelType;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateModel(this, context);
  }

  public void setCurDatabase(String curDatabase) {
    this.curDatabase = curDatabase;
  }

  public String getCurDatabase() {
    return curDatabase;
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

  public void setTargetTables(List<Table> targetTables) {
    this.targetTables = targetTables;
  }

  public void setUseAllData(boolean useAllData) {
    this.useAllData = useAllData;
  }

  public List<String> getTargetDbs() {
    return targetDbs;
  }

  public List<Table> getTargetTables() {
    return targetTables;
  }

  public String getModelId() {
    return modelId;
  }

  public ModelType getModelType() {
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

  @Override
  public List<? extends Node> getChildren() {
    return null;
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
}
