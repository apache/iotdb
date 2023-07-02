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

import org.apache.iotdb.common.rpc.thrift.ModelTask;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CreateModelStatement extends Statement implements IConfigStatement {

  private String modelId;

  private Map<String, String> modelOptions;

  private Map<String, String> hyperParameters;

  private QueryStatement queryBody;

  public CreateModelStatement() {
    // do nothing
  }

  public String getModelId() {
    return modelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public Map<String, String> getModelOptions() {
    return modelOptions;
  }

  public void setModelOptions(Map<String, String> modelOptions) {
    this.modelOptions = modelOptions;
  }

  public Map<String, String> getHyperParameters() {
    return hyperParameters;
  }

  public void setHyperParameters(Map<String, String> hyperParameters) {
    this.hyperParameters = hyperParameters;
  }

  public QueryStatement getQueryBody() {
    return queryBody;
  }

  public void setQueryBody(QueryStatement queryBody) {
    this.queryBody = queryBody;
  }

  public ModelTask getModelTask() {
    return ModelTask.valueOf(modelOptions.get("model_task").toUpperCase());
  }

  public String getModelType() {
    return modelOptions.get("model_type");
  }

  public void semanticCheck() {
    if (!modelOptions.containsKey("model_task")) {
      throw new SemanticException("The attribute `model_task` must be specified.");
    }
    if (!modelOptions.containsKey("model_type")) {
      throw new SemanticException("The attribute `model_type` must be specified.");
    }
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateModel(this, context);
  }
}
