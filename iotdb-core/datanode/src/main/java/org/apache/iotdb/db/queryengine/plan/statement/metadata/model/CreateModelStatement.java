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
  private boolean isAuto;
  private Map<String, String> attributes;
  private QueryStatement queryStatement;

  public CreateModelStatement() {
    // do nothing
  }

  public String getModelId() {
    return modelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public boolean isAuto() {
    return isAuto;
  }

  public void setAuto(boolean auto) {
    isAuto = auto;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public QueryStatement getQueryStatement() {
    return queryStatement;
  }

  public void setQueryStatement(QueryStatement queryStatement) {
    this.queryStatement = queryStatement;
  }

  public ModelTask getModelTask() {
    return ModelTask.valueOf(attributes.get("model_task").toUpperCase());
  }

  public String getModelType() {
    return attributes.get("model_type");
  }

  public void semanticCheck() {
    if (!attributes.containsKey("model_task")) {
      throw new SemanticException("The attribute `model_task` must be specified.");
    }
    if (!attributes.containsKey("model_type")) {
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
