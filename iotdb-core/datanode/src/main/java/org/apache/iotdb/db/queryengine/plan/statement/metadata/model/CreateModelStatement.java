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

import org.apache.iotdb.common.rpc.thrift.TaskType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.model.ModelInformation.MODEL_TYPE;
import static org.apache.iotdb.commons.model.ModelInformation.TASK_TYPE;

public class CreateModelStatement extends Statement implements IConfigStatement {

  private String modelId;

  private Map<String, String> options;

  private Map<String, String> hyperparameters;

  private QueryStatement datasetStatement;

  private final List<String> requiredOptions = Arrays.asList(TASK_TYPE, MODEL_TYPE);

  public CreateModelStatement() {
    // do nothing
  }

  public String getModelId() {
    return modelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  public Map<String, String> getHyperparameters() {
    return hyperparameters;
  }

  public void setHyperparameters(Map<String, String> hyperparameters) {
    this.hyperparameters = hyperparameters;
  }

  public QueryStatement getDatasetStatement() {
    return datasetStatement;
  }

  public void setDatasetStatement(QueryStatement queryBody) {
    this.datasetStatement = queryBody;
  }

  public TaskType getTaskType() {
    try {
      return TaskType.valueOf(options.get("task_type").toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SemanticException("Unknown task type: " + options.get("task_type"));
    }
  }

  public void semanticCheck() {
    for (String requiredOption : requiredOptions) {
      if (!options.containsKey(requiredOption)) {
        throw new SemanticException("The option `" + requiredOption + "` must be specified.");
      }
    }
    if (datasetStatement.isAlignByDevice()) {
      throw new SemanticException("");
    }
    if (datasetStatement.isLastQuery()) {
      throw new SemanticException("");
    }
    if (datasetStatement.isAggregationQuery() && !datasetStatement.isGroupByTime()) {
      throw new SemanticException("");
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
