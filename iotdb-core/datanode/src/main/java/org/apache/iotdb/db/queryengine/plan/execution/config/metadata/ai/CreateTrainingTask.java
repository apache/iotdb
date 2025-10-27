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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;

public class CreateTrainingTask implements IConfigTask {

  private final String modelId;
  private final boolean isTableModel;
  private final Map<String, String> parameters;

  private final String existingModelId;

  // Data schema for table model
  private String targetSql = null;
  // Data schema for tree model
  private List<String> targetPaths;
  private List<List<Long>> timeRanges;

  // For table model
  public CreateTrainingTask(
      String modelId, Map<String, String> parameters, String existingModelId, String targetSql) {
    this.modelId = modelId;
    this.parameters = parameters;
    this.existingModelId = existingModelId;
    this.targetSql = targetSql;
    this.isTableModel = true;
  }

  // For tree model
  public CreateTrainingTask(
      String modelId,
      Map<String, String> parameters,
      List<List<Long>> timeRanges,
      String existingModelId,
      List<String> targetPaths) {
    this.modelId = modelId;
    this.parameters = parameters;
    this.timeRanges = timeRanges;
    this.existingModelId = existingModelId;

    this.isTableModel = false;
    this.targetPaths = targetPaths;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createTraining(
        modelId, isTableModel, parameters, timeRanges, existingModelId, targetSql, targetPaths);
  }
}
