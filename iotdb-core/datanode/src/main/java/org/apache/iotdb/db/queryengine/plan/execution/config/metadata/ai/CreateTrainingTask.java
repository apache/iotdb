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
  private final String modelType;
  private final boolean isTableModel;
  private final Map<String, String> parameters;
  private final boolean useAllData;
  private final List<List<Long>> timeRanges;
  private final String existingModelId;

  // Data schema for table model
  private List<String> targetTables;
  private List<String> targetDbs;
  // Data schema for tree model
  private List<String> targetPaths;

  public CreateTrainingTask(
      String modelId,
      String modelType,
      Map<String, String> parameters,
      boolean useAllData,
      List<List<Long>> timeRanges,
      String existingModelId,
      List<String> targetTables,
      List<String> targetDbs) {
    if (!modelType.equalsIgnoreCase("timer_xl")) {
      throw new UnsupportedOperationException("Only TimerXL model is supported now.");
    }
    this.modelId = modelId;
    this.modelType = modelType;
    this.parameters = parameters;
    this.useAllData = useAllData;
    this.timeRanges = timeRanges;
    this.existingModelId = existingModelId;

    this.isTableModel = true;
    this.targetTables = targetTables;
    this.targetDbs = targetDbs;
  }

  public CreateTrainingTask(
      String modelId,
      String modelType,
      Map<String, String> parameters,
      boolean useAllData,
      List<List<Long>> timeRanges,
      String existingModelId,
      List<String> targetPaths) {
    if (!modelType.equalsIgnoreCase("timer_xl")) {
      throw new UnsupportedOperationException("Only TimerXL model is supported now.");
    }
    this.modelId = modelId;
    this.modelType = modelType;
    this.parameters = parameters;
    this.useAllData = useAllData;
    this.timeRanges = timeRanges;
    this.existingModelId = existingModelId;

    this.isTableModel = false;
    this.targetPaths = targetPaths;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createTraining(
        modelId,
        modelType,
        isTableModel,
        parameters,
        useAllData,
        timeRanges,
        existingModelId,
        targetTables,
        targetDbs,
        targetPaths);
  }
}
