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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

public class CreatePipeTask implements IConfigTask {

  private final CreatePipeStatement createPipeStatement;

  public CreatePipeTask(CreatePipeStatement createPipeStatement) {
    // support now() function
    applyNowFunctionToExtractorAttributes(createPipeStatement.getExtractorAttributes());
    this.createPipeStatement = createPipeStatement;
  }

  public CreatePipeTask(CreatePipe createPipe) {
    createPipeStatement = new CreatePipeStatement(StatementType.CREATE_PIPE);
    createPipeStatement.setPipeName(createPipe.getPipeName());
    createPipeStatement.setIfNotExists(createPipe.hasIfNotExistsCondition());

    // support now() function
    applyNowFunctionToExtractorAttributes(createPipe.getExtractorAttributes());

    createPipeStatement.setExtractorAttributes(createPipe.getExtractorAttributes());
    createPipeStatement.setProcessorAttributes(createPipe.getProcessorAttributes());
    createPipeStatement.setConnectorAttributes(createPipe.getConnectorAttributes());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createPipe(createPipeStatement);
  }

  private void applyNowFunctionToExtractorAttributes(final Map<String, String> attributes) {
    final long currentTime =
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            System.currentTimeMillis(),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision());

    // support now() function
    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeExtractorConstant.SOURCE_START_TIME_KEY,
        PipeExtractorConstant.EXTRACTOR_START_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeExtractorConstant.SOURCE_END_TIME_KEY,
        PipeExtractorConstant.EXTRACTOR_END_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY,
        PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY,
        PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY,
        currentTime);
  }
}
