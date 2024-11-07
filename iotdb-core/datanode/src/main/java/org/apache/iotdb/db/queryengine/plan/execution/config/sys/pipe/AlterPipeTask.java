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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

public class AlterPipeTask implements IConfigTask {

  private final AlterPipeStatement alterPipeStatement;

  public AlterPipeTask(AlterPipeStatement alterPipeStatement) {
    // support now() function
    applyNowFunctionToExtractorAttributes(alterPipeStatement.getExtractorAttributes());
    this.alterPipeStatement = alterPipeStatement;
  }

  public AlterPipeTask(AlterPipe node) {
    alterPipeStatement = new AlterPipeStatement(StatementType.ALTER_PIPE);
    alterPipeStatement.setPipeName(node.getPipeName());
    alterPipeStatement.setIfExists(node.hasIfExistsCondition());

    // support now() function
    applyNowFunctionToExtractorAttributes(node.getExtractorAttributes());

    alterPipeStatement.setExtractorAttributes(node.getExtractorAttributes());
    alterPipeStatement.setProcessorAttributes(node.getProcessorAttributes());
    alterPipeStatement.setConnectorAttributes(node.getConnectorAttributes());
    alterPipeStatement.setReplaceAllExtractorAttributes(node.isReplaceAllExtractorAttributes());
    alterPipeStatement.setReplaceAllProcessorAttributes(node.isReplaceAllProcessorAttributes());
    alterPipeStatement.setReplaceAllConnectorAttributes(node.isReplaceAllConnectorAttributes());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.alterPipe(alterPipeStatement);
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
