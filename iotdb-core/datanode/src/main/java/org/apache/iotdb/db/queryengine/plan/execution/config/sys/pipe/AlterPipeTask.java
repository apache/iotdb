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
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
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

  public AlterPipeTask(final AlterPipeStatement alterPipeStatement) {
    // support now() function
    applyNowFunctionToExtractorAttributes(alterPipeStatement.getSourceAttributes());
    this.alterPipeStatement = alterPipeStatement;
  }

  public AlterPipeTask(final AlterPipe node, final String userName) {
    alterPipeStatement = new AlterPipeStatement(StatementType.ALTER_PIPE);
    alterPipeStatement.setPipeName(node.getPipeName());
    alterPipeStatement.setIfExists(node.hasIfExistsCondition());

    // support now() function
    applyNowFunctionToExtractorAttributes(node.getExtractorAttributes());

    alterPipeStatement.setSourceAttributes(node.getExtractorAttributes());
    alterPipeStatement.setProcessorAttributes(node.getProcessorAttributes());
    alterPipeStatement.setSinkAttributes(node.getConnectorAttributes());
    alterPipeStatement.setReplaceAllSourceAttributes(node.isReplaceAllExtractorAttributes());
    alterPipeStatement.setReplaceAllProcessorAttributes(node.isReplaceAllProcessorAttributes());
    alterPipeStatement.setReplaceAllSinkAttributes(node.isReplaceAllConnectorAttributes());
    alterPipeStatement.setUserName(userName);

    alterPipeStatement.setTableModel(true);
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
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
        PipeSourceConstant.SOURCE_START_TIME_KEY,
        PipeSourceConstant.EXTRACTOR_START_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeSourceConstant.SOURCE_END_TIME_KEY,
        PipeSourceConstant.EXTRACTOR_END_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeSourceConstant.SOURCE_HISTORY_START_TIME_KEY,
        PipeSourceConstant.EXTRACTOR_HISTORY_START_TIME_KEY,
        currentTime);

    PipeFunctionSupport.applyNowFunctionToExtractorAttributes(
        attributes,
        PipeSourceConstant.SOURCE_HISTORY_END_TIME_KEY,
        PipeSourceConstant.EXTRACTOR_HISTORY_END_TIME_KEY,
        currentTime);
  }
}
