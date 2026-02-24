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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShowQueriesOperator implements SourceOperator {

  private final OperatorContext operatorContext;

  private final PlanNodeId sourceId;

  private TsBlock tsBlock;
  private boolean hasConsumed;

  private final Coordinator coordinator;
  private final String allowedUsername;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowQueriesOperator.class);

  public ShowQueriesOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Coordinator coordinator,
      String allowedUsername) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.coordinator = coordinator;
    this.allowedUsername = allowedUsername;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock res = tsBlock;
    hasConsumed = true;
    tsBlock = null;
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (hasConsumed) {
      return false;
    }
    if (tsBlock == null) {
      tsBlock = buildTsBlock();
    }
    return true;
  }

  @Override
  public boolean isFinished() throws Exception {
    return hasConsumed;
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public long calculateMaxPeekMemory() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private TsBlock buildTsBlock() {
    List<TSDataType> outputDataTypes =
        DatasetHeaderFactory.getShowQueriesHeader().getRespDataTypes();
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    List<IQueryExecution> queryExecutions = coordinator.getAllQueryExecutions();

    if (!queryExecutions.isEmpty()) {
      TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
      ColumnBuilder[] columnBuilders = builder.getValueColumnBuilders();
      long currTime = System.currentTimeMillis();
      String[] splits = queryExecutions.get(0).getQueryId().split("_");
      int dataNodeId = Integer.parseInt(splits[splits.length - 1]);

      for (IQueryExecution queryExecution : queryExecutions) {
        if (allowedUsername != null && !allowedUsername.equals(queryExecution.getUser())) {
          continue;
        }
        if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TREE)) {
          timeColumnBuilder.writeLong(
              TimestampPrecisionUtils.convertToCurrPrecision(
                  queryExecution.getStartExecutionTime(), TimeUnit.MILLISECONDS));
          columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
          columnBuilders[1].writeInt(dataNodeId);
          columnBuilders[2].writeFloat(
              (float) (currTime - queryExecution.getStartExecutionTime()) / 1000);
          columnBuilders[3].writeBinary(
              BytesUtils.valueOf(queryExecution.getExecuteSQL().orElse("UNKNOWN")));
          builder.declarePosition();
        }
      }
    }
    return builder.build();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId);
  }
}
