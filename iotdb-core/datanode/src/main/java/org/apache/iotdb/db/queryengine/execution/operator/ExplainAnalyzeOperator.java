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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsDrawer;
import org.apache.iotdb.db.queryengine.statistics.QueryStatisticsFetcher;
import org.apache.iotdb.db.queryengine.statistics.StatisticLine;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ExplainAnalyzeOperator implements ProcessOperator {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.EXPLAIN_ANALYZE_LOGGER_NAME);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExplainAnalyzeOperator.class);
  private static final String LOG_TITLE =
      "---------------------Intermediate Results of EXPLAIN ANALYZE---------------------:";
  private final OperatorContext operatorContext;
  private final Operator child;
  private final boolean verbose;
  private boolean outputResult = false;
  private final List<FragmentInstance> instances;

  private final FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer =
      new FragmentInstanceStatisticsDrawer();

  private final ScheduledFuture<?> logRecordTask;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;
  private final MPPQueryContext mppQueryContext;

  public ExplainAnalyzeOperator(
      OperatorContext operatorContext,
      Operator child,
      long queryId,
      boolean verbose,
      long timeout) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.verbose = verbose;
    Coordinator coordinator = Coordinator.getInstance();

    this.clientManager = coordinator.getInternalServiceClientManager();

    QueryExecution queryExecution = (QueryExecution) coordinator.getQueryExecution(queryId);
    this.instances = queryExecution.getDistributedPlan().getInstances();
    mppQueryContext = queryExecution.getContext();
    fragmentInstanceStatisticsDrawer.renderPlanStatistics(mppQueryContext);

    // The time interval guarantees the result of EXPLAIN ANALYZE will be printed at least three
    // times.
    // And the maximum time interval is 15s.
    long logIntervalInMs = Math.min(timeout / 3, 15000);
    this.logRecordTask =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            queryExecution.getScheduledExecutor(),
            this::logIntermediateResultIfTimeout,
            logIntervalInMs,
            logIntervalInMs,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (child.hasNextWithTimer()) {
      child.nextWithTimer();
      return null;
    }

    fragmentInstanceStatisticsDrawer.renderDispatchCost(mppQueryContext);

    // fetch statics from all fragment instances
    TsBlock result = buildResult();
    outputResult = true;
    return result;
  }

  private List<String> buildFragmentInstanceStatistics(
      List<FragmentInstance> instances, boolean verbose) throws FragmentInstanceFetchException {

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics =
        QueryStatisticsFetcher.fetchAllStatistics(instances, clientManager);
    List<StatisticLine> statisticLines =
        fragmentInstanceStatisticsDrawer.renderFragmentInstances(instances, allStatistics, verbose);

    List<String> analyzeResult = new ArrayList<>();
    for (StatisticLine line : statisticLines) {
      StringBuilder sb = new StringBuilder();
      sb.append(line.getValue());
      for (int i = 0;
          i < fragmentInstanceStatisticsDrawer.getMaxLineLength() - line.getValue().length();
          i++) {
        sb.append(" ");
      }
      analyzeResult.add(sb.toString());
    }
    return analyzeResult;
  }

  // We will log the intermediate result of analyze if timeout
  // It can be used to analyze deadlock problem.
  private void logIntermediateResultIfTimeout() {
    try (SetThreadName ignored =
        new SetThreadName(
            String.format(
                "%s-Explain-Analyze-Logger",
                operatorContext.getInstanceContext().getId().getQueryId()))) {
      List<String> analyzeResult = buildFragmentInstanceStatistics(instances, verbose);

      StringBuilder logContent = new StringBuilder();
      logContent.append("\n").append(LOG_TITLE).append("\n");
      for (String line : analyzeResult) {
        logContent.append(line).append("\n");
      }
      String res = logContent.toString();
      logger.info(res);
    } catch (Exception e) {
      logger.error("Error occurred when logging intermediate result of analyze.", e);
    }
  }

  private TsBlock buildResult() throws FragmentInstanceFetchException {

    List<String> analyzeResult = buildFragmentInstanceStatistics(instances, verbose);

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);

    for (String line : analyzeResult) {
      timeColumnBuilder.writeLong(0);
      columnBuilder.writeBinary(new Binary(line.getBytes()));
      builder.declarePosition();
    }
    return builder.build();
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNext() || !outputResult;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public void close() throws Exception {
    child.close();

    if (logRecordTask != null) {
      boolean cancelResult = logRecordTask.cancel(true);
      if (!cancelResult) {
        logger.debug("cancel state tracking task failed. {}", logRecordTask.isCancelled());
      }
    } else {
      logger.debug("trackTask not started");
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !child.hasNext() && outputResult;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
