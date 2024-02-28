package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsDrawer;
import org.apache.iotdb.db.queryengine.statistics.QueryStatisticsFetcher;
import org.apache.iotdb.db.queryengine.statistics.StatisticLine;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ExplainAnalyzeOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator child;
  private final boolean verbose;
  private boolean outputResult = false;
  private final List<FragmentInstance> instances;
  private final long LOG_INTERNAL_IN_MS = 10000;
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.EXPLAIN_ANALYZE_LOGGER_NAME);
  private final FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer =
      new FragmentInstanceStatisticsDrawer();

  private final ScheduledFuture<?> logRecordTask;

  public ExplainAnalyzeOperator(OperatorContext operatorContext, Operator child, boolean verbose) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.verbose = verbose;
    QueryExecution queryExecution =
        (QueryExecution)
            Coordinator.getInstance()
                .getQueryExecution(operatorContext.getInstanceContext().getId().getQueryId());
    this.instances = queryExecution.getDistributedPlan().getInstances();
    fragmentInstanceStatisticsDrawer.renderPlanStatistics(queryExecution.getContext());
    this.logRecordTask =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            queryExecution.getScheduledExecutor(),
            this::logIntermediateResultIfTimeout,
            LOG_INTERNAL_IN_MS,
            LOG_INTERNAL_IN_MS,
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

    // fetch statics from all fragment instances
    TsBlock result = buildResult();
    outputResult = true;
    return result;
  }

  private List<String> buildFragmentInstanceStatistics(
      List<FragmentInstance> instances, boolean verbose) throws FragmentInstanceFetchException {

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics =
        QueryStatisticsFetcher.fetchAllStatistics(instances);
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
    try {
      List<String> analyzeResult = buildFragmentInstanceStatistics(instances, verbose);

      StringBuilder logContent = new StringBuilder();
      logContent.append("\n").append("Intermediate result of EXPLAIN ANALYZE:").append("\n");
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
}
