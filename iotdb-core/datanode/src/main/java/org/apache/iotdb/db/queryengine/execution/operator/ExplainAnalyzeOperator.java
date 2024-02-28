package org.apache.iotdb.db.queryengine.execution.operator;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExplainAnalyzeOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator child;
  private final boolean verbose;
  private boolean outputResult = false;
  private final List<FragmentInstance> instances;
  private final FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer =
      new FragmentInstanceStatisticsDrawer();

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
    TsBlock result = buildAnalyzeResult();
    outputResult = true;
    return result;
  }

  private TsBlock buildAnalyzeResult() throws FragmentInstanceFetchException {

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics =
        QueryStatisticsFetcher.fetchAllStatistics(instances);
    List<StatisticLine> analyzeResult =
        fragmentInstanceStatisticsDrawer.renderFragmentInstances(instances, allStatistics, verbose);

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);

    for (StatisticLine line : analyzeResult) {

      StringBuilder sb = new StringBuilder();
      sb.append(line.getValue());
      for (int i = 0;
          i < fragmentInstanceStatisticsDrawer.getMaxLineLength() - line.getValue().length();
          i++) {
        sb.append(" ");
      }

      timeColumnBuilder.writeLong(0);
      columnBuilder.writeBinary(new Binary(sb.toString().getBytes()));
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
