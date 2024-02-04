package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

public class ExplainAnalyzeOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator child;
  private final boolean verbose;
  private boolean outputResult = false;

  public ExplainAnalyzeOperator(OperatorContext operatorContext, Operator child, boolean verbose) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.verbose = verbose;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (child.hasNext()) {
      child.next();
      return null;
    }

    Coordinator coordinator = Coordinator.getInstance();
    IQueryExecution queryExecution =
        coordinator.getQueryExecution(operatorContext.getInstanceContext().getId().getQueryId());
    return null;
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
