package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;

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

    DriverContext driverContext = operatorContext.getDriverContext();
    FragmentInstanceContext fragmentInstanceContext = driverContext.getFragmentInstanceContext();

    FragmentInstanceManager fragmentInstanceManager = FragmentInstanceManager.getInstance();
    Coordinator coordinator = Coordinator.getInstance();
    IQueryExecution queryExecution =
        coordinator.getQueryExecution(operatorContext.getInstanceContext().getQueryId());

    StringBuilder res = new StringBuilder();
    res.append(fragmentInstanceContext.getId())
        .append(fragmentInstanceContext.getInstanceInfo().getMessage())
        .append("\n");
    res.append("Total time: ")
        .append(System.currentTimeMillis() - fragmentInstanceContext.getStartTime())
        .append("ms\n");

    TsBlockBuilder blockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    blockBuilder.getTimeColumnBuilder().writeLong(0);
    blockBuilder.getValueColumnBuilders()[0].writeBinary(new Binary(res.toString().getBytes()));
    blockBuilder.declarePosition();
    outputResult = true;
    return blockBuilder.build();
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
