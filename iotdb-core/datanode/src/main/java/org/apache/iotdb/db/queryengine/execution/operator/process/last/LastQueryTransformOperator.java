package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.commons.exception.runtime.UnSupportedDataTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class LastQueryTransformOperator implements ProcessOperator {

  private String viewPath;

  private String dataType;

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private int currentIndex;

  private TsBlockBuilder tsBlockBuilder;

  public LastQueryTransformOperator(
      String viewPath, String dataType, OperatorContext operatorContext, List<Operator> children) {
    this.viewPath = viewPath;
    this.dataType = dataType;
    this.operatorContext = operatorContext;
    this.children = children;
    this.currentIndex = 0;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder(0);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex < 1) {
      List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
      for (int i = currentIndex; i < 1; i++) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
      return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
    } else {
      return Futures.immediateVoidFuture();
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (currentIndex >= 1) {
      TsBlock res = tsBlockBuilder.build();
      tsBlockBuilder.reset();
      return res;
    }

    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    int endIndex = 1;

    while ((System.nanoTime() - start < maxRuntime)
        && (currentIndex < endIndex)
        && !tsBlockBuilder.isFull()) {
      if (children.get(currentIndex).hasNextWithTimer()) {
        TsBlock tsBlock = children.get(currentIndex).nextWithTimer();
        if (tsBlock == null) {
          return null;
        } else if (!tsBlock.isEmpty()) {
          LastQueryUtil.appendLastValue(
              tsBlockBuilder,
              tsBlock.getColumn(0).getLong(0),
              viewPath,
              getValue(tsBlock.getColumn(1)),
              dataType);
        }
      } else {
        children.get(currentIndex).close();
        children.set(currentIndex, null);
      }

      currentIndex++;
    }

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    return currentIndex < 1;
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemory());
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateRetainedSizeAfterCallingNext());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnMemory = 0;
    for (Operator child : children) {
      maxReturnMemory = Math.max(maxReturnMemory, child.calculateMaxReturnSize());
    }
    return maxReturnMemory;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long sum = 0;
    for (Operator operator : children) {
      sum += operator.calculateRetainedSizeAfterCallingNext();
    }
    return sum;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      if (child != null) {
        child.close();
      }
    }
    tsBlockBuilder = null;
  }

  private String getValue(Column column) {
    switch (column.getDataType()) {
      case BOOLEAN:
        return String.valueOf(column.getBoolean(0));
      case INT32:
        return String.valueOf(column.getInt(0));
      case INT64:
        return String.valueOf(column.getLong(0));
      case FLOAT:
        return String.valueOf(column.getFloat(0));
      case DOUBLE:
        return String.valueOf(column.getDouble(0));
      default:
        throw new UnSupportedDataTypeException(
            "UnSupported data type in last query : " + column.getDataType());
    }
  }
}
