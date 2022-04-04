package org.apache.iotdb.db.mpp.operator.meta;

import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.List;

public class MetaMergeOperator implements ProcessOperator {

  protected OperatorContext operatorContext;
  protected int limit;
  protected int offset;
  private final boolean[] noMoreTsBlocks;
  private int count;

  private List<Operator> children;

  public MetaMergeOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    noMoreTsBlocks = new boolean[children.size()];
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws IOException {
    // ToDo consider SHOW LATEST

    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        TsBlock tsBlock = children.get(i).next();
        if (!children.get(i).hasNext()) {
          noMoreTsBlocks[i] = true;
        }
        return tsBlock;
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i] && children.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }
}
