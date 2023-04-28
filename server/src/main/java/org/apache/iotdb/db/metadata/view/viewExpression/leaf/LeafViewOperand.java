package org.apache.iotdb.db.metadata.view.viewExpression.leaf;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

import java.util.ArrayList;
import java.util.List;

public abstract class LeafViewOperand extends ViewExpression {

  @Override
  protected final boolean isLeafOperandInternal() {
    return true;
  }

  @Override
  public final List<ViewExpression> getChildViewExpressions() {
    // leaf node has no child nodes.
    return new ArrayList<>();
  }
}
