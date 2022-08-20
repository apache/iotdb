package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class LeafExpressionNode extends ExpressionNodeImpl {

  public LeafExpressionNode(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public String toCode() {
    return getNodeName();
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new IsNullExpressionNode(this, false);
  }

  @Override
  public String toSingleRowCode() {
    return toCode();
  }
}
