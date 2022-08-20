package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class ConstantExpressionNode extends ExpressionNodeImpl {

  public ConstantExpressionNode(String varName) {
    this.nodeName = varName;
  }

  @Override
  public String toCode() {
    return nodeName;
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new ConstantExpressionNode("true");
  }

  @Override
  public String toSingleRowCode() {
    return nodeName;
  }
}
