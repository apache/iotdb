package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class IsNullExpressionNode extends ExpressionNodeImpl {
  private ExpressionNode subExpression;

  private boolean isNull;

  private IsNullExpressionNode(String nodeName, ExpressionNode subExpression, boolean isNull) {
    this.isNull = isNull;
    this.nodeName = nodeName;
    this.subExpression = subExpression;
  }

  // subExpressionNode of IsNullExpressionNode should always have a name
  public IsNullExpressionNode(ExpressionNode subExpression, boolean isNull) {
    this.nodeName = null;
    this.subExpression = subExpression;
    this.isNull = isNull;
  }

  @Override
  public String toCode() {
    String op = isNull ? "==" : "!=";
    if (subExpression.getNodeName() != null) {
      return subExpression.getNodeName() + " " + op + " null ";
    }
    // this should not happen, and may be meaningless
    return bracket(subExpression.toCode()) + " " + op + " null";
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new ConstantExpressionNode("true");
  }

  @Override
  public String toSingleRowCode() {
    String op = isNull ? "==" : "!=";
    return subExpression.toSingleRowCode() + " " + op + " null ";
  }
}
