package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class UnaryExpressionNode extends ExpressionNodeImpl {
  private final ExpressionNode subNode;

  private final String op;

  public UnaryExpressionNode(String nodeName, ExpressionNode subNode, String op) {
    this.nodeName = nodeName;
    this.subNode = subNode;
    this.op = op;
  }

  @Override
  public String toCode() {
    if (subNode.getNodeName() != null) {
      return op + subNode.getNodeName();
    }
    return op + bracket(subNode.toCode());
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new IsNullExpressionNode(subNode, false);
  }

  @Override
  public String toSingleRowCode() {
    return op + bracket(subNode.toSingleRowCode());
  }
}
