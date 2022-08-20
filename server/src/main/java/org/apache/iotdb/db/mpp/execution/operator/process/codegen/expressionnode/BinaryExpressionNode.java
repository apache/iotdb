package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class BinaryExpressionNode extends ExpressionNodeImpl {

  private final String op;

  private final ExpressionNode leftNode;

  private final ExpressionNode rightNode;

  public BinaryExpressionNode(
      String nodeName, String op, ExpressionNode leftNode, ExpressionNode rightNode) {
    this.nodeName = nodeName;
    this.op = op;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  public BinaryExpressionNode(String op, ExpressionNode leftNode, ExpressionNode rightNode) {
    this.nodeName = null;
    this.op = op;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  @Override
  public String toCode() {
    StringBuilder binaryExpressionCode = new StringBuilder();
    if (leftNode.getNodeName() != null) {
      binaryExpressionCode.append(leftNode.getNodeName());
    } else {
      binaryExpressionCode.append(bracket(leftNode.toCode()));
    }
    binaryExpressionCode.append(op);
    if (rightNode.getNodeName() != null) {
      binaryExpressionCode.append(rightNode.getNodeName());
    } else {
      binaryExpressionCode.append(bracket(rightNode.toCode()));
    }
    return binaryExpressionCode.toString();
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new BinaryExpressionNode(
        "&&",
        new IsNullExpressionNode(leftNode, false),
        new IsNullExpressionNode(rightNode, false));
  }

  @Override
  public String toSingleRowCode() {
    return bracket(leftNode.toSingleRowCode()) + op + bracket(rightNode.toSingleRowCode());
  }
}
