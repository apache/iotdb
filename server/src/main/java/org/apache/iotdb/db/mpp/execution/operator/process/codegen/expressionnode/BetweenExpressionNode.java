package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class BetweenExpressionNode extends ExpressionNodeImpl {
  private final ExpressionNode firstNode;
  private final ExpressionNode secondNode;
  private final ExpressionNode thirdNode;
  private final boolean isNotBetween;

  public BetweenExpressionNode(
      String nodeName,
      ExpressionNode firstNode,
      ExpressionNode secondNode,
      ExpressionNode thirdNode,
      boolean isNotBetween) {
    this.firstNode = firstNode;
    this.secondNode = secondNode;
    this.thirdNode = thirdNode;
    this.nodeName = nodeName;
    this.isNotBetween = isNotBetween;
  }

  @Override
  public String toCode() {
    StringBuilder betweenCode = new StringBuilder();
    betweenCode
        .append(firstNode.getNodeName())
        .append(">=")
        .append(secondNode.getNodeName())
        .append("&&")
        .append(firstNode.getNodeName())
        .append("<=")
        .append(thirdNode.getNodeName());
    return isNotBetween ? "!" + bracket(betweenCode.toString()) : betweenCode.toString();
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new BinaryExpressionNode(
        "&&",
        new IsNullExpressionNode(firstNode, false),
        new BinaryExpressionNode(
            "&&",
            new IsNullExpressionNode(secondNode, false),
            new IsNullExpressionNode(thirdNode, false)));
  }

  @Override
  public String toSingleRowCode() {
    StringBuilder betweenCode = new StringBuilder();
    betweenCode
        .append(firstNode.toSingleRowCode())
        .append(">=")
        .append(secondNode.toSingleRowCode())
        .append("&&")
        .append(firstNode.toSingleRowCode())
        .append("<=")
        .append(thirdNode.toSingleRowCode());
    return isNotBetween ? "!" + bracket(betweenCode.toString()) : betweenCode.toString();
  }
}
