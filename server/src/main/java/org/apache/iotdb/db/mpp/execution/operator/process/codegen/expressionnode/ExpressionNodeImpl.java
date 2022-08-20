package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public abstract class ExpressionNodeImpl implements ExpressionNode {

  protected String nodeName;

  @Override
  public String getNodeName() {
    return nodeName;
  }

  @Override
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  protected String bracket(String code) {
    return "(" + code + ")";
  }
}
