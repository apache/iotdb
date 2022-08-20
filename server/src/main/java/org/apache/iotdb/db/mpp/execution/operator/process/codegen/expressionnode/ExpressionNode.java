package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public interface ExpressionNode {
  String getNodeName();

  void setNodeName(String nodeName);

  // each node has a name will be replaced by its name
  // the nodes without name will call toCode()
  String toCode();

  ExpressionNode checkWhetherNotNull();

  String toSingleRowCode();
}
