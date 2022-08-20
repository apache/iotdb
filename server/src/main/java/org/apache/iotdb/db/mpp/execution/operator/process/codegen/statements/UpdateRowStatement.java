package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class is used to help create UDTF input row. It just update a row but never create a new one
 */
public class UpdateRowStatement implements Statement {
  private List<ExpressionNode> columns;

  private final String varName;

  public UpdateRowStatement(String varName) {
    this.varName = varName;
  }

  public void addData(ExpressionNode ExpressionNode) {
    if (Objects.isNull(columns)) {
      columns = new ArrayList<>();
    }
    columns.add(ExpressionNode);
  }

  public void setColumns(List<ExpressionNode> columns) {
    this.columns = columns;
  }

  @Override
  public String toCode() {
    StringBuilder newRow = new StringBuilder();
    newRow.append(varName).append(".setData(timestamp");
    for (ExpressionNode ExpressionNode : columns) {
      newRow.append(", ").append(ExpressionNode.getNodeName());
    }
    newRow.append(");\n");
    return newRow.toString();
  }
}
