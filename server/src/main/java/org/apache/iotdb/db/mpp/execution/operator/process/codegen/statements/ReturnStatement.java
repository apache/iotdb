package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

public class ReturnStatement implements Statement {
  private ExpressionNode es;

  public ReturnStatement(ExpressionNode es) {
    this.es = es;
  }

  @Override
  public String toCode() {
    return "return " + es.toCode() + ";\n";
  }
}
