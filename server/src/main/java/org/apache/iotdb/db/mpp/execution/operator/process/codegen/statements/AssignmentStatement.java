package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.CodegenDataType;

public class AssignmentStatement implements Statement {
  private final String varName;

  private final ExpressionNode es;

  private final CodegenDataType type;

  public AssignmentStatement(String varName, ExpressionNode es, CodegenDataType type) {
    this.varName = varName;
    this.es = es;
    this.type = type;
  }

  @Override
  public String toCode() {
    return varName + " = " + type + ".valueOf(" + es.toCode() + ");\n";
  }
}
