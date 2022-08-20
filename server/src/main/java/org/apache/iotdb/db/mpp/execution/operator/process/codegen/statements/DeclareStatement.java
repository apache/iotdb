package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.CodegenDataType;

public abstract class DeclareStatement implements Statement {

  // not only declare a new variable
  // this class will also check whether current value is null
  protected String varName;

  protected ExpressionNode es;

  protected CodegenDataType type;

  protected IfStatement ifNull;

  protected void generateNullCheck() {
    ifNull = new IfStatement();
    ifNull.setHaveElse(false);
    ifNull.setCondition(es.checkWhetherNotNull());
    ifNull.addIfBodyStatement(new AssignmentStatement(varName, es, type));
  }

  @Override
  public String toCode() {
    StringBuilder newStatement = new StringBuilder();
    newStatement.append(type).append(" ").append(varName).append(" = null;\n");

    newStatement.append(ifNull.toCode());
    return newStatement.toString();
  }
}
