package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

import java.util.ArrayList;
import java.util.List;

public class IfStatement implements Statement {

  private ExpressionNode condition;

  private final List<Statement> ifBody;

  private boolean haveElse;

  private List<Statement> elseBody;

  public IfStatement() {
    haveElse = false;
    ifBody = new ArrayList<>();
  }

  public void setHaveElse(boolean haveElse) {
    if (haveElse && elseBody == null) {
      elseBody = new ArrayList<>();
    }
    this.haveElse = haveElse;
  }

  public boolean isHaveElse() {
    return haveElse;
  }

  public void addIfBodyStatement(Statement statement) {
    ifBody.add(statement);
  }

  public void addElseBodyStatement(Statement statement) {
    if (!isHaveElse()) {
      // TODO: throw exception
      return;
    }
    elseBody.add(statement);
  }

  public void setCondition(ExpressionNode condition) {
    this.condition = condition;
  }

  @Override
  public String toCode() {
    StringBuilder ifCode = new StringBuilder();
    ifCode.append("if(").append(condition.toCode()).append("){\n");
    for (Statement s : ifBody) {
      ifCode.append(s.toCode());
    }

    if (isHaveElse()) {
      ifCode.append("} else {\n");
      for (Statement s : elseBody) {
        ifCode.append(s.toCode());
      }
    }

    ifCode.append("}\n");
    return ifCode.toString();
  }
}
