package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NewObjectsStatement extends DeclareStatement {
  private final List<ExpressionNode> retValues;

  private final int size;

  public NewObjectsStatement(String varName, int size) {
    this.varName = varName;
    this.size = size;
    this.retValues = new ArrayList<>();
  }

  public void addRetValue(ExpressionNode expressionNode) {
    retValues.add(expressionNode);
  }

  @Override
  public String toCode() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("Object[] ")
        .append(varName)
        .append(" = ")
        .append("new Object[ " + size + " ];\n");
    for (int i = 0; i < retValues.size(); ++i) {
      if (!Objects.isNull(retValues.get(i))) {
        stringBuilder
            .append(varName)
            .append("[ " + i + " ] = ")
            .append(retValues.get(i).getNodeName())
            .append(";\n");
      }
    }
    return stringBuilder.toString();
  }
}
