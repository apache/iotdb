package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class StringDeclareStatement extends DeclareStatement {
  public StringDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.STRING;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
