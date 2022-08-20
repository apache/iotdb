package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class BoolDeclareStatement extends DeclareStatement {
  public BoolDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.BOOLEAN;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
