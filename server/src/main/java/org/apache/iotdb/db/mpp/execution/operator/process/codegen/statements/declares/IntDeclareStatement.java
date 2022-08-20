package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class IntDeclareStatement extends DeclareStatement {
  public IntDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.INT;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
