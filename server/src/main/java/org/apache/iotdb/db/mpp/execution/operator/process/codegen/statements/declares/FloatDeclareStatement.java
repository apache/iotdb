package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class FloatDeclareStatement extends DeclareStatement {
  public FloatDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.FLOAT;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
