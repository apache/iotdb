package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class DoubleDeclareStatement extends DeclareStatement {
  public DoubleDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.DOUBLE;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
