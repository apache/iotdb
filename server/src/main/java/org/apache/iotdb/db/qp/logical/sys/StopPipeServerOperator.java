package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.Operator;

public class StopPipeServerOperator extends Operator {
  protected StopPipeServerOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.STOP_OPERATOR;
  }
}
