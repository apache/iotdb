package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.Operator;

public class StartPipeServerOperator extends Operator {
  protected StartPipeServerOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.START_OPERATOR;
  }
}
