package org.apache.iotdb.db.qp.logical.sys;

public class ShowNowOperator extends ShowOperator{
    public ShowNowOperator(int tokenIntType) {
        super(tokenIntType, OperatorType.SHOW);
    }

    public ShowNowOperator(int tokenIntType, OperatorType operatorType) {
        super(tokenIntType);
        this.operatorType = operatorType;
    }
}
