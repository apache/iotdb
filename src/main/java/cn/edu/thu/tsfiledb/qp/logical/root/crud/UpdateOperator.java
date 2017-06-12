package cn.edu.thu.tsfiledb.qp.logical.root.crud;

import cn.edu.thu.tsfiledb.qp.exception.logical.operator.BasicOperatorException;

/**
 * this class extends {@code RootOperator} and process update statement
 * 
 * @author kangrong
 *
 */
public final class UpdateOperator extends SFWOperator {

    private String value;

    public UpdateOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.UPDATE;
    }

    public void setValue(String value) throws BasicOperatorException {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
