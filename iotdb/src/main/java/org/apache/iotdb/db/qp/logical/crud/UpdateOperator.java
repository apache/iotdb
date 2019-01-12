package org.apache.iotdb.db.qp.logical.crud;

/**
 * this class extends {@code RootOperator} and process update statement
 */
public final class UpdateOperator extends SFWOperator {

    private String value;

    public UpdateOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.UPDATE;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
