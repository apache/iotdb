package org.apache.iotdb.infludb.qp.logical.crud;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;

public class BasicFunctionOperator extends FunctionOperator {
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    protected String value;

    public BasicFunctionOperator(FilterConstant.FilterType filterType, String keyName, String value) {
        super(filterType);
        this.keyName = keyName;
        this.value = value;
    }
}
