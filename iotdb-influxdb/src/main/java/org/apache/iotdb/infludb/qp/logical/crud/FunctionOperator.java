package org.apache.iotdb.infludb.qp.logical.crud;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;

public class FunctionOperator extends FilterOperator{

    public FunctionOperator(FilterConstant.FilterType filterType) {
        super(filterType);
    }
}
