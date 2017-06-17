package cn.edu.thu.tsfiledb.qp.logical.crud;

import cn.edu.thu.tsfiledb.qp.logical.Operator;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 * 
 */
public class QueryOperator extends SFWOperator {

    public QueryOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = Operator.OperatorType.QUERY;
    }
}
