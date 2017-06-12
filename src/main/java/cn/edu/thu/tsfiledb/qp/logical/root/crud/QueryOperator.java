package cn.edu.thu.tsfiledb.qp.logical.root.crud;

import cn.edu.thu.tsfiledb.qp.logical.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 * 
 */
public class QueryOperator extends SFWOperator {
    private static final Logger LOG = LoggerFactory.getLogger(QueryOperator.class);

    public QueryOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = Operator.OperatorType.QUERY;
    }
}
