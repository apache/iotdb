package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;

/**
 * This class is used for GROUP clause(not used up to now).
 * 
 * @since 2016-09-27 16:03:45
 * @author kangrong
 *
 */
public class GroupByOperator extends Operator {
    
    public GroupByOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.GROUPBY;
    }

}
