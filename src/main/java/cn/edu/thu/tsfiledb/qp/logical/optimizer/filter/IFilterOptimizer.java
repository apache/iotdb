package cn.edu.thu.tsfiledb.qp.logical.optimizer.filter;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FilterOperator;



/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    public FilterOperator optimize(FilterOperator filter) throws QueryProcessorException;
}
