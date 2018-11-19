package cn.edu.tsinghua.iotdb.qp.strategy.optimizer;

import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws QueryProcessorException;
}
