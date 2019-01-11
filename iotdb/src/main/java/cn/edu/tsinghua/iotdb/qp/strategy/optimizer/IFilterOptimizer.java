package cn.edu.tsinghua.iotdb.qp.strategy.optimizer;

import cn.edu.tsinghua.iotdb.exception.qp.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;

/**
 * provide a filter operator, optimize it.
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws QueryProcessorException;
}
