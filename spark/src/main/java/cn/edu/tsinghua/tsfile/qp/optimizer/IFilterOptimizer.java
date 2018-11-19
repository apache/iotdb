package cn.edu.tsinghua.tsfile.qp.optimizer;

import cn.edu.tsinghua.tsfile.qp.common.FilterOperator;
import cn.edu.tsinghua.tsfile.qp.exception.DNFOptimizeException;
import cn.edu.tsinghua.tsfile.qp.exception.MergeFilterException;
import cn.edu.tsinghua.tsfile.qp.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
