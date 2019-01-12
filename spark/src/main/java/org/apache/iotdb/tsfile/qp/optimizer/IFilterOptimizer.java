package org.apache.iotdb.tsfile.qp.optimizer;

import org.apache.iotdb.tsfile.qp.common.FilterOperator;
import org.apache.iotdb.tsfile.qp.exception.DNFOptimizeException;
import org.apache.iotdb.tsfile.qp.exception.MergeFilterException;
import org.apache.iotdb.tsfile.qp.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
