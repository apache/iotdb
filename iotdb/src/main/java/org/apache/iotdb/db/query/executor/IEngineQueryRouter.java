
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public interface IEngineQueryRouter {

  /**
   * Execute physical plan.
   */
  QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws FileNodeManagerException;

  /**
   * Execute aggregation query.
   */
  QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context)
      throws QueryFilterOptimizationException, FileNodeManagerException, IOException, PathErrorException, ProcessorException;

  /**
   * Execute groupBy query.
   *
   * @param selectedSeries select path list
   * @param aggres aggregation name list
   * @param expression filter expression
   * @param unit time granularity for interval partitioning, unit is ms.
   * @param origin the datum time point for interval division is divided into a time interval for
   * each TimeUnit time from this point forward and backward.
   * @param intervals time intervals, closed interval.
   */
  QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context)
      throws ProcessorException, QueryFilterOptimizationException, FileNodeManagerException,
      PathErrorException, IOException;
  /**
   * Execute fill query.
   *
   * @param fillPaths select path list
   * @param queryTime timestamp
   * @param fillType type IFill map
   */
  QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) throws FileNodeManagerException, PathErrorException, IOException;

}
