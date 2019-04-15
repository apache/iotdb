package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.IEngineQueryRouter;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public class ClusterQueryRouter implements IEngineQueryRouter {

  @Override
  public QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws FileNodeManagerException {
    return null;
  }

  @Override
  public QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context)
      throws QueryFilterOptimizationException, FileNodeManagerException, IOException, PathErrorException, ProcessorException {
    return null;
  }

  @Override
  public QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context)
      throws ProcessorException, QueryFilterOptimizationException, FileNodeManagerException, PathErrorException, IOException {
    return null;
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) throws FileNodeManagerException, PathErrorException, IOException {
    return null;
  }
}
