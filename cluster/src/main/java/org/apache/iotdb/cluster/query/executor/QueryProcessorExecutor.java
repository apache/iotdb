package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.executor.AbstractQueryProcessExecutor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public class QueryProcessorExecutor extends AbstractQueryProcessExecutor {

  public QueryProcessorExecutor() {
    super(new ClusterQueryRouter());
  }

  @Override
  public TSDataType getSeriesType(Path fullPath) throws PathErrorException {
    return null;
  }

  @Override
  public boolean judgePathExists(Path fullPath) {
    return false;
  }

  @Override
  public QueryDataSet aggregate(List<Path> paths, List<String> aggres, IExpression expression,
      QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException, QueryFilterOptimizationException {
    return null;
  }

  @Override
  public QueryDataSet groupBy(List<Path> paths, List<String> aggres, IExpression expression,
      long unit, long origin, List<Pair<Long, Long>> intervals, QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException, QueryFilterOptimizationException {
    return null;
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillTypes,
      QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException {
    return null;
  }

  @Override
  public boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException {
    return false;
  }

  @Override
  protected boolean delete(Path path, long deleteTime) throws ProcessorException {
    return false;
  }

  @Override
  public int insert(Path path, long insertTime, String value) throws ProcessorException {
    return 0;
  }

  @Override
  public int multiInsert(String deviceId, long insertTime, List<String> measurementList,
      List<String> insertValues) throws ProcessorException {
    return 0;
  }

  @Override
  public List<String> getAllPaths(String originPath) throws PathErrorException {
    return null;
  }
}
