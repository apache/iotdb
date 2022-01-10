/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.iterator.LeafNode;
import org.apache.iotdb.tsfile.read.query.iterator.SeriesIterator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

/** IoTDB query executor. */
public class RawDataQueryExecutor {

  protected RawDataQueryPlan queryPlan;

  public RawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  private static final Logger logger = LoggerFactory.getLogger(RawDataQueryExecutor.class);

  /** without filter or with global time filter. */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, false);
    if (dataSet != null) {
      return dataSet;
    }
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);

    try {
      return new RawQueryDataSetWithoutValueFilter(
          context.getQueryId(), queryPlan, readersOfSelectedSeries);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineException(e.getMessage());
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  public final QueryDataSet executeNonAlign(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, false);
    if (dataSet != null) {
      return dataSet;
    }
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new NonAlignEngineDataSet(
        context.getQueryId(),
        queryPlan.getDeduplicatedPaths(),
        queryPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
  }

  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    Filter timeFilter = null;
    if (queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {

      // init QueryDataSource cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);

      List<PartialPath> paths = queryPlan.getDeduplicatedPaths();
      for (PartialPath path : paths) {
        TSDataType dataType = path.getSeriesType();

        QueryDataSource queryDataSource =
            QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
        timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

        ManagedSeriesReader reader =
            new SeriesRawDataBatchReader(
                path,
                queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                dataType,
                context,
                queryDataSource,
                timeFilter,
                null,
                null,
                queryPlan.isAscending());
        readersOfSelectedSeries.add(reader);
      }
    } catch (Exception e) {
      logger.error("Meet error when init series reader ", e);
      throw new QueryProcessException("Meet error when init series reader.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }
    return readersOfSelectedSeries;
  }

  /**
   * executeWithValueFilter query.
   *
   * @return QueryDataSet object
   * @throws StorageEngineException StorageEngineException
   */
  public final QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, true);
    if (dataSet != null) {
      return dataSet;
    }

    // Here the custom logic kicks in, I guess
    System.out.println("Query Plan: " + queryPlan);
    System.out.println("Expression: " + queryPlan.getExpression());

    if (!(queryPlan.getExpression() instanceof SingleSeriesExpression)) {
      throw new IllegalArgumentException();
    }

    try {
      IBatchReader reader = generateNewBatchReader(context, (SingleSeriesExpression) queryPlan.getExpression());

      // Now do something with the reader?!
      TSDataType seriesType = ((MeasurementPath) ((SingleSeriesExpression) queryPlan.getExpression()).getSeriesPath()).getSeriesType();
      SeriesIterator iterator = new SeriesIterator(seriesType, reader, ((SingleSeriesExpression) queryPlan.getExpression()).getFilter());

      while (iterator.hasNext()) {
        Object[] next = iterator.next();

        System.out.println("Next: " + Arrays.toString(next));
      }

    } catch (IOException e) {
      throw new IllegalStateException();
    }

    TimeGenerator timestampGenerator = getTimeGenerator(context, queryPlan);
    List<Boolean> cached =
        markFilterdPaths(
            queryPlan.getExpression(),
            new ArrayList<>(queryPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries =
        initSeriesReaderByTimestamp(context, queryPlan, cached, timestampGenerator.getTimeFilter());
    return new RawQueryDataSetWithValueFilter(
        queryPlan.getDeduplicatedPaths(),
        queryPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        cached,
        queryPlan.isAscending());
  }

  protected List<IReaderByTimestamp> initSeriesReaderByTimestamp(
      QueryContext context, RawDataQueryPlan queryPlan, List<Boolean> cached, Filter timeFilter)
      throws QueryProcessException, StorageEngineException {
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();

    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);

      for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
        if (cached.get(i)) {
          readersOfSelectedSeries.add(null);
          continue;
        }
        PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTimestamp(
                path,
                queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                queryPlan.getDeduplicatedDataTypes().get(i),
                context);
        readersOfSelectedSeries.add(seriesReaderByTimestamp);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }
    return readersOfSelectedSeries;
  }

  protected IReaderByTimestamp getReaderByTimestamp(
      PartialPath path, Set<String> allSensors, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        allSensors,
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        queryPlan.isAscending());
  }

  protected IBatchReader generateNewBatchReader(QueryContext context, SingleSeriesExpression expression)
      throws IOException {
    Filter valueFilter = expression.getFilter();
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType = path.getSeriesType();
    QueryDataSource queryDataSource;
    try {
      queryDataSource =
          QueryResourceManager.getInstance().getQueryDataSource(path, context, valueFilter);
      // update valueFilter by TTL
      valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
    } catch (Exception e) {
      throw new IOException(e);
    }

    // get the TimeFilter part in SingleSeriesExpression
    Filter timeFilter = getTimeFilter(valueFilter);

    return new SeriesRawDataBatchReader(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        null,
        queryPlan.isAscending());
  }

  /** extract time filter from a value filter */
  protected Filter getTimeFilter(Filter filter) {
    if (filter instanceof UnaryFilter
        && ((UnaryFilter) filter).getFilterType() == FilterType.TIME_FILTER) {
      return filter;
    }
    if (filter instanceof AndFilter) {
      Filter leftTimeFilter = getTimeFilter(((AndFilter) filter).getLeft());
      Filter rightTimeFilter = getTimeFilter(((AndFilter) filter).getRight());
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return filter;
      } else if (leftTimeFilter != null) {
        return leftTimeFilter;
      } else {
        return rightTimeFilter;
      }
    }
    return null;
  }

  protected void newScan(QueryContext context, RawDataQueryPlan queryPlan) throws StorageEngineException {
    List<PartialPath> pathList = new ArrayList<>();
    IExpression expression = queryPlan.getExpression();

    if (!(expression instanceof SingleSeriesExpression)) {
      throw new IllegalArgumentException("Only single series Exceptions are supported at the moment!");
    }

    getAndTransformPartialPathFromExpression(expression, pathList);
    List<VirtualStorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(pathList);
    try {
      // Check if its a single series Expression
//      operatorNode = construct(context, expression);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  protected IBatchReader getReaderForSeries(QueryContext context, SingleSeriesExpression expression) {
    try {
      return generateNewBatchReader(context, expression);
    } catch (IOException e) {
      throw new IllegalStateException();
    }
  }

  protected Node construct(QueryContext context, IExpression expression) {
    try {
      if (expression.getType() == ExpressionType.SERIES) {
        SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
        IBatchReader seriesReader = generateNewBatchReader(context, singleSeriesExp);
        Path path = singleSeriesExp.getSeriesPath();

        // put the current reader to valueCache
        LeafNode leafNode = new LeafNode(seriesReader);

        return leafNode;
      } else {
        Node leftChild = construct(context, ((IBinaryExpression) expression).getLeft());
        Node rightChild = construct(context, ((IBinaryExpression) expression).getRight());

        if (expression.getType() == ExpressionType.OR) {
//          hasOrNode = true;
//          return new OrNode(leftChild, rightChild, isAscending());
        } else if (expression.getType() == ExpressionType.AND) {
//          return new AndNode(leftChild, rightChild, isAscending());
        }
        throw new UnSupportedDataTypeException(
            "Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
      }
    } catch (IOException ex) {
      throw new IllegalStateException();
    }
  }

  /**
   * collect PartialPath from Expression and transform MeasurementPath whose isUnderAlignedEntity is
   * true to AlignedPath
   */
  private void getAndTransformPartialPathFromExpression(
      IExpression expression, List<PartialPath> pathList) {
    if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression seriesExpression = (SingleSeriesExpression) expression;
      MeasurementPath measurementPath = (MeasurementPath) seriesExpression.getSeriesPath();
      // change the MeasurementPath to AlignedPath if the MeasurementPath's isUnderAlignedEntity ==
      // true
      seriesExpression.setSeriesPath(measurementPath.transformToExactPath());
      pathList.add((PartialPath) seriesExpression.getSeriesPath());
    } else {
      getAndTransformPartialPathFromExpression(
          ((IBinaryExpression) expression).getLeft(), pathList);
      getAndTransformPartialPathFromExpression(
          ((IBinaryExpression) expression).getRight(), pathList);
    }
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  /**
   * Check whether need to redirect query to other node.
   *
   * @param context query context
   * @param hasValueFilter if has value filter, we need to check timegenerator
   * @return dummyDataSet to avoid more cost, if null, no need
   */
  protected QueryDataSet needRedirect(QueryContext context, boolean hasValueFilter)
      throws StorageEngineException, QueryProcessException {
    return null;
  }
}
