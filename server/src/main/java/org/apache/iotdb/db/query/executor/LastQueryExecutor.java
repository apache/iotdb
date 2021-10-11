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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

public class LastQueryExecutor {

  private List<PartialPath> selectedSeries;
  private List<TSDataType> dataTypes;
  protected IExpression expression;
  private static final boolean CACHE_ENABLED =
      IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getDeduplicatedPaths();
    this.dataTypes = lastQueryPlan.getDeduplicatedDataTypes();
    this.expression = lastQueryPlan.getExpression();
  }

  public LastQueryExecutor(List<PartialPath> selectedSeries, List<TSDataType> dataTypes) {
    this.selectedSeries = selectedSeries;
    this.dataTypes = dataTypes;
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public QueryDataSet execute(QueryContext context, LastQueryPlan lastQueryPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    ListDataSet dataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_TIMESERIES, false),
                new PartialPath(COLUMN_VALUE, false),
                new PartialPath(COLUMN_TIMESERIES_DATATYPE, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));

    List<Pair<Boolean, TimeValuePair>> lastPairList =
        calculateLastPairForSeries(selectedSeries, dataTypes, context, expression, lastQueryPlan);

    for (int i = 0; i < lastPairList.size(); i++) {
      if (lastPairList.get(i).right != null && lastPairList.get(i).right.getValue() != null) {
        TimeValuePair lastTimeValuePair = lastPairList.get(i).right;
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());

        Field pathField = new Field(TSDataType.TEXT);
        pathField.setBinaryV(
            new Binary(lastQueryPlan.getResultColumns().get(i).getResultColumnName()));
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
        resultRecord.addField(valueField);

        Field typeField = new Field(TSDataType.TEXT);
        typeField.setBinaryV(new Binary(lastTimeValuePair.getValue().getDataType().name()));
        resultRecord.addField(typeField);

        dataSet.putRecord(resultRecord);
      }
    }

    if (!lastQueryPlan.isAscending()) {
      dataSet.sortByTime();
    }
    return dataSet;
  }

  protected List<Pair<Boolean, TimeValuePair>> calculateLastPairForSeries(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      RawDataQueryPlan lastQueryPlan)
      throws QueryProcessException, StorageEngineException, IOException {
    return calculateLastPairForSeriesLocally(
        seriesPaths, dataTypes, context, expression, lastQueryPlan.getDeviceToMeasurements());
  }

  public static List<Pair<Boolean, TimeValuePair>> calculateLastPairForSeriesLocally(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      Map<String, Set<String>> deviceMeasurementsMap)
      throws QueryProcessException, StorageEngineException, IOException {
    List<LastCacheAccessor> cacheAccessors = new ArrayList<>();
    Filter filter = (expression == null) ? null : ((GlobalTimeExpression) expression).getFilter();

    List<PartialPath> nonCachedPaths = new ArrayList<>();
    List<TSDataType> nonCachedDataTypes = new ArrayList<>();
    List<Pair<Boolean, TimeValuePair>> resultContainer =
        readLastPairsFromCache(
            seriesPaths,
            dataTypes,
            filter,
            cacheAccessors,
            nonCachedPaths,
            nonCachedDataTypes,
            context.isDebug());
    if (nonCachedPaths.isEmpty()) {
      return resultContainer;
    }

    // Acquire query resources for the rest series paths
    List<LastPointReader> readerList = new ArrayList<>();
    List<StorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(nonCachedPaths);
    try {
      for (int i = 0; i < nonCachedPaths.size(); i++) {
        QueryDataSource dataSource =
            QueryResourceManager.getInstance()
                .getQueryDataSource(nonCachedPaths.get(i), context, null);
        LastPointReader lastReader =
            new LastPointReader(
                nonCachedPaths.get(i),
                nonCachedDataTypes.get(i),
                deviceMeasurementsMap.getOrDefault(
                    nonCachedPaths.get(i).getDevice(), new HashSet<>()),
                context,
                dataSource,
                Long.MAX_VALUE,
                null);
        readerList.add(lastReader);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

    // Compute Last result for the rest series paths by scanning Tsfiles
    int index = 0;
    for (int i = 0; i < resultContainer.size(); i++) {
      if (Boolean.FALSE.equals(resultContainer.get(i).left)) {
        resultContainer.get(i).right = readerList.get(index++).readLastPoint();
        if (resultContainer.get(i).right.getValue() != null) {
          resultContainer.get(i).left = true;
          if (CACHE_ENABLED) {
            cacheAccessors.get(i).write(resultContainer.get(i).right);
            if (context.isDebug()) {
              DEBUG_LOGGER.info(
                  "[LastQueryExecutor] Update last cache for path: {} with timestamp: {}",
                  seriesPaths,
                  resultContainer.get(i).right.getTimestamp());
            }
          }
        }
      }
    }
    return resultContainer;
  }

  private static List<Pair<Boolean, TimeValuePair>> readLastPairsFromCache(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      Filter filter,
      List<LastCacheAccessor> cacheAccessors,
      List<PartialPath> restPaths,
      List<TSDataType> restDataType,
      boolean debugOn) {
    List<Pair<Boolean, TimeValuePair>> resultContainer = new ArrayList<>();
    if (CACHE_ENABLED) {
      for (PartialPath path : seriesPaths) {
        cacheAccessors.add(new LastCacheAccessor(path));
      }
    } else {
      restPaths.addAll(seriesPaths);
      restDataType.addAll(dataTypes);
      for (int i = 0; i < seriesPaths.size(); i++) {
        resultContainer.add(new Pair<>(false, null));
      }
    }
    for (int i = 0; i < cacheAccessors.size(); i++) {
      TimeValuePair tvPair = cacheAccessors.get(i).read();
      if (tvPair == null) {
        resultContainer.add(new Pair<>(false, null));
        restPaths.add(seriesPaths.get(i));
        restDataType.add(dataTypes.get(i));
      } else if (!satisfyFilter(filter, tvPair)) {
        resultContainer.add(new Pair<>(true, null));
        if (debugOn) {
          DEBUG_LOGGER.info(
              "[LastQueryExecutor] Last cache hit for path: {} with timestamp: {}",
              seriesPaths.get(i),
              tvPair.getTimestamp());
        }
      } else {
        resultContainer.add(new Pair<>(true, tvPair));
        if (debugOn) {
          DEBUG_LOGGER.info(
              "[LastQueryExecutor] Last cache hit for path: {} with timestamp: {}",
              seriesPaths.get(i),
              tvPair.getTimestamp());
        }
      }
    }
    return resultContainer;
  }

  private static class LastCacheAccessor {
    private PartialPath path;
    private IMeasurementMNode node;

    LastCacheAccessor(PartialPath seriesPath) {
      this.path = seriesPath;
    }

    public TimeValuePair read() {
      try {
        node = IoTDB.metaManager.getMeasurementMNode(path);
      } catch (MetadataException e) {
        // cluster mode may not get remote node
        TimeValuePair timeValuePair;
        timeValuePair = IoTDB.metaManager.getLastCache(path);
        if (timeValuePair != null) {
          return timeValuePair;
        }
      }

      if (node == null) {
        return null;
      }

      if (path instanceof VectorPartialPath) {
        // the seriesPath has been transformed to vector path
        // here needs subSensor path
        return IoTDB.metaManager.getLastCache(
            node.getAsMultiMeasurementMNode(), ((VectorPartialPath) path).getSubSensor(0));
      } else {
        return IoTDB.metaManager.getLastCache(node.getAsUnaryMeasurementMNode());
      }
    }

    public void write(TimeValuePair pair) {
      if (node == null) {
        IoTDB.metaManager.updateLastCache(path, pair, false, Long.MIN_VALUE);
      } else {
        if (path instanceof VectorPartialPath) {
          // the seriesPath has been transformed to vector path
          // here needs subSensor path
          IoTDB.metaManager.updateLastCache(
              node.getAsMultiMeasurementMNode(),
              ((VectorPartialPath) path).getSubSensor(0),
              pair,
              false,
              Long.MIN_VALUE);
        } else {
          IoTDB.metaManager.updateLastCache(
              node.getAsUnaryMeasurementMNode(), pair, false, Long.MIN_VALUE);
        }
      }
    }
  }

  private static boolean satisfyFilter(Filter filter, TimeValuePair tvPair) {
    return filter == null || filter.satisfy(tvPair.getTimestamp(), tvPair.getValue().getValue());
  }
}
