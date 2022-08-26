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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.utils.ResourceByPathUtils;
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
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
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
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil.satisfyFilter;

public class LastQueryExecutor {

  private List<PartialPath> selectedSeries;
  private List<TSDataType> dataTypes;
  protected IExpression expression;
  private static final boolean CACHE_ENABLED =
      IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  // for test to reload this parameter after restart, it can't be final
  private static boolean ID_TABLE_ENABLED =
      IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
  private static boolean ascending;

  private static final Logger logger = LoggerFactory.getLogger(LastQueryExecutor.class);

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getDeduplicatedPaths();
    this.dataTypes = lastQueryPlan.getDeduplicatedDataTypes();
    this.expression = lastQueryPlan.getExpression();
    this.ascending = lastQueryPlan.isAscending();
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

    List<TimeValuePair> lastPairList =
        calculateLastPairForSeries(selectedSeries, dataTypes, context, expression, lastQueryPlan);

    for (int i = 0; i < lastPairList.size(); i++) {
      if (lastPairList.get(i) != null && lastPairList.get(i).getValue() != null) {
        TimeValuePair lastTimeValuePair = lastPairList.get(i);
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

  protected List<TimeValuePair> calculateLastPairForSeries(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      RawDataQueryPlan lastQueryPlan)
      throws QueryProcessException, StorageEngineException, IOException {
    return calculateLastPairForSeriesLocally(
        seriesPaths, dataTypes, context, expression, lastQueryPlan.getDeviceToMeasurements());
  }

  public static List<TimeValuePair> calculateLastPairForSeriesLocally(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      Map<String, Set<String>> deviceMeasurementsMap)
      throws QueryProcessException, StorageEngineException, IOException {
    Filter filter = (expression == null) ? null : ((GlobalTimeExpression) expression).getFilter();

    if (CACHE_ENABLED) {
      List<LastCacheAccessor> cacheAccessors = new ArrayList<>();
      for (PartialPath path : seriesPaths) {
        if (ID_TABLE_ENABLED) {
          cacheAccessors.add(new IDTableLastCacheAccessor(path));
        } else {
          cacheAccessors.add(new SchemaProcessorLastCacheAccessor(path));
        }
      }

      List<TimeValuePair> lastPairs =
          readLastPairsFromCache(seriesPaths, cacheAccessors, context.isDebug());

      List<Integer> nonCachedIndices = new ArrayList<>();
      List<PartialPath> nonCachedPaths = new ArrayList<>();
      List<TSDataType> nonCachedDataTypes = new ArrayList<>();
      for (int i = 0; i < lastPairs.size(); i++) {
        TimeValuePair lastPair = lastPairs.get(i);
        if (lastPair == null) {
          nonCachedPaths.add(((MeasurementPath) seriesPaths.get(i)).transformToExactPath());
          nonCachedDataTypes.add(dataTypes.get(i));
          nonCachedIndices.add(i);
        } else if (!satisfyFilter(filter, lastPair)) {
          lastPairs.set(i, null);
          boolean isFilterGtOrGe = (filter instanceof Gt || filter instanceof GtEq);
          if (!isFilterGtOrGe) {
            nonCachedPaths.add(((MeasurementPath) seriesPaths.get(i)).transformToExactPath());
            nonCachedDataTypes.add(dataTypes.get(i));
            nonCachedIndices.add(i);
          }
        }
      }

      List<TimeValuePair> nonCachedLastPairs =
          readLastPairsFromStorage(
              nonCachedPaths, nonCachedDataTypes, filter, context, deviceMeasurementsMap);
      for (int i = 0; i < nonCachedLastPairs.size(); i++) {
        // Update the cache only when,
        // 1. the last value cache doesn't exist
        // 2. the actual last value is not null
        // 3. last value cache is enabled
        // 4. the filter is gt (greater than) or ge (greater than or equal to)
        if (lastPairs.get(nonCachedIndices.get(i)) == null
            && nonCachedLastPairs.get(i) != null
            && ((filter instanceof GtEq) || (filter instanceof Gt))) {
          cacheAccessors.get(nonCachedIndices.get(i)).write(nonCachedLastPairs.get(i));
        }
        lastPairs.set(nonCachedIndices.get(i), nonCachedLastPairs.get(i));
      }
      return lastPairs;
    } else {
      return readLastPairsFromStorage(
          seriesPaths.stream()
              .map(p -> ((MeasurementPath) p).transformToExactPath())
              .collect(Collectors.toList()),
          dataTypes,
          filter,
          context,
          deviceMeasurementsMap);
    }
  }

  /**
   * Get the last values of given timeseries from the cache.
   *
   * @return A list of {@link TimeValuePair}. The null elements indicate that the last value of
   *     corresponding timeseries is not cached.
   */
  private static List<TimeValuePair> readLastPairsFromCache(
      List<PartialPath> seriesPaths, List<LastCacheAccessor> cacheAccessors, boolean debugOn) {
    List<TimeValuePair> ret = new ArrayList<>();
    for (int i = 0; i < cacheAccessors.size(); i++) {
      TimeValuePair tvPair = cacheAccessors.get(i).read();
      ret.add(tvPair);
      if (tvPair != null && debugOn) {
        DEBUG_LOGGER.info(
            "[LastQueryExecutor] Last cache hit for path: {} with timestamp: {}",
            seriesPaths.get(i),
            tvPair.getTimestamp());
      }
    }
    return ret;
  }

  /**
   * Get the last values of given timeseries from the storage.
   *
   * @return A list of {@link TimeValuePair}. The null elements indicate the last value of
   *     corresponding timeseries does not exist.
   */
  private static List<TimeValuePair> readLastPairsFromStorage(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      Filter filter,
      QueryContext context,
      Map<String, Set<String>> deviceMeasurementsMap)
      throws StorageEngineException, QueryProcessException, IOException {
    // Acquire query resources for the rest series paths
    Pair<List<DataRegion>, Map<DataRegion, List<PartialPath>>> lockListAndProcessorToSeriesMapPair =
        StorageEngine.getInstance().mergeLock(seriesPaths);
    List<DataRegion> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<DataRegion, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, filter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    List<LastPointReader> readers = new ArrayList<>();
    for (int i = 0; i < seriesPaths.size(); i++) {
      QueryDataSource dataSource =
          QueryResourceManager.getInstance()
              .getQueryDataSource(seriesPaths.get(i), context, filter, ascending);
      LastPointReader lastReader =
          ResourceByPathUtils.getResourceInstance(seriesPaths.get(i))
              .createLastPointReader(
                  dataTypes.get(i),
                  deviceMeasurementsMap.getOrDefault(
                      seriesPaths.get(i).getDevice(), new HashSet<>()),
                  context,
                  dataSource,
                  Long.MAX_VALUE,
                  filter);
      readers.add(lastReader);
    }

    List<TimeValuePair> lastPairs = new ArrayList<>(seriesPaths.size());
    for (LastPointReader reader : readers) {
      lastPairs.add(reader.readLastPoint());
    }
    return lastPairs;
  }

  private interface LastCacheAccessor {
    TimeValuePair read();

    void write(TimeValuePair pair);
  }

  private static class SchemaProcessorLastCacheAccessor implements LastCacheAccessor {

    private final MeasurementPath path;
    private IMeasurementMNode node;

    SchemaProcessorLastCacheAccessor(PartialPath seriesPath) {
      this.path = (MeasurementPath) seriesPath;
    }

    public TimeValuePair read() {
      try {
        node = IoTDB.schemaProcessor.getMeasurementMNode(path);
      } catch (MetadataException e) {
        // cluster mode may not get remote node
        TimeValuePair timeValuePair;
        timeValuePair = IoTDB.schemaProcessor.getLastCache(path);
        if (timeValuePair != null) {
          return timeValuePair;
        }
      }

      if (node == null) {
        return null;
      }

      return IoTDB.schemaProcessor.getLastCache(node);
    }

    public void write(TimeValuePair pair) {
      if (node == null) {
        IoTDB.schemaProcessor.updateLastCache(path, pair, false, Long.MIN_VALUE);
      } else {
        IoTDB.schemaProcessor.updateLastCache(node, pair, false, Long.MIN_VALUE);
      }
    }
  }

  private static class IDTableLastCacheAccessor implements LastCacheAccessor {

    private PartialPath fullPath;

    IDTableLastCacheAccessor(PartialPath seriesPath) {
      fullPath = seriesPath;
    }

    @Override
    public TimeValuePair read() {
      try {
        IDTable table =
            StorageEngine.getInstance().getProcessor(fullPath.getDevicePath()).getIdTable();
        return table.getLastCache(new TimeseriesID(fullPath));
      } catch (StorageEngineException | MetadataException e) {
        logger.error("last query can't find storage group: path is: " + fullPath);
      }

      return null;
    }

    @Override
    public void write(TimeValuePair pair) {
      try {
        IDTable table =
            StorageEngine.getInstance().getProcessor(fullPath.getDevicePath()).getIdTable();
        table.updateLastCache(new TimeseriesID(fullPath), pair, false, Long.MIN_VALUE);
      } catch (MetadataException | StorageEngineException e) {
        logger.error("last query can't find storage group: path is: " + fullPath);
      }
    }
  }

  public static void clear() {
    ID_TABLE_ENABLED = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
  }
}
