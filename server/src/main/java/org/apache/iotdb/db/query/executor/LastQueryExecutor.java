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


import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
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

public class LastQueryExecutor {

  private List<PartialPath> selectedSeries;
  private List<TSDataType> dataTypes;
  private IExpression expression;
  private static final boolean lastCacheEnabled =
          IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();

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

    ListDataSet dataSet = new ListDataSet(
        Arrays.asList(new PartialPath(COLUMN_TIMESERIES, false), new PartialPath(COLUMN_VALUE, false)),
        Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    List<Pair<Boolean, TimeValuePair>> lastPairList = calculateLastPairForSeriesLocally(
            selectedSeries, dataTypes, context, expression, lastQueryPlan);

    for (int i = 0; i < lastPairList.size(); i++) {
      if (lastPairList.get(i).left) {
        TimeValuePair lastTimeValuePair = lastPairList.get(i).right;
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());
        Field pathField = new Field(TSDataType.TEXT);
        if (selectedSeries.get(i).getTsAlias() != null) {
          pathField.setBinaryV(new Binary(selectedSeries.get(i).getTsAlias()));
        } else {
          if (selectedSeries.get(i).getMeasurementAlias() != null) {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPathWithAlias()));
          } else {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPath()));
          }
        }
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        if (lastTimeValuePair.getValue() != null) {
          valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
          resultRecord.addField(valueField);
        } else {
          resultRecord.addField(null);
        }

        dataSet.putRecord(resultRecord);
      }
    }

    if (!lastQueryPlan.isAscending()) {
      dataSet.sortByTime();
    }
    return dataSet;
  }

  public static List<Pair<Boolean, TimeValuePair>> calculateLastPairForSeriesLocally(
          List<PartialPath> seriesPaths, List<TSDataType> dataTypes, QueryContext context,
          IExpression expression, RawDataQueryPlan lastQueryPlan)
          throws QueryProcessException, StorageEngineException, IOException {
    List<LastCacheAccessor> cacheAccessors = new ArrayList<>();
    Filter filter = (expression == null) ? null : ((GlobalTimeExpression) expression).getFilter();

    List<PartialPath> restPaths = new ArrayList<>();
    List<Pair<Boolean, TimeValuePair>> resultContainer =
            readLastPairsFromCache(seriesPaths, filter, cacheAccessors, restPaths);
    // If any '>' or '>=' filters are specified, only access cache to get Last result.
    if (filter != null || restPaths.isEmpty()) {
      return resultContainer;
    }

    List<LastPointReader> readerList = new ArrayList<>();
    List<StorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(restPaths);
    try {
      for (int i = 0; i < restPaths.size(); i++) {
        QueryDataSource dataSource =
                QueryResourceManager.getInstance().getQueryDataSource(seriesPaths.get(i), context, null);
        LastPointReader lastReader = new LastPointReader(seriesPaths.get(i), dataTypes.get(i),
                lastQueryPlan.getAllMeasurementsInDevice(seriesPaths.get(i).getDevice()),
                context, dataSource, Long.MAX_VALUE, null);
        readerList.add(lastReader);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

    int index = 0;
    for (int i = 0; i < resultContainer.size(); i++) {
      if (!resultContainer.get(i).left) {
        resultContainer.get(i).left = true;
        resultContainer.get(i).right = readerList.get(index++).readLastPoint();
        if (lastCacheEnabled) {
          cacheAccessors.get(i).write(resultContainer.get(i).right);
        }
      }
    }
    return resultContainer;
  }

  private static List<Pair<Boolean, TimeValuePair>> readLastPairsFromCache(List<PartialPath> seriesPaths,
          Filter filter, List<LastCacheAccessor> cacheAccessors, List<PartialPath> restPaths) {
    List<Pair<Boolean, TimeValuePair>> resultContainer = new ArrayList<>();
    if (lastCacheEnabled) {
      for (PartialPath path : seriesPaths) {
        cacheAccessors.add(new LastCacheAccessor(path, filter));
      }
    } else {
      restPaths.addAll(seriesPaths);
    }
    for (int i = 0; i < cacheAccessors.size(); i++) {
      TimeValuePair tvPair = cacheAccessors.get(i).read();
      if (tvPair != null) {
        resultContainer.add(new Pair<>(true, tvPair));
      } else {
        resultContainer.add(new Pair<>(false, null));
        restPaths.add(seriesPaths.get(i));
      }
    }
    return resultContainer;
  }

  private static class LastCacheAccessor {
    private PartialPath path;
    private Filter filter;
    private MeasurementMNode node;

    LastCacheAccessor(PartialPath seriesPath) {
      this.path = seriesPath;
    }

    LastCacheAccessor(PartialPath seriesPath, Filter filter) {
      this.path = seriesPath;
      this.filter = filter;
    }

    public TimeValuePair read() {
      try {
        node = (MeasurementMNode) IoTDB.metaManager.getNodeByPath(path);
      } catch (MetadataException e) {
        TimeValuePair timeValuePair = IoTDB.metaManager.getLastCache(path);
        if (timeValuePair != null && satisfyFilter(filter, timeValuePair)) {
          return timeValuePair;
        } else if (timeValuePair != null) {
          return null;
        }
      }

      if (node != null && node.getCachedLast() != null) {
        TimeValuePair timeValuePair =  node.getCachedLast();
        if (timeValuePair != null && satisfyFilter(filter, timeValuePair)) {
          return timeValuePair;
        } else if (timeValuePair != null) {
          return null;
        }
      }
      return null;
    }

    public void write(TimeValuePair pair) {
      IoTDB.metaManager.updateLastCache(path, pair, false, Long.MIN_VALUE, node);
    }
  }

  private static boolean satisfyFilter(Filter filter, TimeValuePair tvPair) {
    return filter == null ||
            filter.satisfy(tvPair.getTimestamp(), tvPair.getValue().getValue());
  }
}
