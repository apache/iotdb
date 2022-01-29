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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.executor.fill.ValueFill;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.UnsupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FillQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(FillQueryExecutor.class);

  protected FillQueryPlan plan;
  protected List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected Map<TSDataType, IFill> typeIFillMap;
  protected long queryTime;

  public FillQueryExecutor(FillQueryPlan fillQueryPlan) {
    this.plan = fillQueryPlan;
    this.selectedSeries = plan.getDeduplicatedPaths();
    this.typeIFillMap = plan.getFillType();
    this.dataTypes = plan.getDeduplicatedDataTypes();
    this.queryTime = plan.getQueryTime();
  }

  /**
   * execute fill.
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    RowRecord record = new RowRecord(queryTime);

    Pair<List<StorageGroupProcessor>, Map<StorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair = StorageEngine.getInstance().mergeLock(selectedSeries);
    List<StorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<StorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, contructTimeFilter());
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    List<TimeValuePair> timeValuePairs = getTimeValuePairs(context);
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    for (int i = 0; i < selectedSeries.size(); i++) {
      PartialPath path = selectedSeries.get(i);
      TSDataType dataType = dataTypes.get(i);

      if (timeValuePairs.get(i) != null) {
        // No need to fill
        record.addField(timeValuePairs.get(i).getValue().getValue(), dataType);
        continue;
      }

      IFill fill;
      if (!typeIFillMap.containsKey(dataType)) {
        switch (dataType) {
          case INT32:
          case INT64:
          case FLOAT:
          case DOUBLE:
          case BOOLEAN:
          case TEXT:
            fill = new PreviousFill(dataType, queryTime, defaultFillInterval);
            break;
          default:
            throw new UnsupportedDataTypeException("unsupported data type " + dataType);
        }
      } else {
        fill = typeIFillMap.get(dataType).copy();
      }
      fill =
          configureFill(
              fill,
              path,
              dataType,
              queryTime,
              plan.getAllMeasurementsInDevice(path.getDevice()),
              context);

      TimeValuePair timeValuePair = fill.getFillResult();
      if (timeValuePair == null || timeValuePair.getValue() == null) {
        record.addField(null);
      } else {
        record.addField(timeValuePair.getValue().getValue(), dataType);
      }
    }

    SingleDataSet dataSet = new SingleDataSet(selectedSeries, dataTypes);
    dataSet.setRecord(record);
    return dataSet;
  }

  private Filter contructTimeFilter() throws UnsupportedDataTypeException {
    long lowerBound = Long.MAX_VALUE;
    long upperBound = Long.MIN_VALUE;
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType dataType = dataTypes.get(i);
      if (!typeIFillMap.containsKey(dataType)) {
        switch (dataType) {
          case INT32:
          case INT64:
          case FLOAT:
          case DOUBLE:
          case BOOLEAN:
          case TEXT:
            lowerBound =
                Math.min(
                    lowerBound,
                    defaultFillInterval == -1 ? Long.MIN_VALUE : queryTime - defaultFillInterval);
            upperBound = Math.max(upperBound, queryTime);
            break;
          default:
            throw new UnsupportedDataTypeException("unsupported data type " + dataType);
        }
      } else {
        IFill fill = typeIFillMap.get(dataType).copy();
        if (fill instanceof PreviousFill) {
          long beforeRange = ((PreviousFill) fill).getBeforeRange();
          lowerBound =
              Math.min(lowerBound, beforeRange == -1 ? Long.MIN_VALUE : queryTime - beforeRange);
          upperBound = Math.max(upperBound, queryTime);
        } else if (fill instanceof LinearFill) {
          long beforeRange = ((LinearFill) fill).getBeforeRange();
          long afterRange = ((LinearFill) fill).getAfterRange();
          lowerBound =
              Math.min(lowerBound, beforeRange == -1 ? Long.MIN_VALUE : queryTime - beforeRange);
          upperBound =
              Math.max(upperBound, afterRange == -1 ? Long.MAX_VALUE : queryTime + afterRange);
        } else if (fill instanceof ValueFill) {
          lowerBound = Math.min(lowerBound, queryTime);
          upperBound = Math.max(upperBound, queryTime);
        }
      }
    }
    return FilterFactory.and(TimeFilter.gtEq(lowerBound), TimeFilter.ltEq(upperBound));
  }

  protected IFill configureFill(
      IFill fill,
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context)
      throws QueryProcessException, StorageEngineException {
    fill.configureFill(path, dataType, queryTime, deviceMeasurements, context);
    return fill;
  }

  protected List<TimeValuePair> getTimeValuePairs(QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException {
    List<ManagedSeriesReader> readers = initManagedSeriesReader(context);
    List<TimeValuePair> ret = new ArrayList<>(selectedSeries.size());
    for (ManagedSeriesReader reader : readers) {
      if (reader.hasNextBatch()) {
        BatchData batchData = reader.nextBatch();
        if (batchData.hasCurrent()) {
          ret.add(new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType()));
          continue;
        }
      }
      ret.add(null);
    }

    return ret;
  }

  private List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    Filter timeFilter = TimeFilter.eq(queryTime);
    List<ManagedSeriesReader> readers = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      PartialPath path = selectedSeries.get(i);
      TSDataType dataType = dataTypes.get(i);
      QueryDataSource queryDataSource =
          QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
      timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
      ManagedSeriesReader reader =
          new SeriesRawDataBatchReader(
              path,
              plan.getAllMeasurementsInDevice(path.getDevice()),
              dataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              plan.isAscending());
      readers.add(reader);
    }

    return readers;
  }
}
