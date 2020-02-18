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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.*;

public class LastQueryExecutor {
  private List<Path> selectedSeries;
  private List<TSDataType> dataTypes;

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getPaths();
    this.dataTypes = lastQueryPlan.getDataTypes();
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context)
          throws StorageEngineException, IOException, QueryProcessException {

    List<LastQueryResult> lastQueryResultList = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      LastQueryResult lastQueryResult = calculateLastPairForOneSeries(selectedSeries.get(i), dataTypes.get(i), context);
      lastQueryResultList.add(lastQueryResult);
    }

    RowRecord resultRecord = constructLastRowRecord(lastQueryResultList);
    SingleDataSet dataSet = new SingleDataSet(selectedSeries, dataTypes);
    dataSet.setRecord(resultRecord);
    return dataSet;
  }

  /**
   * get aggregation result for one series
   *
   * @param context query context
   * @return AggregateResult list
   */
  private LastQueryResult calculateLastPairForOneSeries(
          Path seriesPath, TSDataType tsDataType,
          QueryContext context)
          throws IOException, QueryProcessException, StorageEngineException {
    LastQueryResult queryResult = new LastQueryResult();
    MNode node = null;
    try {
      node = MManager.getInstance().getNodeByPathFromCache(seriesPath.toString());
    } catch (PathException e) {
      throw new QueryProcessException(e);
    } catch (CacheException e) {
      throw new QueryProcessException(e.getMessage());
    }
    if (node.getCachedLast() != null) {
      queryResult.setPairResult(node.getCachedLast());
      return queryResult;
    }

    // construct series reader without value filter
    Filter timeFilter = null;
    IAggregateReader seriesReader = new SeriesAggregateReader(
            seriesPath, tsDataType, context, QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter), timeFilter, null);

    long maxTime = Long.MIN_VALUE;
    while (seriesReader.hasNextChunk()) {
      // cal by chunk statistics
      if (seriesReader.canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        if (chunkStatistics.getEndTime() > maxTime) {
          maxTime = chunkStatistics.getEndTime();
          queryResult.setPairResult(maxTime, chunkStatistics.getLastValue(), tsDataType);
        }
        seriesReader.skipCurrentChunk();
        continue;
      }
      while (seriesReader.hasNextPage()) {
        //cal by page statistics
        if (seriesReader.canUseCurrentPageStatistics()) {
          Statistics pageStatistic = seriesReader.currentPageStatistics();
          if (pageStatistic.getEndTime() > maxTime) {
            maxTime = pageStatistic.getEndTime();
            queryResult.setPairResult(maxTime, pageStatistic.getLastValue(), tsDataType);
          }
          seriesReader.skipCurrentPage();
          continue;
        }
        // cal by page data
        while (seriesReader.hasNextOverlappedPage()) {
          BatchData nextOverlappedPageData = seriesReader.nextOverlappedPage();
          int maxIndex = nextOverlappedPageData.length() - 1;
          if (maxIndex < 0) {
            continue;
          }
          long time = nextOverlappedPageData.getTimeByIndex(maxIndex);
          if (time > maxTime) {
            maxTime = time;
            queryResult.setPairResult(maxTime, nextOverlappedPageData.getValueInTimestamp(time), tsDataType);
          }
          nextOverlappedPageData.resetBatchData();
        }
      }
    }
    if (queryResult.hasResult())
      node.setCachedLast(queryResult.getPairResult());
    return queryResult;
  }

  /**
   * using last result data list construct QueryDataSet.
   *
   * @param lastQueryResultList last result list
   */
  private RowRecord constructLastRowRecord(List<LastQueryResult> lastQueryResultList) {
    long maxTime = Long.MIN_VALUE;
    for (LastQueryResult lastPair : lastQueryResultList) {
      if (lastPair.hasResult() && lastPair.getTimestamp() > maxTime)
        maxTime = lastPair.getTimestamp();
    }

    RowRecord resultRecord = new RowRecord(maxTime);
    for (int i = 0; i < lastQueryResultList.size(); i++) {
      TSDataType dataType = dataTypes.get(i);
      LastQueryResult lastPair = lastQueryResultList.get(i);
      if (lastPair.hasResult() && lastPair.getTimestamp() == maxTime)
        resultRecord.addField(lastPair.getValue(), dataType);
      else
        resultRecord.addField(null, dataType);
    }

    return resultRecord;
  }

  class LastQueryResult {
    private TimeValuePair pairResult;

    public LastQueryResult() {
      pairResult = null;
    }

    public void setPairResult(TimeValuePair timeValuePair) {
      pairResult = timeValuePair;
    }

    public void setPairResult(long time, Object value, TSDataType dataType) {
      if (pairResult == null) {
        pairResult = new TimeValuePair(time, TsPrimitiveType.getByType(dataType, value));
      } else {
        pairResult.setTimestamp(time);
        pairResult.setValue(TsPrimitiveType.getByType(dataType, value));
      }
    }

    public TimeValuePair getPairResult() {
      return pairResult;
    }

    public boolean hasResult() {
      return pairResult != null;
    }

    public long getTimestamp() {
      if (pairResult == null)
        return 0;
      return pairResult.getTimestamp();
    }

    public Object getValue() {
      if (pairResult == null)
        return null;
      return pairResult.getValue().getValue();
    }
  }
}
