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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
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

    ListDataSet dataSet =
            new ListDataSet(
                    Arrays.asList(new Path(COLUMN_TIMESERIES), new Path(COLUMN_VALUE)),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));
    for (int i = 0; i < selectedSeries.size(); i++) {
      TimeValuePair lastTimeValuePair =
          calculateLastPairForOneSeries(selectedSeries.get(i), dataTypes.get(i), context);
      if (lastTimeValuePair.getValue() != null) {
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());
        Field pathField = new Field(TSDataType.TEXT);
        pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPath()));
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
        resultRecord.addField(valueField);

        dataSet.putRecord(resultRecord);
      }
    }

    return dataSet;
  }

  /**
   * get aggregation result for one series
   *
   * @param context query context
   * @return AggregateResult list
   */
  private TimeValuePair calculateLastPairForOneSeries(
      Path seriesPath, TSDataType tsDataType, QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {

    // Retrieve last value from MNode
    MNode node = null;
     try {
       node = MManager.getInstance().getDeviceNodeWithAutoCreateStorageGroup(seriesPath.toString());
     } catch (MetadataException e) {
       throw new QueryProcessException(e);
     }
     if (node.getCachedLast() != null) {
       return node.getCachedLast();
     }

    // construct series reader without value filter
    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath,
            tsDataType,
            context,
            QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, null),
            null,
            null,
            null);

    TimeValuePair resultPair = new TimeValuePair(0, null);
    long maxTime = Long.MIN_VALUE;
    while (seriesReader.hasNextChunk()) {
      // cal by chunk statistics
      if (seriesReader.canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        if (chunkStatistics.getEndTime() > maxTime) {
          maxTime = chunkStatistics.getEndTime();
          resultPair.setTimestamp(maxTime);
          resultPair.setValue(TsPrimitiveType.getByType(tsDataType, chunkStatistics.getLastValue()));
        }
        seriesReader.skipCurrentChunk();
        continue;
      }
      while (seriesReader.hasNextPage()) {
        // cal by page statistics
        if (seriesReader.canUseCurrentPageStatistics()) {
          Statistics pageStatistic = seriesReader.currentPageStatistics();
          if (pageStatistic.getEndTime() > maxTime) {
            maxTime = pageStatistic.getEndTime();
            resultPair.setTimestamp(maxTime);
            resultPair.setValue(TsPrimitiveType.getByType(tsDataType, pageStatistic.getLastValue()));
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
            resultPair.setTimestamp(maxTime);
            resultPair.setValue(TsPrimitiveType.getByType(tsDataType, nextOverlappedPageData.getValueInTimestamp(maxTime)));
          }
          nextOverlappedPageData.resetBatchData();
        }
      }
    }
    // Update cached last value
    if (resultPair.getValue() != null)
      node.updateCachedLast(resultPair, false);
    return resultPair;
  }

}
