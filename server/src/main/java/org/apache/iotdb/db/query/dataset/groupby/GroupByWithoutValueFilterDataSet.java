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

package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesDataReaderWithoutValueFilter;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private List<SeriesDataReaderWithoutValueFilter> sequenceReaderList;
  private List<BatchData> batchDataList;
  private Filter timeFilter;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws PathException, IOException, StorageEngineException {
    super(context, groupByPlan);

    this.sequenceReaderList = new ArrayList<>();
    this.timeFilter = null;
    this.batchDataList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      batchDataList.add(null);
    }
    initGroupBy(context, groupByPlan);
  }

  /**
   * init reader and aggregate function.
   */
  private void initGroupBy(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException, IOException, PathException {
    IExpression expression = groupByPlan.getExpression();
    initAggreFuction(groupByPlan);
    // init reader
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      // sequence reader for sealed tsfile, unsealed tsfile, memory
      SeriesDataReaderWithoutValueFilter seqResourceIterateReader = new SeriesDataReaderWithoutValueFilter(
          path, dataTypes.get(i), timeFilter, context);
      sequenceReaderList.add(seqResourceIterateReader);
    }
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next() "
          + "in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record = new RowRecord(startTime);
    for (int i = 0; i < functions.size(); i++) {
      AggreResultData res;
      try {
        res = nextSeries(i);
      } catch (QueryProcessException e) {
        throw new IOException(e);
      }
      if (res == null) {
        record.addField(new Field(null));
      } else {
        record.addField(getField(res));
      }
    }
    return record;
  }

  /**
   * calculate the group by result of the series indexed by idx.
   *
   * @param idx series id
   */
  private AggreResultData nextSeries(int idx) throws IOException, QueryProcessException {
    SeriesDataReaderWithoutValueFilter sequenceReader = sequenceReaderList.get(idx);
    AggregateResult function = functions.get(idx);
    function.init();
    TimeRange timeRange = new TimeRange(startTime, endTime - 1);

    BatchData lastBatch = batchDataList.get(idx);
    calcBatchData(idx, function, lastBatch);
    if (isEndCalc(function, lastBatch)) {
      return function.getResult().deepCopy();
    }
    while (sequenceReader.hasNextChunk()) {
      Statistics chunkStatistics = sequenceReader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() > endTime) {
        break;
      }
      if (sequenceReader.canUseChunkStatistics() && timeRange.contains(
          new TimeRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime()))) {
        function.updateResultFromStatistics(chunkStatistics);
        if (function.isCalculatedAggregationResult()) {
          break;
        }
        sequenceReader.skipChunkData();
        continue;
      }

      while (sequenceReader.hasNextPage()) {
        Statistics pageStatistics = sequenceReader.currentChunkStatistics();
        if (pageStatistics.getStartTime() > endTime) {
          break;
        }
        if (sequenceReader.canUsePageStatistics() && timeRange.contains(
            new TimeRange(pageStatistics.getStartTime(), pageStatistics.getEndTime()))) {
          function.updateResultFromStatistics(pageStatistics);
          if (function.isCalculatedAggregationResult()) {
            break;
          }
          sequenceReader.skipPageData();
          continue;
        }
        while (sequenceReader.hasNextBatch()) {
          BatchData batchData = sequenceReader.nextBatch();
          calcBatchData(idx, function, batchData);
          if (isEndCalc(function, lastBatch)) {
            break;
          }
        }
      }
    }
    return function.getResult().deepCopy();
  }

  private boolean isEndCalc(AggregateResult function, BatchData lastBatch) {
    return (lastBatch != null && lastBatch.hasCurrent() && lastBatch.currentTime() > endTime)
        || function.isCalculatedAggregationResult();
  }

  /**
   * @return this batchData >= endTime
   */
  private void calcBatchData(int idx, AggregateResult function, BatchData batchData)
      throws IOException {
    if (batchData == null || !batchData.hasCurrent()) {
      return;
    }
    while (batchData.hasCurrent() && batchData.currentTime() < startTime) {
      batchData.next();
    }
    if (batchData.hasCurrent()) {
      function.updateResultFromPageData(batchData, endTime);
      if (batchData.hasCurrent()) {
        batchDataList.set(idx, batchData);
      }
    }
  }
}