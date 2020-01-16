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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.db.query.reader.seriesRelated.IAggregateReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesDataReaderWithoutValueFilter;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private List<SeriesDataReaderWithoutValueFilter> sequenceReaderList;
  private List<BatchData> batchDataList;
  private Filter timeFilter;
  private GroupByPlan groupByPlan;

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
    this.groupByPlan = groupByPlan;
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
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult res;
      try {
        res = nextSeries(i);
      } catch (QueryProcessException e) {
        throw new IOException(e);
      }
      if (res == null) {
        record.addField(new Field(null));
      } else {
        record.addField(res.getResult(), res.getDataType());
      }
    }
    return record;
  }

  /**
   * calculate the group by result of the series indexed by idx.
   *
   * @param idx series id
   */
  private AggregateResult nextSeries(int idx) throws IOException, QueryProcessException {
    IAggregateReader sequenceReader = sequenceReaderList.get(idx);
    AggregateResult result = AggreResultFactory
        .getAggrResultByName(groupByPlan.getDeduplicatedAggregations().get(idx),
            groupByPlan.getDeduplicatedDataTypes().get(idx));

    TimeRange timeRange = new TimeRange(startTime, endTime - 1);

    BatchData lastBatch = batchDataList.get(idx);
    calcBatchData(idx, result, lastBatch);
    if (isEndCalc(result, lastBatch)) {
      return result;
    }
    while (sequenceReader.hasNextChunk()) {
      Statistics chunkStatistics = sequenceReader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() > endTime) {
        break;
      }
      if (sequenceReader.canUseCurrentChunkStatistics() && timeRange.contains(
          new TimeRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime()))) {
        result.updateResultFromStatistics(chunkStatistics);
        if (result.isCalculatedAggregationResult()) {
          break;
        }
        sequenceReader.skipChunkData();
        continue;
      }

      while (sequenceReader.hasNextPage()) {
        Statistics pageStatistics = sequenceReader.currentPageStatistics();
        if (pageStatistics.getStartTime() > endTime) {
          break;
        }
        if (sequenceReader.canUseCurrentPageStatistics() && timeRange.contains(
            new TimeRange(pageStatistics.getStartTime(), pageStatistics.getEndTime()))) {
          result.updateResultFromStatistics(pageStatistics);
          if (result.isCalculatedAggregationResult()) {
            break;
          }
          sequenceReader.skipPageData();
          continue;
        }
        while (sequenceReader.hasNextOverlappedPage()) {
          BatchData batchData = sequenceReader.nextOverlappedPage();
          calcBatchData(idx, result, batchData);
          if (isEndCalc(result, lastBatch)) {
            break;
          }
        }
      }
    }
    return result;
  }

  private boolean isEndCalc(AggregateResult function, BatchData lastBatch) {
    return (lastBatch != null && lastBatch.hasCurrent() && lastBatch.currentTime() > endTime)
        || function.isCalculatedAggregationResult();
  }

  /**
   * this batchData >= endTime
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