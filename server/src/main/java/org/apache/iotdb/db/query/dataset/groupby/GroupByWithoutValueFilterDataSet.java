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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.resourceRelated.OldUnseqResourceMergeReader;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private List<IPointReader> unSequenceReaderList;
  private List<IAggregateReader> sequenceReaderList;
  private List<BatchData> batchDataList;
  private List<Boolean> hasCachedSequenceDataList;
  private Filter timeFilter;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws PathException, IOException, StorageEngineException {
    super(context, groupByPlan);

    this.unSequenceReaderList = new ArrayList<>();
    this.sequenceReaderList = new ArrayList<>();
    this.timeFilter = null;
    this.hasCachedSequenceDataList = new ArrayList<>();
    this.batchDataList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      hasCachedSequenceDataList.add(false);
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
    for (Path path : paths) {
      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context);
      timeFilter = queryDataSource.updateTimeFilter(timeFilter);

      // sequence reader for sealed tsfile, unsealed tsfile, memory
      IAggregateReader seqResourceIterateReader = new SeqResourceIterateReader(
          queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), timeFilter, context,
          false);

      // unseq reader for all chunk groups in unSeqFile, memory
      IPointReader unseqResourceMergeReader = new OldUnseqResourceMergeReader(
          queryDataSource.getSeriesPath(), queryDataSource.getUnseqResources(), context,
          timeFilter);

      sequenceReaderList.add(seqResourceIterateReader);
      unSequenceReaderList.add(unseqResourceMergeReader);
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
    IPointReader unsequenceReader = unSequenceReaderList.get(idx);
    IAggregateReader sequenceReader = sequenceReaderList.get(idx);
    AggregateFunction function = functions.get(idx);
    function.init();

    // skip the points with timestamp less than startTime
    skipBeforeStartTimeData(idx, sequenceReader, unsequenceReader);

    // cal group by in batch data
    boolean finishCheckSequenceData = calGroupByInBatchData(idx, function, unsequenceReader);
    if (finishCheckSequenceData) {
      // check unsequence data
      function.calculateValueFromUnsequenceReader(unsequenceReader, endTime);
      return function.getResult().deepCopy();
    }

    // continue checking sequence data
    while (sequenceReader.hasNextBatch()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();

      // memory data
      if (pageHeader == null) {
        batchDataList.set(idx, sequenceReader.nextBatch());
        hasCachedSequenceDataList.set(idx, true);
        finishCheckSequenceData = calGroupByInBatchData(idx, function, unsequenceReader);
      } else {
        // page data
        long minTime = pageHeader.getStartTime();
        long maxTime = pageHeader.getEndTime();
        // no point in sequence data with a timestamp less than endTime
        if (minTime >= endTime) {
          finishCheckSequenceData = true;
        } else if (canUseHeader(minTime, maxTime, unsequenceReader, function)) {
          // cal using page header
          function.calculateValueFromPageHeader(pageHeader);
          sequenceReader.skipPageData();
        } else {
          // cal using page data
          batchDataList.set(idx, sequenceReader.nextBatch());
          hasCachedSequenceDataList.set(idx, true);
          finishCheckSequenceData = calGroupByInBatchData(idx, function, unsequenceReader);
        }

        if (finishCheckSequenceData) {
          break;
        }
      }
    }
    // cal using unsequence data
    function.calculateValueFromUnsequenceReader(unsequenceReader, endTime);
    return function.getResult().deepCopy();
  }

  /**
   * calculate groupBy's result in batch data.
   *
   * @param idx              series index
   * @param function         aggregate function of the series
   * @param unsequenceReader unsequence reader of the series
   * @return if all sequential data been computed
   */
  private boolean calGroupByInBatchData(int idx, AggregateFunction function,
      IPointReader unsequenceReader)
      throws IOException, QueryProcessException {
    BatchData batchData = batchDataList.get(idx);
    boolean hasCachedSequenceData = hasCachedSequenceDataList.get(idx);
    boolean finishCheckSequenceData = false;
    // there was unprocessed data in last batch
    if (hasCachedSequenceData && batchData.hasCurrent()) {
      function.calculateValueFromPageData(batchData, unsequenceReader, endTime);
    }

    if (hasCachedSequenceData && batchData.hasCurrent()) {
      finishCheckSequenceData = true;
    } else {
      hasCachedSequenceData = false;
    }
    batchDataList.set(idx, batchData);
    hasCachedSequenceDataList.set(idx, hasCachedSequenceData);
    return finishCheckSequenceData;
  }

  /**
   * skip the points with timestamp less than startTime.
   *
   * @param idx              the index of series
   * @param sequenceReader   sequence Reader
   * @param unsequenceReader unsequence Reader
   * @throws IOException exception when reading file
   */
  private void skipBeforeStartTimeData(int idx, IAggregateReader sequenceReader,
      IPointReader unsequenceReader)
      throws IOException {

    // skip the unsequenceReader points with timestamp less than startTime
    skipPointInUnsequenceData(unsequenceReader);

    // skip the cached batch data points with timestamp less than startTime
    if (skipPointInBatchData(idx)) {
      return;
    }

    // skip the points in sequenceReader data whose timestamp are less than startTime
    while (sequenceReader.hasNextBatch()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      // memory data
      if (pageHeader == null) {
        batchDataList.set(idx, sequenceReader.nextBatch());
        hasCachedSequenceDataList.set(idx, true);
        if (skipPointInBatchData(idx)) {
          return;
        }
      } else {
        // page data

        // timestamps of all points in the page are less than startTime
        if (pageHeader.getEndTime() < startTime) {
          sequenceReader.skipPageData();
          continue;
        } else if (pageHeader.getStartTime() >= startTime) {
          // timestamps of all points in the page are greater or equal to startTime, needn't to skip
          return;
        }
        // the page has overlap with startTime
        batchDataList.set(idx, sequenceReader.nextBatch());
        hasCachedSequenceDataList.set(idx, true);
        if (skipPointInBatchData(idx)) {
          return;
        }
      }
    }
  }

  /**
   * skip points in unsequence reader whose timestamp is less than startTime.
   *
   * @param unsequenceReader unsequence reader
   */
  private void skipPointInUnsequenceData(IPointReader unsequenceReader) throws IOException {
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < startTime) {
      unsequenceReader.next();
    }
  }

  /**
   * skip points in batch data whose timestamp is less than startTime.
   *
   * @param idx series index
   * @return whether has next in batch data
   */
  private boolean skipPointInBatchData(int idx) {
    BatchData batchData = batchDataList.get(idx);
    boolean hasCachedSequenceData = hasCachedSequenceDataList.get(idx);
    if (!hasCachedSequenceData) {
      return false;
    }

    // skip the cached batch data points with timestamp less than startTime
    while (batchData.hasCurrent() && batchData.currentTime() < startTime) {
      batchData.next();
    }
    batchDataList.set(idx, batchData);
    if (batchData.hasCurrent()) {
      return true;
    } else {
      hasCachedSequenceDataList.set(idx, false);
      return false;
    }
  }

  private boolean canUseHeader(long minTime, long maxTime, IPointReader unSequenceReader,
      AggregateFunction function)
      throws IOException, QueryProcessException {
    if (timeFilter != null && !timeFilter.containStartEndTime(minTime, maxTime)) {
      return false;
    }

    TimeRange range = new TimeRange(startTime, endTime - 1);
    if (!range.contains(new TimeRange(minTime, maxTime))) {
      return false;
    }

    // cal unsequence data with timestamps between pages.
    function.calculateValueFromUnsequenceReader(unSequenceReader, minTime);

    return !(unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp() <= maxTime);
  }
}
