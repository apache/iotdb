/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

public class GroupByWithOnlyTimeFilterDataSet extends GroupByEngine {

  protected List<IPointReader> unSequenceReaderList;
  protected List<IAggregateReader> sequenceReaderList;
  private List<BatchData> batchDataList;
  private List<Boolean> hasCachedSequenceDataList;
  private Filter timeFilter;

  /**
   * constructor.
   */
  public GroupByWithOnlyTimeFilterDataSet(long jobId, List<Path> paths, long unit, long origin,
      List<Pair<Long, Long>> mergedIntervals) {
    super(jobId, paths, unit, origin, mergedIntervals);
    this.unSequenceReaderList = new ArrayList<>();
    this.sequenceReaderList = new ArrayList<>();
    this.timeFilter = null;
    this.hasCachedSequenceDataList = new ArrayList<>();
    this.batchDataList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      hasCachedSequenceDataList.add(false);
      batchDataList.add(null);
    }
  }

  /**
   * init reader and aggregate function.
   */
  public void initGroupBy(QueryContext context, List<String> aggres, IExpression expression)
      throws FileNodeManagerException, PathErrorException, ProcessorException, IOException {
    initAggreFuction(aggres);
    //init reader
    QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedSeries);
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    for (int i = 0; i < selectedSeries.size(); i++) {
      QueryDataSource queryDataSource = QueryDataSourceManager
          .getQueryDataSource(jobId, selectedSeries.get(i), context);

      // sequence reader for sealed tsfile, unsealed tsfile, memory
      SequenceDataReader sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
          timeFilter, context, false);

      // unseq reader for all chunk groups in unSeqFile, memory
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);

      sequenceReaderList.add(sequenceReader);
      unSequenceReaderList.add(unSeqMergeReader);
    }

  }

  @Override
  public RowRecord next() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() in GroupByWithOnlyTimeFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record = new RowRecord(startTime);
    for (int i = 0; i < functions.size(); i++) {
      AggreResultData res = null;
      try {
        res = nextSeries(i);
      } catch (ProcessorException e) {
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

  private AggreResultData nextSeries(int idx) throws IOException, ProcessorException {
    IPointReader unsequenceReader = unSequenceReaderList.get(idx);
    IAggregateReader sequenceReader = sequenceReaderList.get(idx);
    AggregateFunction function = functions.get(idx);
    function.init();
    boolean finishCheckSequenceData = false;

    //skip the points with timestamp less than startTime
    skipExessData(idx, sequenceReader, unsequenceReader);

    BatchData batchData = batchDataList.get(idx);
    boolean hasCachedSequenceData = hasCachedSequenceDataList.get(idx);
    //there was unprocessed data in last batch
    if (hasCachedSequenceData && batchData.hasNext()) {
      function.calculateValueFromPageData(batchData, unsequenceReader, endTime);
    }

    if (hasCachedSequenceData && batchData.hasNext()) {
      finishCheckSequenceData = true;
    } else {
      hasCachedSequenceData = false;
    }

    if (finishCheckSequenceData) {
      //check unsequence data
      function.calculateValueFromUnsequenceReader(unsequenceReader, endTime);
      return function.getResult().deepCopy();
    }

    //continue checking sequence data
    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();

      //memory data
      if (pageHeader == null) {
        batchData = sequenceReader.nextBatch();
        function.calculateValueFromPageData(batchData, unsequenceReader, endTime);
        //no point in sequence data with a timestamp less than endTime
        if (batchData.hasNext()) {
          hasCachedSequenceData = true;
          break;
        }
      }

      //page data
      long minTime = pageHeader.getMinTimestamp();
      long maxTime = pageHeader.getMaxTimestamp();
      //no point in sequence data with a timestamp less than endTime
      if (minTime >= endTime) {
        hasCachedSequenceData = true;
        batchData = sequenceReader.nextBatch();
        break;
      }

      if (canUseHeader(minTime, maxTime, unsequenceReader, function)) {
        //cal using page header
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();
      } else {
        //cal using page data
        batchData = sequenceReader.nextBatch();
        function.calculateValueFromPageData(batchData, unsequenceReader, endTime);
        if (batchData.hasNext()) {
          hasCachedSequenceData = true;
          break;
        }
      }

    }

    function.calculateValueFromUnsequenceReader(unsequenceReader, endTime);
    hasCachedSequenceDataList.set(idx, hasCachedSequenceData);
    batchDataList.set(idx, batchData);
    return function.getResult().deepCopy();
  }

  //skip the points with timestamp less than startTime
  private void skipExessData(int idx, IAggregateReader sequenceReader,
      IPointReader unsequenceReader)
      throws IOException {
    BatchData batchData = batchDataList.get(idx);
    boolean hasCachedSequenceData = hasCachedSequenceDataList.get(idx);

    //skip the unsequenceReader points with timestamp less than startTime
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < startTime) {
      unsequenceReader.next();
    }

    //skip the cached batch data points with timestamp less than startTime
    if (hasCachedSequenceData) {
      while (batchData.hasNext() && batchData.currentTime() < startTime) {
        batchData.next();
      }
    }
    if (hasCachedSequenceData && !batchData.hasNext()) {
      hasCachedSequenceData = false;
    } else {
      return;
    }

    //skip the points in sequenceReader data whose timestamp are less than startTime
    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //memory data
      if (pageHeader == null) {
        batchData = sequenceReader.nextBatch();
        hasCachedSequenceData = true;
        while (batchData.hasNext() && batchData.currentTime() < startTime) {
          batchData.next();
        }
        if (batchData.hasNext()) {
          break;
        } else {
          hasCachedSequenceData = false;
          continue;
        }
      }
      //timestamps of all points in the page are less than startTime
      if (pageHeader.getMaxTimestamp() < startTime) {
        sequenceReader.skipPageData();
        continue;
      }
      //timestamps of all points in the page are greater or equal to startTime, don't need to skip
      if (pageHeader.getMinTimestamp() >= startTime) {
        break;
      }
      //the page has overlap with startTime
      batchData = sequenceReader.nextBatch();
      hasCachedSequenceData = true;
      while (batchData.hasNext() && batchData.currentTime() < startTime) {
        batchData.next();
      }
      break;
    }

    batchDataList.set(idx, batchData);
    hasCachedSequenceDataList.set(idx, hasCachedSequenceData);
  }

  private boolean canUseHeader(long minTime, long maxTime, IPointReader unSequenceReader,
      AggregateFunction function)
      throws IOException, ProcessorException {
    if (timeFilter != null && !timeFilter.containStartEndTime(minTime, maxTime)) {
      return false;
    }

    //cal unsequence data with timestamps between pages.
    function.calculateValueFromUnsequenceReader(unSequenceReader, minTime);

    if (unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp() <= maxTime) {
      return false;
    }
    return true;
  }
}
