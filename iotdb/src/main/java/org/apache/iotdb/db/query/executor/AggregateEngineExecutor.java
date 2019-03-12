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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreFuncFactory;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.AggregateDataSet;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class AggregateEngineExecutor {

  private long jobId;
  private List<Path> selectedSeries;
  private List<String> aggres;
  private IExpression expression;

  /**
   * aggregation batch calculation size.
   **/
  private int aggregateFetchSize = 10 * IoTDBDescriptor.getInstance().getConfig().fetchSize;

  /**
   * constructor.
   */
  public AggregateEngineExecutor(long jobId, List<Path> selectedSeries, List<String> aggres,
      IExpression expression) {
    this.jobId = jobId;
    this.selectedSeries = selectedSeries;
    this.aggres = aggres;
    this.expression = expression;
  }

  /**
   * execute aggregate function with only time filter or no filter.
   *
   * @param context query context
   */
  public AggregateDataSet executeWithOutTimeGenerator(QueryContext context)
      throws FileNodeManagerException, IOException, PathErrorException, ProcessorException {
    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedSeries);

    List<SequenceDataReader> readersOfSequenceData = new ArrayList<>();
    List<IPointReader> readersOfUnSequenceData = new ArrayList<>();
    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      //construct AggregateFunction
      TSDataType tsDataType = MManager.getInstance()
          .getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
      function.init();
      aggregateFunctions.add(function);

      QueryDataSource queryDataSource = QueryDataSourceManager
          .getQueryDataSource(jobId, selectedSeries.get(i), context);

      // sequence reader for sealed tsfile, unsealed tsfile, memory
      SequenceDataReader sequenceReader = null;
      if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
        sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
            context, true);
      } else {
        sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
            context, false);
      }

      // unseq reader for all chunk groups in unSeqFile, memory
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);
      readersOfSequenceData.add(sequenceReader);
      readersOfUnSequenceData.add(unSeqMergeReader);
    }

    List<BatchData> batchDatas = new ArrayList<BatchData>();
    //TODO use multi-thread
    for (int i = 0; i < selectedSeries.size(); i++) {
      BatchData batchData = aggregateWithOutTimeGenerator(aggregateFunctions.get(i),
          readersOfSequenceData.get(i), readersOfUnSequenceData.get(i));
      batchDatas.add(batchData);
    }
    return constructDataSet(batchDatas);
  }

  private BatchData aggregateWithOutTimeGenerator(AggregateFunction function,
      SequenceDataReader sequenceReader, IPointReader unSequenceReader)
      throws IOException, ProcessorException {
    if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
      return handleLastMaxTimeWithOutTimeGenerator(function, sequenceReader, unSequenceReader);
    }

    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //judge if overlap with unsequence data
      if (canUseHeader(pageHeader, unSequenceReader)) {
        //cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();
      } else {
        //cal by pageData
        function.calculateValueFromPageData(sequenceReader.nextBatch(), unSequenceReader);
      }
    }

    //cal with unsequence data
    if (unSequenceReader.hasNext()) {
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }

  private boolean canUseHeader(PageHeader pageHeader, IPointReader unSequenceReader)
      throws IOException {
    //if page data is memory data.
    if (pageHeader == null) {
      return false;
    }

    long minTime = pageHeader.getMinTimestamp();
    long maxTime = pageHeader.getMaxTimestamp();
    while (unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp() <= maxTime) {
      if (minTime <= unSequenceReader.current().getTimestamp()) {
        return false;
      }
      unSequenceReader.next();
    }
    return true;
  }

  /**
   * handle last and max_time aggregate function with only time filter or no filter.
   *
   * @param function aggregate function
   * @param sequenceReader sequence data reader
   * @param unSequenceReader unsequence data reader
   * @return BatchData-aggregate result
   */
  private BatchData handleLastMaxTimeWithOutTimeGenerator(AggregateFunction function,
      SequenceDataReader sequenceReader, IPointReader unSequenceReader)
      throws IOException, ProcessorException {
    long lastBatchTimeStamp = Long.MIN_VALUE;

    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //judge if overlap with unsequence data
      if (canUseHeader(pageHeader, unSequenceReader)) {
        //cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();

        if (lastBatchTimeStamp > pageHeader.getMinTimestamp()) {
          //the chunk is end.
          break;
        } else {
          //current page and last page are in the same chunk.
          lastBatchTimeStamp = pageHeader.getMinTimestamp();
        }
      } else {
        //cal by pageData
        BatchData batchData = sequenceReader.nextBatch();
        if (lastBatchTimeStamp > batchData.currentTime()) {
          //the chunk is end.
          break;
        } else {
          //current page and last page are in the same chunk.
          lastBatchTimeStamp = batchData.currentTime();
        }
        function.calculateValueFromPageData(batchData, unSequenceReader);

      }
    }

    //cal with unsequence data
    if (unSequenceReader.hasNext()) {
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }


  /**
   * execute aggregate function with value filter.
   * @param context query context.
   */
  public AggregateDataSet executeWithTimeGenerator(QueryContext context)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException {
    QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedSeries);
    QueryTokenManager.getInstance().beginQueryOfGivenExpression(jobId, expression);

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(jobId, expression, context);
    List<EngineReaderByTimeStamp> readersOfSelectedSeries = getReadersOfSelectedPaths(
        selectedSeries, context);

    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType type = MManager.getInstance().getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), type);
      function.init();
      aggregateFunctions.add(function);
    }
    List<BatchData> batchDatas = aggregateWithTimeGenerator(aggregateFunctions, timestampGenerator,
        readersOfSelectedSeries);
    return constructDataSet(batchDatas);
  }

  private List<BatchData> aggregateWithTimeGenerator(List<AggregateFunction> aggregateFunctions,
      EngineTimeGenerator timestampGenerator,
      List<EngineReaderByTimeStamp> readersOfSelectedSeries)
      throws IOException, ProcessorException {

    while (timestampGenerator.hasNext()) {

      //generate timestamps for aggregate
      List<Long> timestamps = new ArrayList<>(aggregateFetchSize);
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timestamps.add(timestampGenerator.next());
      }

      //cal part of aggregate result
      for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
        aggregateFunctions.get(i)
            .calcAggregationUsingTimestamps(timestamps, readersOfSelectedSeries.get(i));
      }

    }

    List<BatchData> batchDataList = new ArrayList<>();
    for (AggregateFunction function : aggregateFunctions) {
      batchDataList.add(function.getResult());
    }
    return batchDataList;
  }

  private List<EngineReaderByTimeStamp> getReadersOfSelectedPaths(List<Path> paths,
      QueryContext context)
      throws IOException, FileNodeManagerException {

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path,
          context);

      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
          null, context);

      // reader for unSequence data
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);

      if (tsFilesReader == null || !tsFilesReader.hasNext()) {
        mergeReaderByTimestamp
            .addReaderWithPriority(unSeqMergeReader, PriorityMergeReader.HIGH_PRIORITY);
      } else {
        mergeReaderByTimestamp
            .addReaderWithPriority(new AllDataReader(tsFilesReader, unSeqMergeReader),
                PriorityMergeReader.HIGH_PRIORITY);
      }

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  private AggregateDataSet constructDataSet(List<BatchData> batchDataList) throws IOException {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (BatchData batchData : batchDataList) {
      dataTypes.add(batchData.getDataType());
    }
    return new AggregateDataSet(selectedSeries, aggres, dataTypes, batchDataList);
  }

}
