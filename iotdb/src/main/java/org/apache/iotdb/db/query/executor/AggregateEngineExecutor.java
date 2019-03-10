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
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreFuncFactory;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.AggregateDataSet;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class AggregateEngineExecutor {
  private long jobId;
  private List<Path> selectedSeries;
  private List<String> aggres;
  private IExpression expression;

  public AggregateEngineExecutor(long jobId, List<Path> selectedSeries, List<String> aggres, IExpression expression) {
    this.jobId = jobId;
    this.selectedSeries = selectedSeries;
    this.aggres = aggres;
    this.expression = expression;
  }

  public AggregateDataSet executeWithOutTimeGenerator(QueryContext context)
      throws FileNodeManagerException, IOException, PathErrorException, ProcessorException {
    Filter timeFilter = null;
    if(timeFilter!=null){
      timeFilter = ((GlobalTimeExpression)expression).getFilter();
    }
    QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedSeries);

    List<IAggregateReader> readersOfSequenceData = new ArrayList<>();
    List<IPointReader> readersOfUnSequenceData = new ArrayList<>();
    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for(int i = 0; i < selectedSeries.size(); i++){
      //construct AggregateFunction
      TSDataType tsDataType = MManager.getInstance().getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
      function.init();
      aggregateFunctions.add(function);

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, selectedSeries.get(i), context);
      // sequence reader for sealed tsfile, unsealed tsfile, memory
      SequenceDataReader sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter, context);
      // unseq reader for all chunk groups in unSeqFile, memory
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);
      readersOfSequenceData.add(sequenceReader);
      readersOfUnSequenceData.add(unSeqMergeReader);
    }

    List<BatchData> batchDatas = new ArrayList<BatchData>();
    //TODO use multi-thread
    for(int i = 0; i < selectedSeries.size(); i++){
      BatchData batchData = aggregateWithOutTimeGenerator(aggregateFunctions.get(i), readersOfSequenceData.get(i), readersOfUnSequenceData.get(i));
      batchDatas.add(batchData);
    }
    return constructDataSet(batchDatas);
  }

  private BatchData aggregateWithOutTimeGenerator(AggregateFunction function, IAggregateReader sequenceReader, IPointReader unSequenceReader)
      throws IOException, ProcessorException {
    if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc){
      //TODO Optimization
      return function.getResult();
    }

    while (sequenceReader.hasNext()){
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //judge if overlap with unsequence data
      if(canUseHeader(pageHeader, unSequenceReader)){
        //cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();
      }
      else {
        //cal by pageData
        function.calculateValueFromPageData(sequenceReader.nextBatch(), unSequenceReader);
      }
    }

    //cal with unsequence data
    if(unSequenceReader.hasNext()){
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }

  private boolean canUseHeader(PageHeader pageHeader, IPointReader unSequenceReader)
      throws IOException {
    //if page data is memory data.
    if(pageHeader == null){
      return false;
    }

    long minTime = pageHeader.getMinTimestamp();
    long maxTime = pageHeader.getMaxTimestamp();
    while(unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp() <= maxTime){
      if(minTime <= unSequenceReader.current().getTimestamp()){
        return false;
      }
      unSequenceReader.next();
    }
    return true;
  }

  public AggregateDataSet executeWithTimeGenerator(QueryContext context){
    return null;
  }

  private AggregateDataSet constructDataSet(List<BatchData> batchDataList) throws IOException {
    List<TSDataType> dataTypes = new ArrayList<>();
    for(BatchData batchData : batchDataList){
      dataTypes.add(batchData.getDataType());
    }
    return new AggregateDataSet(selectedSeries,aggres,dataTypes, batchDataList);
  }


}
