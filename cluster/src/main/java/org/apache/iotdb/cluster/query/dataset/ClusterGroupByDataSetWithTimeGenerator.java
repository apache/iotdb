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
package org.apache.iotdb.cluster.query.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterSeriesGroupEntity;
import org.apache.iotdb.cluster.query.timegenerator.ClusterTimeGenerator;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

public class ClusterGroupByDataSetWithTimeGenerator extends GroupByWithValueFilterDataSet {

  private ClusterRpcSingleQueryManager queryManager;

  private List<TSDataType> selectSeriesDataTypes;

  /**
   * constructor.
   */
  public ClusterGroupByDataSetWithTimeGenerator(long jobId,
      List<Path> paths, long unit, long origin,
      List<Pair<Long, Long>> mergedIntervals, ClusterRpcSingleQueryManager queryManager) {
    super(jobId, paths, unit, origin, mergedIntervals);
    this.queryManager = queryManager;
    selectSeriesDataTypes = new ArrayList<>();
  }

  /**
   * init reader and aggregate function.
   */
  @Override
  public void initGroupBy(QueryContext context, List<String> aggres, IExpression expression)
      throws FileNodeManagerException, PathErrorException, ProcessorException, IOException {
    initAggreFuction(aggres);

    /** add query token for filter series which can handle locally **/
    Set<String> deviceIdSet = new HashSet<>();
    for (FilterSeriesGroupEntity filterSeriesGroupEntity : queryManager
        .getFilterSeriesGroupEntityMap().values()) {
      List<Path> remoteFilterSeries = filterSeriesGroupEntity.getFilterPaths();
      remoteFilterSeries.forEach(seriesPath -> deviceIdSet.add(seriesPath.getDevice()));
    }
    QueryResourceManager.getInstance()
        .beginQueryOfGivenExpression(context.getJobId(), expression, deviceIdSet);

    /** add query token for query series which can handle locally **/
    List<Path> localQuerySeries = new ArrayList<>(selectedSeries);
    Set<Path> remoteQuerySeries = new HashSet<>();
    queryManager.getSelectSeriesGroupEntityMap().values().forEach(
        selectSeriesGroupEntity -> remoteQuerySeries
            .addAll(selectSeriesGroupEntity.getSelectPaths()));
    localQuerySeries.removeAll(remoteQuerySeries);
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), localQuerySeries);

    this.timestampGenerator = new ClusterTimeGenerator(expression, context, queryManager);
    this.allDataReaderList = ClusterSeriesReaderFactory
        .createReadersByTimestampOfSelectedPaths(selectedSeries, context, queryManager,
            selectSeriesDataTypes);
  }

  @Override
  public RowRecord next() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next()"
          + " in GroupByWithOnlyTimeFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    for (AggregateFunction function : functions) {
      function.init();
    }

    long[] timestampArray = new long[timestampFetchSize];
    int timeArrayLength = 0;
    if (hasCachedTimestamp) {
      if (timestamp < endTime) {
        hasCachedTimestamp = false;
        timestampArray[timeArrayLength++] = timestamp;
      } else {
        return constructRowRecord();
      }
    }

    while (timestampGenerator.hasNext()) {
      // construct timestamp array
      timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

      fetchSelectDataFromRemoteNode(timeArrayLength, timestampArray);

      // cal result using timestamp array
      for (int i = 0; i < selectedSeries.size(); i++) {
        functions.get(i).calcAggregationUsingTimestamps(
            timestampArray, timeArrayLength, allDataReaderList.get(i));
      }

      timeArrayLength = 0;
      // judge if it's end
      if (timestamp >= endTime) {
        hasCachedTimestamp = true;
        break;
      }
    }

    // fetch select series data from remote node
    fetchSelectDataFromRemoteNode(timeArrayLength, timestampArray);

    if (timeArrayLength > 0) {
      // cal result using timestamp array
      for (int i = 0; i < selectedSeries.size(); i++) {
        functions.get(i).calcAggregationUsingTimestamps(
            timestampArray, timeArrayLength, allDataReaderList.get(i));
      }
    }
    return constructRowRecord();
  }

  /**
   * Get select series batch data by batch timestamp
   * @param timeArrayLength length of batch timestamp
   * @param timestampArray timestamp array
   */
  private void fetchSelectDataFromRemoteNode(int timeArrayLength, long[] timestampArray)
      throws IOException {
    if(timeArrayLength != 0){
      List<Long> batchTimestamp = new ArrayList<>();
      for(int i = 0 ; i < timeArrayLength; i++){
        batchTimestamp.add(timestampArray[i]);
      }

      try {
        queryManager.fetchBatchDataByTimestampForAllSelectPaths(batchTimestamp);
      } catch (
          RaftConnectionException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * construct an array of timestamps for one batch of a group by partition calculating.
   *
   * @param timestampArray timestamp array
   * @param timeArrayLength the current length of timestamp array
   * @return time array length
   */
  private int constructTimeArrayForOneCal(long[] timestampArray, int timeArrayLength)
      throws IOException {
    for (int cnt = 1; cnt < timestampFetchSize && timestampGenerator.hasNext(); cnt++) {
      timestamp = timestampGenerator.next();
      if (timestamp < endTime) {
        timestampArray[timeArrayLength++] = timestamp;
      } else {
        hasCachedTimestamp = true;
        break;
      }
    }
    return timeArrayLength;
  }
}
