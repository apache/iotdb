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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.SelectSeriesGroupEntity;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithOnlyTimeFilterDataSet;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Handle group by query with only time filter
 */
public class ClusterGroupByDataSetWithOnlyTimeFilter extends GroupByWithOnlyTimeFilterDataSet {

  private ClusterRpcSingleQueryManager queryManager;
  private List<IPointReader> readersOfSelectedSeries;

  /**
   * constructor.
   */
  public ClusterGroupByDataSetWithOnlyTimeFilter(long jobId,
      List<Path> paths, long unit, long origin,
      List<Pair<Long, Long>> mergedIntervals, ClusterRpcSingleQueryManager queryManager) {
    super(jobId, paths, unit, origin, mergedIntervals);
    this.queryManager  =queryManager;
    this.readersOfSelectedSeries = new ArrayList<>();
  }


  /**
   * init reader and aggregate function.
   */
  public void initGroupBy(QueryContext context, List<String> aggres, IExpression expression)
      throws FileNodeManagerException, PathErrorException, ProcessorException, IOException {
    initAggreFuction(aggres);

    /** add query token for query series which can handle locally **/
    List<Path> localQuerySeries = new ArrayList<>(selectedSeries);
    Set<Path> remoteQuerySeries = new HashSet<>();
    queryManager.getSelectSeriesGroupEntityMap().values().forEach(
        selectSeriesGroupEntity -> remoteQuerySeries
            .addAll(selectSeriesGroupEntity.getSelectPaths()));
    localQuerySeries.removeAll(remoteQuerySeries);
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), localQuerySeries);
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    Map<String, SelectSeriesGroupEntity> selectSeriesGroupEntityMap = queryManager
        .getSelectSeriesGroupEntityMap();
    //Mark filter series reader index group by group id
    Map<String, Integer> selectSeriesReaderIndex = new HashMap<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      Path path = selectedSeries.get(i);
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (selectSeriesGroupEntityMap.containsKey(groupId)) {
        int index = selectSeriesReaderIndex.getOrDefault(groupId, 0);
        ClusterSelectSeriesReader reader = selectSeriesGroupEntityMap.get(groupId)
            .getSelectSeriesReaders().get(index);
        readersOfSelectedSeries.add(reader);
        selectSeriesReaderIndex.put(groupId, index + 1);
      } else {
        readersOfSelectedSeries.add(null);
        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(selectedSeries.get(i), context);

        // sequence reader for sealed tsfile, unsealed tsfile, memory
        SequenceDataReader sequenceReader = new SequenceDataReader(
            queryDataSource.getSeqDataSource(),
            timeFilter, context, false);

        // unseq reader for all chunk groups in unSeqFile, memory
        PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
            .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);

        sequenceReaderList.add(sequenceReader);
        unSequenceReaderList.add(unSeqMergeReader);
      }
    }
  }

  @Override
  public RowRecord next() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next() "
          + "in GroupByWithOnlyTimeFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record = new RowRecord(startTime);
    for (int i = 0; i < functions.size(); i++) {
      IPointReader reader = readersOfSelectedSeries.get(i);
      if(reader != null){
        TimeValuePair timeValuePair = reader.next();
        record.addField(getField(timeValuePair.getValue().getValue(), dataTypes.get(i)));
      }else {
        AggreResultData res;
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
    }
    return record;
  }
}
