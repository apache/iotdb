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
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterSeriesGroupEntity;
import org.apache.iotdb.cluster.query.timegenerator.ClusterTimeGenerator;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
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
}
