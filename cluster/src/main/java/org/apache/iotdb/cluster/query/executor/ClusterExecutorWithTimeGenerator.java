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
package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.query.dataset.ClusterDataSetWithTimeGenerator;
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.reader.ClusterSeriesReader;
import org.apache.iotdb.cluster.query.timegenerator.ClusterTimeGenerator;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterExecutorWithTimeGenerator {

  private QueryExpression queryExpression;
  private ClusterRpcSingleQueryManager queryManager;

  public ClusterExecutorWithTimeGenerator(QueryExpression queryExpression,
      ClusterRpcSingleQueryManager queryManager) {
    this.queryExpression = queryExpression;
    this.queryManager = queryManager;
  }

  /**
   * execute query.
   *
   * @return QueryDataSet object
   * @throws IOException IOException
   * @throws FileNodeManagerException FileNodeManagerException
   */
  public QueryDataSet execute(QueryContext context) throws FileNodeManagerException {

    /** add query token for query series which can handle locally **/
    List<Path> localQuerySeries = new ArrayList<>(queryExpression.getSelectedSeries());
    Set<String> remoteQuerySeries = queryManager.getSelectSeriesReaders().keySet();
    remoteQuerySeries.forEach(seriesPath -> localQuerySeries.remove(new Path(seriesPath)));
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), localQuerySeries);

    /** add query token for filter series which can handle locally **/
    Set<String> deviceIdSet = new HashSet<>();
    Set<String> remoteFilterSeries = queryManager.getFilterSeriesReaders().keySet();
    remoteFilterSeries.forEach(seriesPath -> deviceIdSet.add(new Path(seriesPath).getDevice()));
    QueryResourceManager.getInstance()
        .beginQueryOfGivenExpression(context.getJobId(), queryExpression.getExpression(), deviceIdSet);

    ClusterTimeGenerator timestampGenerator;
    List<EngineReaderByTimeStamp> readersOfSelectedSeries;
    try {
      timestampGenerator = new ClusterTimeGenerator(queryExpression.getExpression(), context,
          queryManager);
      readersOfSelectedSeries = ClusterSeriesReaderFactory
          .getByTimestampReadersOfSelectedPaths(queryExpression.getSelectedSeries(), context,
              queryManager);
    } catch (IOException ex) {
      throw new FileNodeManagerException(ex);
    }

    /** Get data type of select paths **/
    List<TSDataType> dataTypes = new ArrayList<>();
    Map<String, ClusterSeriesReader> selectSeriesReaders = queryManager.getSelectSeriesReaders();
    for (Path path : queryExpression.getSelectedSeries()) {
      try {
        if (selectSeriesReaders.containsKey(path.getFullPath())) {
          dataTypes.add(selectSeriesReaders.get(path.getFullPath()).getDataType());
        } else {
          dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
        }
      }catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

    }

    return new ClusterDataSetWithTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
        timestampGenerator,
        readersOfSelectedSeries);
  }
}
