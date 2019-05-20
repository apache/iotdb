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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.SelectSeriesGroupEntity;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.AbstractExecutorWithoutTimeGenerator;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterExecutorWithoutTimeGenerator extends AbstractExecutorWithoutTimeGenerator {

  /**
   * Query expression
   */
  private QueryExpression queryExpression;

  /**
   * Manger for all remote query series reader resource in the query
   */
  private ClusterRpcSingleQueryManager queryManager;

  /**
   * Constructor of ClusterExecutorWithoutTimeGenerator
   */
  public ClusterExecutorWithoutTimeGenerator(QueryExpression queryExpression,
      ClusterRpcSingleQueryManager queryManager) {
    this.queryExpression = queryExpression;
    this.queryManager = queryManager;
  }

  /**
   * Execute query without filter or with only global time filter.
   */
  public QueryDataSet execute(QueryContext context)
      throws FileNodeManagerException, PathErrorException {

    Filter timeFilter = null;
    if (queryExpression.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryExpression.getExpression()).getFilter();
    }

    List<IPointReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    Map<String, SelectSeriesGroupEntity> selectSeriesGroupEntityMap = queryManager
        .getSelectSeriesGroupEntityMap();
    List<Path> paths = new ArrayList<>();
    //Mark filter series reader index group by group id
    Map<String, Integer> selectSeriesReaderIndex = new HashMap<>();
    for (Path path : queryExpression.getSelectedSeries()) {

      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());

      if (selectSeriesGroupEntityMap.containsKey(groupId)) {
        int index = selectSeriesReaderIndex.getOrDefault(groupId, 0);
        ClusterSelectSeriesReader reader = selectSeriesGroupEntityMap.get(groupId)
            .getSelectSeriesReaders().get(index);
        readersOfSelectedSeries.add(reader);
        dataTypes.add(reader.getDataType());
        selectSeriesReaderIndex.put(groupId, index + 1);
      } else {
        IPointReader reader = createSeriesReader(context, path, dataTypes, timeFilter);
        readersOfSelectedSeries.add(reader);
        paths.add(path);
      }
    }

    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), paths);

    try {
      return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
          readersOfSelectedSeries);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
  }

}
