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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.IFillEngineExecutor;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterFillEngineExecutor implements IFillEngineExecutor {

  private List<Path> selectedSeries;
  private long queryTime;
  private Map<TSDataType, IFill> typeIFillMap;
  private ClusterRpcSingleQueryManager queryManager;


  public ClusterFillEngineExecutor(List<Path> selectedSeries, long queryTime,
      Map<TSDataType, IFill> typeIFillMap, ClusterRpcSingleQueryManager queryManager) {
    this.selectedSeries = selectedSeries;
    this.queryTime = queryTime;
    this.typeIFillMap = typeIFillMap;
    this.queryManager = queryManager;
  }

  @Override
  public QueryDataSet execute(QueryContext context)
      throws FileNodeManagerException, PathErrorException, IOException {
    List<Path> paths = new ArrayList<>();
    List<IFill> fillList = new ArrayList<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    List<IPointReader> readers = new ArrayList<>();
    Map<String, SelectSeriesGroupEntity> selectSeriesEntityMap = queryManager.getSelectSeriesGroupEntityMap();
    //Mark filter series reader index group by group id
    Map<String, Integer> selectSeriesReaderIndex = new HashMap<>();
    for (Path path : selectedSeries) {
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());

      if (selectSeriesEntityMap.containsKey(groupId)) {
        int index = selectSeriesReaderIndex.getOrDefault(groupId, 0);
        ClusterSelectSeriesReader reader = selectSeriesEntityMap.get(groupId).getSelectSeriesReaders().get(index);
        readers.add(reader);
        dataTypeList.add(reader.getDataType());
        selectSeriesReaderIndex.put(groupId, index + 1);
      } else {
        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(path, context);
        TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
        dataTypeList.add(dataType);
        IFill fill;
        if (!typeIFillMap.containsKey(dataType)) {
          fill = new PreviousFill(dataType, queryTime, 0);
        } else {
          fill = typeIFillMap.get(dataType).copy(path);
        }
        fill.setDataType(dataType);
        fill.setQueryTime(queryTime);
        fill.constructReaders(queryDataSource, context);
        fillList.add(fill);
        readers.add(fill.getFillResult());
      }
    }

    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), paths);

    return new EngineDataSetWithoutTimeGenerator(selectedSeries, dataTypeList, readers);
  }
}
