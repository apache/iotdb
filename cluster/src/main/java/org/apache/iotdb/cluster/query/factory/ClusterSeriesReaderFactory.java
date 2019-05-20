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
package org.apache.iotdb.cluster.query.factory;

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
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Reader factory for cluster
 */
public class ClusterSeriesReaderFactory {

  private ClusterSeriesReaderFactory() {
  }

  /**
   * Construct ReaderByTimestamp , include sequential data and unsequential data. And get all series dataType.
   *
   * @param paths selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  public static List<EngineReaderByTimeStamp> createReadersByTimestampOfSelectedPaths(
      List<Path> paths, QueryContext context, ClusterRpcSingleQueryManager queryManager, List<TSDataType> dataTypes)
      throws IOException, FileNodeManagerException, PathErrorException {

    Map<String, SelectSeriesGroupEntity> selectSeriesEntityMap = queryManager
        .getSelectSeriesGroupEntityMap();
    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();
    //Mark filter series reader index group by group id
    Map<String, Integer> selectSeriesReaderIndex = new HashMap<>();

    for (Path path : paths) {
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (selectSeriesEntityMap.containsKey(groupId)) {
        int index = selectSeriesReaderIndex.getOrDefault(groupId, 0);
        ClusterSelectSeriesReader reader = selectSeriesEntityMap.get(groupId).getSelectSeriesReaders().get(index);
        readersOfSelectedSeries.add(reader);
        dataTypes.add(reader.getDataType());
        selectSeriesReaderIndex.put(groupId, index + 1);
      } else {
        /** can handle series query locally **/
        EngineReaderByTimeStamp readerByTimeStamp = createReaderByTimeStamp(path, context);
        readersOfSelectedSeries.add(readerByTimeStamp);
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      }
    }
    return readersOfSelectedSeries;
  }

  /**
   * Create single ReaderByTimestamp
   *
   * @param path series path
   * @param context query context
   */
  public static EngineReaderByTimeStamp createReaderByTimeStamp(Path path, QueryContext context)
      throws IOException, FileNodeManagerException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path,
            context);

    PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

    // reader for sequence data
    SequenceDataReaderByTimestamp tsFilesReader = new SequenceDataReaderByTimestamp(
        queryDataSource.getSeqDataSource(), context);
    mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

    // reader for unSequence data
    PriorityMergeReaderByTimestamp unSeqMergeReader = SeriesReaderFactory.getInstance()
        .createUnSeqMergeReaderByTimestamp(queryDataSource.getOverflowSeriesDataSource());
    mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);
    return mergeReaderByTimestamp;
  }
}
