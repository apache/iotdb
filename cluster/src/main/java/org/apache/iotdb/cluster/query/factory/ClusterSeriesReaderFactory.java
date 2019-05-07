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
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Reader factory for cluster
 */
public class ClusterSeriesReaderFactory {

  /**
   * Construct ReaderByTimestamp , include sequential data and unsequential data.
   *
   * @param paths selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  public static List<EngineReaderByTimeStamp> createReadersByTimestampOfSelectedPaths(
      List<Path> paths, QueryContext context, ClusterRpcSingleQueryManager queryManager)
      throws IOException, FileNodeManagerException {

    Map<Path, ClusterSelectSeriesReader> selectSeriesReaders = queryManager.getSelectSeriesReaders();
    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      if (selectSeriesReaders.containsKey(path)) {
        readersOfSelectedSeries.add(selectSeriesReaders.get(path));
      } else {
        /** can handle series query locally **/
        EngineReaderByTimeStamp readerByTimeStamp = createReaderByTimeStamp(path, context);
        readersOfSelectedSeries.add(readerByTimeStamp);
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
