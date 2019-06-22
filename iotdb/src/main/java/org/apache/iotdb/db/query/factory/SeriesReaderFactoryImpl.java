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
package org.apache.iotdb.db.query.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReaderByTimestampV2;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReaderV2;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeriesReaderFactoryImpl implements ISeriesReaderFactory {

  @Override
  public IPointReader createUnSeqReader(Path seriesPath, List<TsFileResourceV2> unSeqResources,
      Filter filter) throws IOException {
    return null;
  }

  @Override
  public List<EngineReaderByTimeStamp> createByTimestampReadersOfSelectedPaths(List<Path> paths,
      QueryContext context) throws FileNodeManagerException {
    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSourceV2 queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSourceV2(path,
              context);

      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReaderByTimestampV2 tsFilesReader = new SequenceDataReaderByTimestampV2(path,
          queryDataSource.getSeqResources(), context);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unSequence data
      //TODO add create unseq reader
      PriorityMergeReaderByTimestamp unSeqMergeReader = createUnSeqByTimestampReader(
          queryDataSource.getUnseqResources());
      mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  private PriorityMergeReaderByTimestamp createUnSeqByTimestampReader(
      List<TsFileResourceV2> unseqResources) {
    return null;
  }

  @Override
  public IPointReader createAllDataReader(Path path, Filter timeFilter, QueryContext context)
      throws FileNodeManagerException {
    QueryDataSourceV2 queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSourceV2(path,
            context);

    // sequence reader for one sealed tsfile
    SequenceDataReaderV2 tsFilesReader;
    try {
      tsFilesReader = new SequenceDataReaderV2(queryDataSource.getSeriesPath(),
          queryDataSource.getSeqResources(),
          timeFilter, context);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }

    // unseq reader for all chunk groups in unSeqFile
    IPointReader unSeqMergeReader = null;
    try {
      unSeqMergeReader = createUnSeqReader(path, queryDataSource.getUnseqResources(), timeFilter);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
    // merge sequence data with unsequence data.
    return new AllDataReader(tsFilesReader, unSeqMergeReader);
  }

}
