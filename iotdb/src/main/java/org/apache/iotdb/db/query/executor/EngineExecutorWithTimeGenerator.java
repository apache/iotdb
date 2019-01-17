/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * IoTDB query executor with filter.
 */
public class EngineExecutorWithTimeGenerator {

  private QueryExpression queryExpression;
  private long jobId;

  EngineExecutorWithTimeGenerator(long jobId, QueryExpression queryExpression) {
    this.jobId = jobId;
    this.queryExpression = queryExpression;
  }

  /**
   * execute query.
   *
   * @return QueryDataSet object
   * @throws IOException IOException
   * @throws FileNodeManagerException FileNodeManagerException
   */
  public QueryDataSet execute() throws IOException, FileNodeManagerException {

    QueryTokenManager.getInstance()
        .beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());
    QueryTokenManager.getInstance()
        .beginQueryOfGivenExpression(jobId, queryExpression.getExpression());

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(jobId,
        queryExpression.getExpression());

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = getReadersOfSelectedPaths(
        queryExpression.getSelectedSeries());

    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : queryExpression.getSelectedSeries()) {
      try {
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

    }
    return new EngineDataSetWithTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
        timestampGenerator,
        readersOfSelectedSeries);
  }

  private List<EngineReaderByTimeStamp> getReadersOfSelectedPaths(List<Path> paths)
      throws IOException, FileNodeManagerException {

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
          null);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unSequence data
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
      mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

}
