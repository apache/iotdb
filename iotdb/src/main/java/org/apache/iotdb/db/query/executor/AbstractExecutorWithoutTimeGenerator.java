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
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Query executor with global time filter.
 */
public abstract class AbstractExecutorWithoutTimeGenerator {

  /**
   * Create reader of a series
   *
   * @param context query context
   * @param path series path
   * @param dataTypes list of data type
   * @param timeFilter time filter
   * @return reader of the series
   */
  public static IPointReader createSeriesReader(QueryContext context, Path path,
      List<TSDataType> dataTypes, Filter timeFilter)
      throws FileNodeManagerException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance().getQueryDataSource(path,
        context);
    // add data type
    try {
      dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
    } catch (PathErrorException e) {
      throw new FileNodeManagerException(e);
    }

    // sequence reader for one sealed tsfile
    SequenceDataReader tsFilesReader;
    try {
      tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
          timeFilter, context);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }

    // unseq reader for all chunk groups in unSeqFile
    PriorityMergeReader unSeqMergeReader;
    try {
      unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
    // merge sequence data with unsequence data.
    return new AllDataReader(tsFilesReader, unSeqMergeReader);
  }
}
