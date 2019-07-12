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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeqAndUnseqMergeReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeqAndUnseqMergeReaderWithExtFilter;
import org.apache.iotdb.db.query.reader.seriesRelated.SeqAndUnseqReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeriesReaderFactoryImpl implements ISeriesReaderFactory {

  private SeriesReaderFactoryImpl() {
  }

  public static SeriesReaderFactoryImpl getInstance() {
    return SeriesReaderFactoryHelper.INSTANCE;
  }

  @Override
  public IPointReader createSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
      QueryContext context) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context);

    // reader for sequence resources
    IBatchReader seqResourceIterateReader = new SeqResourceIterateReader(
        queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), timeFilter, context);

    // reader for unsequence resources
    IPointReader unseqResourceMergeReader = new UnseqResourceMergeReader(path,
        queryDataSource.getUnseqResources(), context, timeFilter);

    if (!seqResourceIterateReader.hasNext()) { //TODO here need more considerartion
      // only have unsequence data.
      return unseqResourceMergeReader;
    } else {
      // merge sequence data with unsequence data.
      return new SeqAndUnseqMergeReader(seqResourceIterateReader, unseqResourceMergeReader);
    }
  }

  @Override
  // Note that Filter for unseqMergeReader is null, because we won't push down filter in unsequence data source. More see JIRA IOTDB-121.
  // NOTE that here the filter is not pushed down on the unsequence data.
  // TODO 在这里解释执行策略
  public IPointReader createSeriesReaderWithValueFilter(Path path, Filter filter,
      QueryContext context) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context);

    // reader for sequence resources
    IBatchReader seqResourceIterateReader = new SeqResourceIterateReader(
        queryDataSource.getSeriesPath(), queryDataSource.getSeqResources(), filter, context);

    // reader for unsequence resources
    IPointReader unseqResourceMergeReader = new UnseqResourceMergeReader(path,
        queryDataSource.getUnseqResources(), context, null);

    // merge
    return new SeqAndUnseqMergeReaderWithExtFilter(seqResourceIterateReader,
        unseqResourceMergeReader, filter);
  }

  @Override
  public List<IReaderByTimestamp> createSeriesReadersByTimestamp(List<Path> paths,
      QueryContext context) throws StorageEngineException, IOException {
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path,
              context);

      // reader for sequence resources
      SeqResourceReaderByTimestamp seqResourceReaderByTimestamp = new SeqResourceReaderByTimestamp(
          path, queryDataSource.getSeqResources(), context);

      // reader for unsequence resources
      UnseqResourceReaderByTimestamp unseqResourceReaderByTimestamp = new UnseqResourceReaderByTimestamp(
          path, queryDataSource.getUnseqResources(), context);

      // merge
      SeqAndUnseqReaderByTimestamp mergeReaderByTimestamp = new SeqAndUnseqReaderByTimestamp(
          seqResourceReaderByTimestamp, unseqResourceReaderByTimestamp);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  private static class SeriesReaderFactoryHelper {

    private static final SeriesReaderFactoryImpl INSTANCE = new SeriesReaderFactoryImpl();

    private SeriesReaderFactoryHelper() {
    }
  }
}
