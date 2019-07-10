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
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.SeriesReaderWithValueFilter;
import org.apache.iotdb.db.query.reader.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.query.reader.mem.MemChunkReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceSeriesReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceSeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.unsequence.DiskChunkReader;
import org.apache.iotdb.db.query.reader.unsequence.DiskChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.unsequence.UnsequenceSeriesReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;

public class SeriesReaderFactoryImpl implements ISeriesReaderFactory {

  private SeriesReaderFactoryImpl() {
  }

  public static SeriesReaderFactoryImpl getInstance() {
    return SeriesReaderFactoryHelper.INSTANCE;
  }

  @Override
  public IPointReader createUnseqSeriesReader(Path seriesPath, List<TsFileResource> unseqResources,
                                              QueryContext context,
                                              Filter filter) throws IOException {
    UnsequenceSeriesReader unseqMergeReader = new UnsequenceSeriesReader();

    int priorityValue = 1;

    for (TsFileResource tsFileResource : unseqResources) {
      priorityValue = constructReaders(tsFileResource, unseqMergeReader, context, filter,
          seriesPath, priorityValue);
    }

    // TODO add external sort when needed
    return unseqMergeReader;
  }

  private int constructReaders(TsFileResource tsFileResource,
      UnsequenceSeriesReader unseqMergeReader, QueryContext context, Filter filter,
      Path seriesPath, int priorityValue)
      throws IOException {
    int newPriority = constructChunkReader(tsFileResource, seriesPath,
        context, filter, unseqMergeReader, priorityValue);

    // add reader for MemTable
    if (!tsFileResource.isClosed()) {
      unseqMergeReader.addReaderWithPriority(
          new MemChunkReader(tsFileResource.getReadOnlyMemChunk(), filter), newPriority++);
    }
    return newPriority;
  }

  private int constructChunkReader(TsFileResource tsFileResource, Path seriesPath,
      QueryContext context, Filter filter, UnsequenceSeriesReader unseqMergeReader, int priority)
      throws IOException {
    int newPriority = priority;
    // store only one opened file stream into manager, to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(tsFileResource, tsFileResource.isClosed());

    // get modified chunk metadatas
    List<ChunkMetaData> metaDataList;
    if (tsFileResource.isClosed()) {
      MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
      metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
      // mod
      List<Modification> pathModifications = context
          .getPathModifications(tsFileResource.getModFile(),
              seriesPath.getFullPath());
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
    } else {
      metaDataList = tsFileResource.getChunkMetaDatas();
    }

    // add readers for chunks
    // TODO future advancement: decrease the duplicated code
    ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
    for (ChunkMetaData chunkMetaData : metaDataList) {

      DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
          chunkMetaData.getEndTime(),
          chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
          chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
          chunkMetaData.getTsDataType());

      if (filter != null && !filter.satisfy(digest)) {
        continue;
      }

      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      ChunkReader chunkReader = filter != null ? new ChunkReaderWithFilter(chunk, filter)
          : new ChunkReaderWithoutFilter(chunk);

      unseqMergeReader.addReaderWithPriority(new DiskChunkReader(chunkReader), newPriority);

      newPriority++;
    }
    return newPriority;
  }


  private SeriesReaderByTimestamp createUnseqSeriesReaderByTimestamp(Path seriesPath,
                                                                     List<TsFileResource> unseqResources, QueryContext context) throws IOException {
    SeriesReaderByTimestamp unseqMergeReader = new SeriesReaderByTimestamp();

    int priorityValue = 1;

    for (TsFileResource tsFileResource : unseqResources) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
              .get(tsFileResource, tsFileResource.isClosed());

      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        // mod
        List<Modification> pathModifications = context
                .getPathModifications(tsFileResource.getModFile(),
                        seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResource.getChunkMetaDatas();
      }

      // add reader for chunk
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        unseqMergeReader.addReaderWithPriority(new DiskChunkReaderByTimestamp(chunkReader),
                priorityValue);

        priorityValue++;
      }

      // add reader for MemTable
      if (!tsFileResource.isClosed()) {
        unseqMergeReader.addReaderWithPriority(
                new MemChunkReaderByTimestamp(tsFileResource.getReadOnlyMemChunk()), priorityValue++);
      }
    }

    // TODO add external sort when needed
    return unseqMergeReader;
  }

  @Override
  public List<IReaderByTimeStamp> createSeriesReadersByTimestamp(List<Path> paths,
                                                                 QueryContext context) throws StorageEngineException, IOException {
    List<IReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path,
              context);

      SeriesReaderByTimestamp mergeReaderByTimestamp = new SeriesReaderByTimestamp();

      // reader for sequence data
      SequenceSeriesReaderByTimestamp tsFilesReader = new SequenceSeriesReaderByTimestamp(path,
              queryDataSource.getSeqResources(), context);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unSequence data
      SeriesReaderByTimestamp unSeqMergeReader = createUnseqSeriesReaderByTimestamp(path,
              queryDataSource.getUnseqResources(), context);
      mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  @Override
  public IPointReader createSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
                                                           QueryContext context)
          throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
              .getQueryDataSource(path, context);

    // sequence reader for one sealed tsfile
    SequenceSeriesReader tsFilesReader;

    tsFilesReader = new SequenceSeriesReader(queryDataSource.getSeriesPath(),
            queryDataSource.getSeqResources(),
            timeFilter, context);

    // unseq reader for all chunk groups in unseqFile
    IPointReader unseqMergeReader;
    unseqMergeReader = createUnseqSeriesReader(path, queryDataSource.getUnseqResources(), context,
            timeFilter);

    if (!tsFilesReader.hasNext()) {
      //only have unsequence data.
      return unseqMergeReader;
    } else {
      //merge sequence data with unsequence data.
      return new SeriesReaderWithoutValueFilter(tsFilesReader, unseqMergeReader);
    }
  }

  @Override
  public IPointReader createSeriesReaderWithValueFilter(Path path, Filter filter, QueryContext context)
          throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
              .getQueryDataSource(path, context);

    // sequence reader for one sealed tsfile
    SequenceSeriesReader tsFilesReader;

    tsFilesReader = new SequenceSeriesReader(queryDataSource.getSeriesPath(),
            queryDataSource.getSeqResources(),
            filter, context);

    // unseq reader for all chunk groups in unseqFile. Filter for unseqMergeReader is null, because
    // we won't push down filter in unsequence data source.
    IPointReader unseqMergeReader;
    unseqMergeReader = createUnseqSeriesReader(path, queryDataSource.getUnseqResources(), context, null);

    return new SeriesReaderWithValueFilter(tsFilesReader, unseqMergeReader, filter);
  }

  private static class SeriesReaderFactoryHelper {

    private static final SeriesReaderFactoryImpl INSTANCE = new SeriesReaderFactoryImpl();

    private SeriesReaderFactoryHelper() {
    }
  }
}
