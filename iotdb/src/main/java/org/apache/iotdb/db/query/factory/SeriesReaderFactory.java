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
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SealedTsFilesReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeriesReaderFactory {

  private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactory.class);

  private SeriesReaderFactory() {
  }

  public static SeriesReaderFactory getInstance() {
    return SeriesReaderFactoryHelper.INSTANCE;
  }

  /**
   * This method is used to create unseq file reader for IoTDB request, such as query, aggregation
   * and groupby request. Note that, job id equals -1 meant that this method is used for IoTDB merge
   * process, it's no need to maintain the opened file stream.
   */
  public PriorityMergeReader createUnSeqMergeReader(
      OverflowSeriesDataSource overflowSeriesDataSource, Filter filter)
      throws IOException {

    PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();

    int priorityValue = 1;

    for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource
        .getOverflowInsertFileList()) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
          .get(overflowInsertFile.getFilePath(), false);

      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

      for (ChunkMetaData chunkMetaData : overflowInsertFile.getChunkMetaDataList()) {

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

        unSeqMergeReader
            .addReaderWithPriority(new EngineChunkReader(chunkReader, unClosedTsFileReader),
                priorityValue);
        priorityValue++;
      }
    }

    // add reader for MemTable
    if (overflowSeriesDataSource.hasRawChunk()) {
      unSeqMergeReader.addReaderWithPriority(
          new MemChunkReader(overflowSeriesDataSource.getReadableMemChunk(), filter),
          priorityValue);
    }

    // TODO add external sort when needed
    return unSeqMergeReader;
  }

  // TODO createUnSeqMergeReaderByTime a method with filter

  /**
   * This method is used to construct reader for merge process in IoTDB. To merge only one TsFile
   * data and one UnSeqFile data.
   */
  public IReader createSeriesReaderForMerge(TsFileResource tsFileResource,
      OverflowSeriesDataSource overflowSeriesDataSource,
      SingleSeriesExpression singleSeriesExpression,
      QueryContext context)
      throws IOException {

    logger.debug("Create seriesReaders for merge. SeriesFilter = {}. TsFilePath = {}",
        singleSeriesExpression,
        tsFileResource.getFilePath());

    // sequence reader
    IBatchReader seriesInTsFileReader = createSealedTsFileReaderForMerge(tsFileResource,
        singleSeriesExpression, context);

    // unSequence merge reader
    IPointReader unSeqMergeReader = createUnSeqMergeReader(overflowSeriesDataSource,
        singleSeriesExpression.getFilter());
    if (!seriesInTsFileReader.hasNext()) {
      // only have unsequence data.
      return unSeqMergeReader;
    } else {
      // merge sequence data with unsequence data.
      return new AllDataReader(seriesInTsFileReader, unSeqMergeReader);
    }

  }

  private IBatchReader createSealedTsFileReaderForMerge(TsFileResource fileNode,
      SingleSeriesExpression singleSeriesExpression,
      QueryContext context)
      throws IOException {
    TsFileSequenceReader tsFileSequenceReader = FileReaderManager.getInstance()
        .get(fileNode.getFilePath(), true);
    ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);
    MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(tsFileSequenceReader);
    List<ChunkMetaData> metaDataList = metadataQuerier
        .getChunkMetaDataList(singleSeriesExpression.getSeriesPath());

    List<Modification> modifications = context.getPathModifications(fileNode.getModFile(),
        singleSeriesExpression.getSeriesPath().getFullPath());
    QueryUtils.modifyChunkMetaData(metaDataList, modifications);

    FileSeriesReader seriesInTsFileReader = new FileSeriesReaderWithFilter(chunkLoader,
        metaDataList,
        singleSeriesExpression.getFilter());
    return new SealedTsFilesReader(seriesInTsFileReader, context);
  }

  /**
   * construct ByTimestampReader, include sequential data and unsequential data.
   *
   * @param jobId query jobId
   * @param paths selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  public static List<EngineReaderByTimeStamp> getByTimestampReadersOfSelectedPaths(long jobId,
      List<Path> paths, QueryContext context) throws IOException, FileNodeManagerException {

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path,
          context);

      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
          null, context);

      // reader for unSequence data
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);

      if (!tsFilesReader.hasNext()) {
        mergeReaderByTimestamp
            .addReaderWithPriority(unSeqMergeReader, PriorityMergeReader.HIGH_PRIORITY);
      } else {
        mergeReaderByTimestamp
            .addReaderWithPriority(new AllDataReader(tsFilesReader, unSeqMergeReader),
                PriorityMergeReader.HIGH_PRIORITY);
      }

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  private static class SeriesReaderFactoryHelper {

    private static final SeriesReaderFactory INSTANCE = new SeriesReaderFactory();

    private SeriesReaderFactoryHelper() {
    }
  }
}