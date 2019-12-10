/*
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

package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.externalsort.ExternalSortJobEngine;
import org.apache.iotdb.db.query.externalsort.SimpleExternalSortEngine;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.chunkRelated.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

/**
 * To read a list of unsequence TsFiles, this class implements <code>IBatchReader</code> for the TsFiles.
 * <p>
 * Note that an unsequence TsFile can be either closed or unclosed. An unclosed unsequence TsFile
 * consists of data on disk and data in memtables that will be flushed to this unclosed TsFile.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter}.
 */
public class UnseqResourceMergeReader implements IBatchReader {

  private Path seriesPath;

  private PriorityMergeReader priorityMergeReader;

  /**
   * Zesong Sun !!!
   *
   * put Reader into merge reader one by one
   *
   * (1) get all ChunkMetadata
   * (2) set priority to each ChunkMetadata
   * (3) sort All ChunkMetadata by start time
   * (4) create a ChunkReader with priority for each ChunkMetadata and add the ChunkReader to merge reader one by one
   */
  public UnseqResourceMergeReader(Path seriesPath, List<TsFileResource> unseqResources,
      QueryContext context, Filter timeFilter) throws IOException {

    long queryId = context.getJobId();
    List<ChunkReaderWrap> readerWrapList = new ArrayList<>();

    for (TsFileResource tsFileResource : unseqResources) {
      // prepare metaDataList
      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        if (!ResourceRelatedUtil.isTsFileSatisfied(tsFileResource, timeFilter, seriesPath)) {
          continue;
        }

        metaDataList = DeviceMetaDataCache.getInstance().get(tsFileResource, seriesPath);
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResource.getModFile(), seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        if (tsFileResource.getEndTimeMap().size() != 0) {
          if (!ResourceRelatedUtil.isTsFileSatisfied(tsFileResource, timeFilter, seriesPath)) {
            continue;
          }
        }
        metaDataList = tsFileResource.getChunkMetaDataList();
      }

      ChunkLoaderImpl chunkLoader = null;
      if (!metaDataList.isEmpty()) {
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(tsFileResource, tsFileResource.isClosed());
        chunkLoader = new ChunkLoaderImpl(tsFileReader);
      }

      for (ChunkMetaData chunkMetaData : metaDataList) {
        if (timeFilter != null && !timeFilter.satisfy(chunkMetaData.getStatistics())) {
          continue;
        }
        // create and add DiskChunkReader
        readerWrapList.add(new ChunkReaderWrap(chunkMetaData, chunkLoader, timeFilter));
      }

      if (!tsFileResource.isClosed()) {
        // create and add MemChunkReader
        readerWrapList.add(new ChunkReaderWrap(tsFileResource.getReadOnlyMemChunk(), timeFilter));
      }
    }

    ExternalSortJobEngine externalSortJobEngine = SimpleExternalSortEngine.getInstance();
    List<IPointReader> readerList = externalSortJobEngine
        .executeForIPointReader(queryId, readerWrapList);
    int index = 1;

    priorityMergeReader = new PriorityMergeReader();
    for (IPointReader chunkReader : readerList) {
      priorityMergeReader.addReaderWithPriority(chunkReader, index++);
    }
  }


  /**
   * Zesong Sun !!!
   */
  @Override
  public boolean hasNextBatch() throws IOException {
    return false;
  }


  /**
   * Zesong Sun !!!
   */
  @Override
  public BatchData nextBatch() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
