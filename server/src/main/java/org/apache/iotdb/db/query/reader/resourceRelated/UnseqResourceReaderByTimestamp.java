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
import org.apache.iotdb.db.query.reader.IPointReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;

/**
 * To read a list of unsequence TsFiles by timestamp, this class extends {@link
 * PriorityMergeReaderByTimestamp} to implement <code>IReaderByTimestamp</code> for the TsFiles.
 * <p>
 * Note that an unsequence TsFile can be either closed or unclosed. An unclosed unsequence TsFile
 * consists of data on disk and data in memtables that will be flushed to this unclosed TsFile.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp}.
 */
public class UnseqResourceReaderByTimestamp extends PriorityMergeReaderByTimestamp {

  private long queryId;

  public UnseqResourceReaderByTimestamp(Path seriesPath,
      List<TsFileResource> unseqResources, QueryContext context) throws IOException {
    this.queryId = context.getQueryId();
    List<ChunkReaderWrap> chunkReaderWrapList = new ArrayList<>();
    for (TsFileResource tsFileResource : unseqResources) {

      // prepare metaDataList
      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        metaDataList = DeviceMetaDataCache.getInstance()
            .get(tsFileResource, seriesPath);
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResource.getModFile(), seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResource.getChunkMetaDataList();
      }

      ChunkLoaderImpl chunkLoader = null;
      if (!metaDataList.isEmpty()) {
        // create and add ChunkReader with priority
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(tsFileResource, tsFileResource.isClosed());
        chunkLoader = new ChunkLoaderImpl(tsFileReader);
      }
      for (ChunkMetaData chunkMetaData : metaDataList) {
        chunkReaderWrapList.add(new ChunkReaderWrap(chunkMetaData, chunkLoader, null));
      }

      if (!tsFileResource.isClosed()) {
        // create and add MemChunkReader
        chunkReaderWrapList.add(new ChunkReaderWrap(tsFileResource.getReadOnlyMemChunk(), null));
      }
    }

    // TODO future work: create reader when getValueInTimestamp so that resources
    //  whose start and end time do not satisfy can be skipped.

    ExternalSortJobEngine externalSortJobEngine = SimpleExternalSortEngine.getInstance();
    List<IPointReaderByTimestamp> readerList = externalSortJobEngine
        .executeForByTimestampReader(queryId, chunkReaderWrapList);
    int priorityValue = 1;
    for (IPointReaderByTimestamp chunkReader : readerList) {
      addReaderWithPriority(chunkReader, priorityValue++);
    }
  }
}
