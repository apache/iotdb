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
package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunkRelated.DiskChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

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

  public UnseqResourceReaderByTimestamp(Path seriesPath,
      List<TsFileResource> unseqResources, QueryContext context) throws IOException {
    int priorityValue = 1;

    for (TsFileResource tsFileResource : unseqResources) {

      // prepare metaDataList
      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        metaDataList = DeviceMetaDataCache.getInstance()
            .get(tsFileResource.getFile().getPath(), seriesPath);
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResource.getModFile(), seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResource.getChunkMetaDatas();
      }

      ChunkLoaderImpl chunkLoader = null;
      if (!metaDataList.isEmpty()) {
        // create and add ChunkReader with priority
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(tsFileResource, tsFileResource.isClosed());
        chunkLoader = new ChunkLoaderImpl(tsFileReader);
      }
      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        addReaderWithPriority(new DiskChunkReaderByTimestamp(chunkReader),
            priorityValue);

        priorityValue++;
      }

      if (!tsFileResource.isClosed()) {
        // create and add MemChunkReader with priority
        addReaderWithPriority(
            new MemChunkReaderByTimestamp(tsFileResource.getReadOnlyMemChunk()), priorityValue++);
      }
    }

    // TODO add external sort when needed

    // TODO future work: create reader when getValueInTimestamp so that resources
    //  whose start and end time do not satisfy can be skipped.
  }
}
