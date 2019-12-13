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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunkRelated.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

/**
 * To read a list of unsequence TsFiles, this class implements <code>IBatchReader</code> for the
 * TsFiles. Note that an unsequence TsFile can be either closed or unclosed. An unclosed unsequence
 * TsFile consists of data on disk and data in memtables that will be flushed to this unclosed
 * TsFile. This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter}.
 */
public class UnseqResourceMergeReader implements IBatchReader {

  private PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
  private List<ChunkMetaData> metaDataList = new ArrayList<>();
  private Filter timeFilter;
  private int index = 0; // used to index current metadata in metaDataList

  /**
   * prepare metaDataList
   */
  public UnseqResourceMergeReader(Path seriesPath, List<TsFileResource> unseqResources,
      QueryContext context, Filter timeFilter) throws IOException {

    this.timeFilter = timeFilter;
    int priority = 1;

    // get all ChunkMetadata
    for (TsFileResource tsFileResource : unseqResources) {
      List<ChunkMetaData> tsFileMetaDataList;
      if (tsFileResource.isClosed()) {
        if (!ResourceRelatedUtil.isTsFileSatisfied(tsFileResource, timeFilter, seriesPath)) {
          continue;
        }

        tsFileMetaDataList = DeviceMetaDataCache.getInstance().get(tsFileResource, seriesPath);
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
        tsFileMetaDataList = tsFileResource.getChunkMetaDataList();
      }

      if (!tsFileMetaDataList.isEmpty()) {
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(tsFileResource, tsFileResource.isClosed());
        ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);

        for (ChunkMetaData chunkMetaData : tsFileMetaDataList) {
          if (timeFilter != null && !timeFilter.satisfy(chunkMetaData.getStatistics())) {
            tsFileMetaDataList.remove(chunkMetaData);
            continue;
          }
          chunkMetaData.setPriority(priority++);
          chunkMetaData.setChunkLoader(chunkLoader);
        }
        metaDataList.addAll(tsFileMetaDataList);
      }

      // create and add MemChunkReader
      if (!tsFileResource.isClosed()) {
        ChunkReaderWrap memChunkReaderWrap = new ChunkReaderWrap(
            tsFileResource.getReadOnlyMemChunk(), timeFilter);
        priorityMergeReader.addReaderWithPriority(memChunkReaderWrap.getIPointReader(), priority++);
      }
    }

    // sort All ChunkMetadata by start time
    metaDataList = metaDataList.stream().sorted(Comparator.comparing(ChunkMetaData::getStartTime))
        .collect(Collectors.toList());
  }

  /**
   * Create a ChunkReader with priority for each ChunkMetadata and put the ChunkReader to
   * mergeReader one by one
   */
  @Override
  public boolean hasNextBatch() throws IOException {
    ChunkMetaData metaData;
    ChunkReaderWrap diskChunkReaderWrap;
    if (priorityMergeReader.hasNext()) {
      long currentTime = priorityMergeReader.current().getTimestamp();

      metaData = metaDataList.get(index);
      long nextMetaDataStartTime = metaData.getStartTime();

      // create and add DiskChunkReader
      while (currentTime >= nextMetaDataStartTime) {
        diskChunkReaderWrap = new ChunkReaderWrap(metaData, metaData.getChunkLoader(), timeFilter);
        priorityMergeReader
            .addReaderWithPriority(diskChunkReaderWrap.getIPointReader(), metaData.getPriority());
        index++;
        if (index < metaDataList.size()) {
          metaData = metaDataList.get(index);
          nextMetaDataStartTime = metaData.getStartTime();
        }
      }
      return true;
    }

    if (index >= metaDataList.size()) {
      return false;
    }
    metaData = metaDataList.get(index++);
    diskChunkReaderWrap = new ChunkReaderWrap(metaData, metaData.getChunkLoader(), timeFilter);
    priorityMergeReader
        .addReaderWithPriority(diskChunkReaderWrap.getIPointReader(), metaData.getPriority());

    return hasNextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    BatchData batchData = new BatchData();
    for (int i = 0; i < 4096; i++) {
      TimeValuePair timeValuePair = priorityMergeReader.next();
      batchData.putTime(timeValuePair.getTimestamp());
      batchData.putAnObject(timeValuePair.getValue());
      if (!hasNextBatch()) {
        break;
      }
    }

    return batchData;
  }

  @Override
  public void close() throws IOException {
    priorityMergeReader.close();
  }
}
