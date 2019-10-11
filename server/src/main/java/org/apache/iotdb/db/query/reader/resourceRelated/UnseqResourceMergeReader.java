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
import java.nio.ByteBuffer;
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
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * To read a list of unsequence TsFiles, this class extends {@link PriorityMergeReader} to
 * implement
 * <code>IPointReader</code> for the TsFiles.
 * <p>
 * Note that an unsequence TsFile can be either closed or unclosed. An unclosed unsequence TsFile
 * consists of data on disk and data in memtables that will be flushed to this unclosed TsFile.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter}.
 */
public class UnseqResourceMergeReader extends PriorityMergeReader {

  private Path seriesPath;
  private long queryId;

  public UnseqResourceMergeReader(Path seriesPath, List<TsFileResource> unseqResources,
      QueryContext context, Filter filter) throws IOException {
    this.seriesPath = seriesPath;
    this.queryId = context.getJobId();

    List<ChunkReaderWrap> readerWrapList = new ArrayList<>();
    for (TsFileResource tsFileResource : unseqResources) {

      // prepare metaDataList
      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        if (isTsFileNotSatisfied(tsFileResource, filter)) {
          continue;
        }

        metaDataList = DeviceMetaDataCache.getInstance()
            .get(tsFileResource, seriesPath);
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResource.getModFile(), seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        if (tsFileResource.getEndTimeMap().size() != 0) {
          if (isTsFileNotSatisfied(tsFileResource, filter)) {
            continue;
          }
        }
        metaDataList = tsFileResource.getChunkMetaDatas();
      }

      ChunkLoaderImpl chunkLoader = null;
      if (!metaDataList.isEmpty()) {
        TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(tsFileResource, tsFileResource.isClosed());
        chunkLoader = new ChunkLoaderImpl(tsFileReader);
      }

      for (ChunkMetaData chunkMetaData : metaDataList) {

        if (filter != null) {
          ByteBuffer minValue = null;
          ByteBuffer maxValue = null;
          ByteBuffer[] statistics = chunkMetaData.getDigest().getStatistics();
          if (statistics != null) {
            minValue = statistics[StatisticType.min_value.ordinal()]; // note still CAN be null
            maxValue = statistics[StatisticType.max_value.ordinal()]; // note still CAN be null
          }

          DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
              chunkMetaData.getEndTime(), minValue, maxValue, chunkMetaData.getTsDataType());
          if (!filter.satisfy(digest)) {
            continue;
          }
        }
        // create and add DiskChunkReader
        readerWrapList.add(new ChunkReaderWrap(chunkMetaData, chunkLoader, filter));
      }

      if (!tsFileResource.isClosed()) {
        // create and add MemChunkReader
        readerWrapList.add(new ChunkReaderWrap(tsFileResource.getReadOnlyMemChunk(), filter));
      }
    }

    ExternalSortJobEngine externalSortJobEngine = SimpleExternalSortEngine.getInstance();
    List<IPointReader> readerList = externalSortJobEngine
        .executeForIPointReader(queryId, readerWrapList);
    int priorityValue = 1;
    for (IPointReader chunkReader : readerList) {
      addReaderWithPriority(chunkReader, priorityValue++);
    }
  }

  /**
   * Returns true if the start and end time of the series data in this unsequence TsFile do not
   * satisfy the filter condition. Returns false if satisfy.
   * <p>
   * This method is used to in the constructor function to check whether this TsFile can be
   * skipped.
   *
   * @param tsFile the TsFileResource corresponding to this TsFile
   * @param filter filter condition. Null if no filter.
   * @return True if the TsFile's start and end time do not satisfy the filter condition; False if
   * satisfy.
   */
  // TODO future work: deduplicate code. See SeqResourceIterateReader.
  private boolean isTsFileNotSatisfied(TsFileResource tsFile, Filter filter) {
    if (filter == null) {
      return false;
    }
    long startTime = tsFile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsFile.getEndTimeMap().get(seriesPath.getDevice());
    return !filter.satisfyStartEndTime(startTime, endTime);
  }
}
