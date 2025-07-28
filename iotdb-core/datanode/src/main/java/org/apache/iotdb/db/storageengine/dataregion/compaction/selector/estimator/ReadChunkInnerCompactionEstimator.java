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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

public class ReadChunkInnerCompactionEstimator extends AbstractInnerSpaceEstimator {

  @Override
  public long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo) {
    long cost = 0;
    // add ChunkMetadata size of MultiTsFileDeviceIterator
    long maxAlignedSeriesMemCost =
        taskInfo.getFileInfoList().stream()
            .mapToLong(fileInfo -> fileInfo.maxMemToReadAlignedSeries)
            .sum();
    long maxNonAlignedSeriesMemCost =
        taskInfo.getFileInfoList().stream()
            .mapToLong(fileInfo -> fileInfo.maxMemToReadNonAlignedSeries)
            .sum();
    cost +=
        Math.min(
            Math.max(maxAlignedSeriesMemCost, maxNonAlignedSeriesMemCost),
            taskInfo.getFileInfoList().size()
                * taskInfo.getMaxChunkMetadataNumInDevice()
                * taskInfo.getMaxChunkMetadataSize());

    // add ChunkMetadata size of targetFileWriter
    cost += fixedMemoryBudget;

    return cost;
  }

  @Override
  public long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) {
    if (taskInfo.getTotalChunkNum() == 0) {
      return taskInfo.getModificationFileSize();
    }
    long averageChunkSize = taskInfo.getTotalFileSize() / taskInfo.getTotalChunkNum();

    int batchSize = config.getCompactionMaxAlignedSeriesNumInOneBatch();
    int maxConcurrentSeriesNum =
        Math.min(
            batchSize <= 0 ? Integer.MAX_VALUE : batchSize, taskInfo.getMaxConcurrentSeriesNum());

    long maxConcurrentSeriesSizeOfTotalFiles =
        averageChunkSize
                * taskInfo.getFileInfoList().size()
                * maxConcurrentSeriesNum
                * taskInfo.getMaxChunkMetadataNumInSeries()
            + maxConcurrentSeriesNum * tsFileConfig.getPageSizeInByte();
    long maxTargetChunkWriterSize = config.getTargetChunkSize() * maxConcurrentSeriesNum;
    long targetChunkWriterSize =
        Math.min(maxConcurrentSeriesSizeOfTotalFiles, maxTargetChunkWriterSize);

    long chunkSizeFromSourceFile =
        (averageChunkSize + tsFileConfig.getPageSizeInByte()) * maxConcurrentSeriesNum;

    return targetChunkWriterSize + chunkSizeFromSourceFile + taskInfo.getModificationFileSize();
  }

  @Override
  public long roughEstimateInnerCompactionMemory(
      CompactionScheduleContext context, List<TsFileResource> resources) {
    if (config.getCompactionMaxAlignedSeriesNumInOneBatch() <= 0) {
      return -1L;
    }
    CompactionTaskMetadataInfo metadataInfo =
        CompactionEstimateUtils.collectMetadataInfoFromCachedFileInfo(
            resources, roughInfoMap, false);

    int maxConcurrentSeriesNum = metadataInfo.getMaxConcurrentSeriesNum(false);
    long maxChunkSize = config.getTargetChunkSize();
    long maxPageSize = tsFileConfig.getPageSizeInByte();
    // source files (chunk + uncompressed page)
    // target file (chunk + unsealed page writer)
    return 2 * maxConcurrentSeriesNum * (maxChunkSize + maxPageSize)
        + fixedMemoryBudget
        + metadataInfo.metadataMemCost;
  }
}
