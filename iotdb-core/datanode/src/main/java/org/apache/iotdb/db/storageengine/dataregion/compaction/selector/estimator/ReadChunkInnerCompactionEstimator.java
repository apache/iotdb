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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadChunkInnerCompactionEstimator extends AbstractInnerSpaceEstimator {

  private static final Logger logger =
      LoggerFactory.getLogger(ReadChunkInnerCompactionEstimator.class);

  @Override
  public long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo) {
    long cost = 0;
    // add ChunkMetadata size of MultiTsFileDeviceIterator
    cost +=
        taskInfo.getFileInfoList().size()
            * taskInfo.getMaxChunkMetadataNumInDevice()
            * taskInfo.getMaxChunkMetadataSize();

    logger.info("chunk metadata size {}", cost);
    // add ChunkMetadata size of targetFileWriter
    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    logger.info("size for file writer: {}", sizeForFileWriter);
    cost += sizeForFileWriter;

    return cost;
  }

  @Override
  public long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) {
    long cost = 0;
    cost += taskInfo.getModificationFileSize();

    if (taskInfo.getTotalChunkNum() == 0) {
      return cost;
    }

    long uncompressedChunkSize =
        taskInfo.getTotalFileSize() * compressionRatio / taskInfo.getTotalChunkNum();
    cost += uncompressedChunkSize * taskInfo.getMaxConcurrentSeriesNum();

    long targetChunkWriterSize = config.getTargetChunkSize() * taskInfo.getMaxConcurrentSeriesNum();
    long maxSeriesSizeOfTotalFiles =
        uncompressedChunkSize
            * taskInfo.getFileInfoList().size()
            * taskInfo.getMaxConcurrentSeriesNum()
            * taskInfo.getMaxChunkMetadataNumInSeries();
    cost += Math.min(targetChunkWriterSize, maxSeriesSizeOfTotalFiles);

    return cost;
  }
}
