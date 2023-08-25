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

import java.io.IOException;

public class FastCompactionInnerCompactionEstimator extends AbstractInnerSpaceEstimator {

  /**
   * The metadata algorithm is: maxChunkMetaDataSize * maxChunkNumber * fileSize * maxSeriesNumber
   *
   * @return estimate metadata memory cost
   */
  @Override
  public long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo) {
    long cost = 0;
    // add ChunkMetadata size of MultiTsFileDeviceIterator
    cost +=
        Math.min(
            taskInfo.getTotalChunkMetadataSize(),
            taskInfo.getFileInfoList().size()
                * taskInfo.getMaxChunkMetadataNumInSeries()
                * taskInfo.getMaxChunkMetadataSize()
                * Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum()));

    // add ChunkMetadata size of targetFileWriter
    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    cost += sizeForFileWriter;

    return cost;
  }

  /**
   * The data algorithm is: (targetChunkSize * maxConcurrentSeriesNumber) + modsFileSize +
   * (totalFileSize * compressionRatio / totalChunkNum) * maxConcurrentSeriesNum *
   * maxConcurrentFileNum
   *
   * @return estimate data memory cost
   */
  @Override
  public long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException {
    long cost = 0;
    cost += taskInfo.getModificationFileSize();

    long uncompressedTotalFileSize = taskInfo.getTotalFileSize() * compressionRatio;
    if (taskInfo.getTotalChunkNum() == 0) {
      return cost;
    }

    long maxConcurrentSeriesNum =
        Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum());
    long targetChunkWriterSize = config.getTargetChunkSize() * maxConcurrentSeriesNum;
    cost += Math.min(uncompressedTotalFileSize, targetChunkWriterSize);

    cost +=
        uncompressedTotalFileSize
            * maxConcurrentSeriesNum
            * calculatingMaxOverlapFileNumInSubCompactionTask(taskInfo.getResources())
            / taskInfo.getTotalChunkNum();
    return cost;
  }
}
