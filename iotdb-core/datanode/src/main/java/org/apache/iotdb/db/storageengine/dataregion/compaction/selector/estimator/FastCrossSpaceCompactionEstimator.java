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

import java.io.IOException;

public class FastCrossSpaceCompactionEstimator extends AbstractCrossSpaceEstimator {

  /**
   * The metadata algorithm is: maxChunkMetaDataSize * maxChunkNumber * fileSize * maxSeriesNumber
   *
   * @return estimate metadata memory cost
   */
  @Override
  protected long calculatingMetadataMemoryCost(CompactionTaskInfo taskInfo) {
    return taskInfo.getFileInfoList().size()
        * taskInfo.getMaxChunkMetadataNumInDevice()
        * taskInfo.getMaxChunkMetadataSize()
        * Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum());
  }

  @Override
  protected long calculatingDataMemoryCost(CompactionTaskInfo taskInfo) throws IOException {
    long cost = 0;
    long maxConcurrentSeriesNum =
        Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum());

    long uncompressedTotalChunkSize = taskInfo.getTotalFileSize() * compressionRatio;
    long targetChunkWriterSize =
        config.getTargetChunkSize() * maxConcurrentSeriesNum * seqResources.size();
    cost += Math.min(uncompressedTotalChunkSize, targetChunkWriterSize);

    cost += taskInfo.getModificationFileSize();
    cost +=
        taskInfo.getTotalFileSize()
            * compressionRatio
            * maxConcurrentSeriesNum
            * calculatingMaxOverlapFileNumInSubCompactionTask(taskInfo.getResources())
            / taskInfo.getTotalChunkNum();
    return cost;
  }
}
