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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastCompactionInnerCompactionEstimator extends AbstractInnerSpaceEstimator {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  @Override
  public long calculatingMetadataMemoryCost(InnerCompactionTaskInfo taskInfo) {
    return taskInfo.getFileInfoList().size()
        * taskInfo.getMaxChunkMetadataNumInDevice()
        * taskInfo.getMaxChunkMetadataSize();
  }

  @Override
  public long calculatingDataMemoryCost(InnerCompactionTaskInfo taskInfo) {
    long cost =
        config.getTargetChunkSize()
            * taskInfo.getFileInfoList().size()
            * Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum())
            * compressionRatio;
    return cost;
  }

  private long calculatingFastCompactionCost(int fileSize, int maxSeriesNumber) {
    return config.getTargetChunkSize()
        * fileSize
        * Math.max(config.getSubCompactionTaskNum(), maxSeriesNumber)
        * compressionRatio;
  }

  private long calculatingWriteTargetFileCost(InnerCompactionTaskInfo taskInfo, int fileSize) {
    long cost =
        Math.max(config.getSubCompactionTaskNum(), taskInfo.getMaxConcurrentSeriesNum())
            * IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize()
            * fileSize;

    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    cost += sizeForFileWriter;
    return cost;
  }
}
