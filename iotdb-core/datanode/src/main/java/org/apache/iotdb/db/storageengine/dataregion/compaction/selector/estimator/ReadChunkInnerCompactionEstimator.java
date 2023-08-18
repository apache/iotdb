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

public class ReadChunkInnerCompactionEstimator extends AbstractInnerSpaceEstimator {

  @Override
  public long calculatingMetadataMemoryCost(InnerCompactionTaskInfo taskInfo) {
    long cost = 0;
    // add ChunkMetadata size of MultiTsFileDeviceIterator
    cost +=
        taskInfo.getFileInfoList().size()
            * taskInfo.getMaxChunkMetadataNumInDevice()
            * taskInfo.getMaxChunkMetadataSize();

    // add ChunkMetadata size of targetFileWriter
    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    cost += sizeForFileWriter;

    return cost;
  }

  @Override
  public long calculatingDataMemoryCost(InnerCompactionTaskInfo taskInfo) {
    // add max target chunk size and max source chunk size
    long cost =
        2
            * taskInfo.getMaxConcurrentSeriesNum()
            * IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

    // add modification file size
    cost += taskInfo.getModificationFileSize();
    return cost;
  }
}
