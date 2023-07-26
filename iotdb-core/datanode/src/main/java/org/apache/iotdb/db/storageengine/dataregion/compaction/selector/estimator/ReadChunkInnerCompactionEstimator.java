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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ReadChunkInnerCompactionEstimator extends AbstractInnerSpaceEstimator {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  @Override
  public long estimateInnerCompactionMemory(List<TsFileResource> resources) throws IOException {
    if (resources.isEmpty()) {
      return -1L;
    }
    long cost = 0;
    InnerCompactionTaskInfo taskInfo = calculatingReadChunkCompactionTaskInfo(resources);
    cost += calculatingMultiDeviceIteratorCost(taskInfo);
    cost += calculatingReadChunkCost();
    cost += calculatingWriteTargetFileCost(taskInfo);
    return cost;
  }

  private long calculatingMultiDeviceIteratorCost(InnerCompactionTaskInfo taskInfo) {
    long cost = 0;
    cost +=
        taskInfo.getFileInfoList().size()
            * taskInfo.getMaxChunkMetadataNumInDevice()
            * taskInfo.getMaxChunkMetadataSize();
    cost += taskInfo.getModificationFileSize();
    return cost;
  }

  private long calculatingReadChunkCost() {
    return IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  }

  private long calculatingWriteTargetFileCost(InnerCompactionTaskInfo taskInfo) {
    long cost =
        taskInfo.getMaxConcurrentSeriesNum()
            * IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    cost += sizeForFileWriter;
    return cost;
  }
}
