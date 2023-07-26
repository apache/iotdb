/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Estimate the memory cost of one inner space compaction task with specific source files based on
 * its corresponding implementation.
 */
public abstract class AbstractInnerSpaceEstimator extends AbstractCompactionEstimator {
  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public abstract long estimateInnerCompactionMemory(List<TsFileResource> resources)
      throws IOException;

  public long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException {
    throw new RuntimeException(
        "This kind of estimator cannot be used to estimate cross space compaction task");
  }

  protected InnerCompactionTaskInfo calculatingReadChunkCompactionTaskInfo(
      List<TsFileResource> resources) throws IOException {
    List<FileInfo> fileInfoList = new ArrayList<>();
    for (TsFileResource resource : resources) {
      TsFileSequenceReader reader = getFileReader(resource);
      FileInfo fileInfo = CompactionEstimateUtils.getSeriesAndDeviceChunkNum(reader);
      fileInfoList.add(fileInfo);
    }
    return new InnerCompactionTaskInfo(resources, fileInfoList);
  }

  protected static class InnerCompactionTaskInfo {
    private final List<FileInfo> fileInfoList;
    private int maxConcurrentSeriesNum = 1;
    private long maxChunkMetadataSize = 0;
    private int maxChunkMetadataNumInDevice = 0;
    private long modificationFileSize = 0;

    protected InnerCompactionTaskInfo(List<TsFileResource> resources, List<FileInfo> fileInfoList) {
      this.fileInfoList = fileInfoList;
      for (TsFileResource resource : resources) {
        ModificationFile modificationFile = resource.getModFile();
        if (modificationFile.exists()) {
          modificationFileSize += modificationFile.getSize();
        }
      }
      for (FileInfo fileInfo : fileInfoList) {
        maxConcurrentSeriesNum =
            Math.max(maxConcurrentSeriesNum, fileInfo.maxAlignedSeriesNumInDevice);
        maxChunkMetadataNumInDevice =
            Math.max(maxChunkMetadataNumInDevice, fileInfo.maxDeviceChunkNum);
        maxChunkMetadataSize = Math.max(maxChunkMetadataSize, fileInfo.averageChunkMetadataSize);
      }
    }

    public int getMaxChunkMetadataNumInDevice() {
      return maxChunkMetadataNumInDevice;
    }

    public long getMaxChunkMetadataSize() {
      return maxChunkMetadataSize;
    }

    public List<FileInfo> getFileInfoList() {
      return fileInfoList;
    }

    public int getMaxConcurrentSeriesNum() {
      return maxConcurrentSeriesNum;
    }

    public long getModificationFileSize() {
      return modificationFileSize;
    }
  }
}
