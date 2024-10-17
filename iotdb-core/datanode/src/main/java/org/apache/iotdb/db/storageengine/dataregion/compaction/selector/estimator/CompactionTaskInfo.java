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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

public class CompactionTaskInfo {
  private final List<FileInfo> fileInfoList;
  private final List<TsFileResource> resources;
  private int maxConcurrentSeriesNum = 1;
  private long maxChunkMetadataSize = 0;
  private int maxChunkMetadataNumInDevice = 0;
  private int maxChunkMetadataNumInSeries = 0;
  private long modificationFileSize = 0;
  private long totalFileSize = 0;
  private long totalChunkNum = 0;
  private long totalChunkMetadataSize = 0;

  protected CompactionTaskInfo(List<TsFileResource> resources, List<FileInfo> fileInfoList) {
    this.fileInfoList = fileInfoList;
    this.resources = resources;
    for (TsFileResource resource : resources) {
      this.modificationFileSize += resource.getTotalModSizeInByte();
      this.totalFileSize += resource.getTsFileSize();
    }
    for (FileInfo fileInfo : fileInfoList) {
      maxConcurrentSeriesNum =
          Math.max(maxConcurrentSeriesNum, fileInfo.maxAlignedSeriesNumInDevice);
      maxChunkMetadataNumInSeries =
          Math.max(maxChunkMetadataNumInSeries, fileInfo.maxSeriesChunkNum);
      maxChunkMetadataNumInDevice =
          Math.max(maxChunkMetadataNumInDevice, fileInfo.maxDeviceChunkNum);
      maxChunkMetadataSize = Math.max(maxChunkMetadataSize, fileInfo.averageChunkMetadataSize);
      totalChunkNum += fileInfo.totalChunkNum;
      totalChunkMetadataSize += fileInfo.totalChunkNum * fileInfo.averageChunkMetadataSize;
    }
  }

  public int getMaxChunkMetadataNumInDevice() {
    return maxChunkMetadataNumInDevice;
  }

  public int getMaxChunkMetadataNumInSeries() {
    return maxChunkMetadataNumInSeries;
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

  public long getTotalFileSize() {
    return totalFileSize;
  }

  public long getTotalChunkNum() {
    return totalChunkNum;
  }

  public List<TsFileResource> getResources() {
    return resources;
  }

  public long getTotalChunkMetadataSize() {
    return totalChunkMetadataSize;
  }
}
