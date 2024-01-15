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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class TimePartitionFiles {
  private final String databaseName;
  private final String dataRegionId;
  private TsFileManager tsFileManager;
  private final long timePartition;
  private long maxFileTimestamp;

  TimePartitionFiles(DataRegion dataRegion, long timePartition) {
    this.databaseName = dataRegion.getDatabaseName();
    this.dataRegionId = dataRegion.getDataRegionId();
    this.tsFileManager = dataRegion.getTsFileManager();
    this.timePartition = timePartition;
    this.maxFileTimestamp = calculateMaxTimestamp();
  }

  TimePartitionFiles(String databaseName, String dataRegionId, long timePartition) {
    this.databaseName = databaseName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
  }

  private long calculateMaxTimestamp() {
    long maxTimestamp = 0;
    List<TsFileResource> resources = tsFileManager.getTsFileListSnapshot(timePartition, true);
    if (!resources.isEmpty()) {
      maxTimestamp = getFileTimestamp(resources.get(resources.size() - 1));
    }
    resources = tsFileManager.getTsFileListSnapshot(timePartition, false);
    if (!resources.isEmpty()) {
      maxTimestamp = Math.max(maxTimestamp, getFileTimestamp(resources.get(resources.size() - 1)));
    }
    return maxTimestamp;
  }

  public long getTimePartition() {
    return timePartition;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getDataRegionId() {
    return dataRegionId;
  }

  public TsFileManager getTsFileManager() {
    return tsFileManager;
  }

  public List<TsFileResource> getSeqFiles() {
    return tsFileManager.getTsFileListSnapshot(timePartition, true).stream()
        .filter(this::resourceTimestampFilter)
        .collect(Collectors.toList());
  }

  public List<TsFileResource> getUnseqFiles() {
    return tsFileManager.getTsFileListSnapshot(timePartition, false).stream()
        .filter(this::resourceTimestampFilter)
        .collect(Collectors.toList());
  }

  private boolean resourceTimestampFilter(TsFileResource resource) {
    if (resource.getStatus() == TsFileResourceStatus.DELETED
        || resource.getStatus() == TsFileResourceStatus.UNCLOSED) {
      return false;
    }
    long fileTimestamp = getFileTimestamp(resource);
    return fileTimestamp >= 0 && fileTimestamp <= maxFileTimestamp;
  }

  private long getFileTimestamp(TsFileResource resource) {
    long timestamp = -1;
    try {
      TsFileNameGenerator.TsFileName tsFileName =
          TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
      timestamp = tsFileName.getTime();
    } catch (IOException ignored) {
    }
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimePartitionFiles that = (TimePartitionFiles) o;
    return timePartition == that.timePartition
        && Objects.equals(databaseName, that.databaseName)
        && Objects.equals(dataRegionId, that.dataRegionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, dataRegionId, timePartition);
  }
}
