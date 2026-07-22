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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RepairTimePartition {
  private final String databaseName;
  private final String dataRegionId;
  private TsFileManager tsFileManager;
  private final long timePartitionId;
  private long maxFileTimestamp;
  private boolean repaired;

  public RepairTimePartition(DataRegion dataRegion, long timePartitionId, long maxFileTimestamp) {
    this.databaseName = dataRegion.getDatabaseName();
    this.dataRegionId = dataRegion.getDataRegionIdString();
    this.tsFileManager = dataRegion.getTsFileManager();
    this.timePartitionId = timePartitionId;
    this.maxFileTimestamp = maxFileTimestamp;
    this.repaired = false;
  }

  public RepairTimePartition(String databaseName, String dataRegionId, long timePartitionId) {
    this.databaseName = databaseName;
    this.dataRegionId = dataRegionId;
    this.timePartitionId = timePartitionId;
  }

  public long getTimePartitionId() {
    return timePartitionId;
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

  public boolean isRepaired() {
    return repaired;
  }

  public void setRepaired(boolean repaired) {
    this.repaired = repaired;
  }

  public boolean needRepair() {
    return !getAllFileSnapshot().isEmpty();
  }

  public List<TsFileResource> getSeqFileSnapshot() {
    return tsFileManager.getTsFileListSnapshot(timePartitionId, true).stream()
        .filter(this::resourceTimestampFilter)
        .collect(Collectors.toList());
  }

  public List<TsFileResource> getUnSeqFileSnapshot() {
    return tsFileManager.getTsFileListSnapshot(timePartitionId, false).stream()
        .filter(this::resourceTimestampFilter)
        .collect(Collectors.toList());
  }

  public List<TsFileResource> getAllFileSnapshot() {
    List<TsFileResource> seqFiles = getSeqFileSnapshot();
    List<TsFileResource> unseqFiles = getUnSeqFileSnapshot();
    List<TsFileResource> allFiles = new ArrayList<>(seqFiles.size() + unseqFiles.size());
    allFiles.addAll(seqFiles);
    allFiles.addAll(unseqFiles);
    return allFiles;
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
    RepairTimePartition that = (RepairTimePartition) o;
    return timePartitionId == that.timePartitionId
        && Objects.equals(databaseName, that.databaseName)
        && Objects.equals(dataRegionId, that.dataRegionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, dataRegionId, timePartitionId);
  }
}
