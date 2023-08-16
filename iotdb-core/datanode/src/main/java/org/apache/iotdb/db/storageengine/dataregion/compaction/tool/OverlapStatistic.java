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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

public class OverlapStatistic {
  long totalSequenceFile;
  long totalChunkGroupsInSequenceFile;
  long totalChunksInSequenceFile;

  long overlappedSequenceFiles;
  long overlappedChunkGroupsInSequenceFile;
  long overlappedChunksInSequenceFile;

  long totalUnsequenceFile;
  long totalSequenceFileSize;
  long totalUnsequenceFileSize;

  public void merge(OverlapStatistic other) {
    this.totalSequenceFile += other.totalSequenceFile;
    this.totalChunkGroupsInSequenceFile += other.totalChunkGroupsInSequenceFile;
    this.totalChunksInSequenceFile += other.totalChunksInSequenceFile;
    this.overlappedSequenceFiles += other.overlappedSequenceFiles;
    this.overlappedChunkGroupsInSequenceFile += other.overlappedChunkGroupsInSequenceFile;
    this.overlappedChunksInSequenceFile += other.overlappedChunksInSequenceFile;

    this.totalUnsequenceFile += other.totalUnsequenceFile;
    this.totalSequenceFileSize += other.totalSequenceFileSize;
    this.totalUnsequenceFileSize += other.totalUnsequenceFileSize;
  }

  public void mergeSingleSequenceFileTaskResult(SequenceFileTaskSummary summary) {
    if (summary.equals(new SequenceFileTaskSummary())) {
      return;
    }
    this.overlappedChunkGroupsInSequenceFile += summary.overlapChunkGroup;
    this.totalChunkGroupsInSequenceFile += summary.totalChunkGroups;
    this.overlappedChunksInSequenceFile += summary.overlapChunk;
    this.totalChunksInSequenceFile += summary.totalChunks;
    this.totalSequenceFile += 1;
    this.totalSequenceFileSize += summary.fileSize;
  }

  public void mergeUnSeqSpaceStatistics(UnseqSpaceStatistics statistics) {
    this.totalUnsequenceFile += statistics.unsequenceFileNum;
    this.totalUnsequenceFileSize += statistics.unsequenceFileSize;
  }
}
