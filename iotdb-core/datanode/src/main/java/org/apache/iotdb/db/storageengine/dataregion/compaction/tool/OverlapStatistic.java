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

import java.util.HashSet;

public class OverlapStatistic {
  long totalSequenceFile;
  long totalSequenceFileSize;
  long totalChunkGroupsInSequenceFile;
  long totalChunksInSequenceFile;

  HashSet<String> sequenceNumber = new HashSet<>();
  long sequenceMinStartTime = Long.MAX_VALUE;
  long sequenceMaxEndTime = Long.MIN_VALUE;

  long totalUnsequenceFile;
  long totalUnsequenceFileSize;
  long totalChunkGroupsInUnSequenceFile;
  long totalChunksInUnSequenceFile;
  long unSequenceMinStartTime = Long.MAX_VALUE;
  long unSequenceMaxEndTime = Long.MIN_VALUE;

  long overlappedSequenceFiles;
  long overlappedChunkGroupsInSequenceFile;
  long overlappedChunksInSequenceFile;

  public void merge(OverlapStatistic other) {
    this.totalSequenceFile += other.totalSequenceFile;
    this.totalSequenceFileSize += other.totalSequenceFileSize;
    this.totalChunkGroupsInSequenceFile += other.totalChunkGroupsInSequenceFile;
    this.totalChunksInSequenceFile += other.totalChunksInSequenceFile;
    this.sequenceMinStartTime = Math.min(this.sequenceMinStartTime, other.sequenceMinStartTime);
    this.sequenceMaxEndTime = Math.max(this.sequenceMaxEndTime, other.sequenceMaxEndTime);

    this.totalUnsequenceFile += other.totalUnsequenceFile;
    this.totalUnsequenceFileSize += other.totalUnsequenceFileSize;
    this.totalChunkGroupsInUnSequenceFile += other.totalChunkGroupsInUnSequenceFile;
    this.totalChunksInUnSequenceFile += other.totalChunksInUnSequenceFile;
    this.unSequenceMinStartTime =
        Math.min(this.unSequenceMinStartTime, other.unSequenceMinStartTime);
    this.unSequenceMaxEndTime = Math.max(this.unSequenceMaxEndTime, other.unSequenceMaxEndTime);

    this.overlappedSequenceFiles += other.overlappedSequenceFiles;
    this.overlappedChunkGroupsInSequenceFile += other.overlappedChunkGroupsInSequenceFile;
    this.overlappedChunksInSequenceFile += other.overlappedChunksInSequenceFile;
  }

  public void mergeSingleSequenceFileTaskResult(SequenceFileTaskSummary summary) {
    if (summary.equals(new SequenceFileTaskSummary())) {
      return;
    }
    if (summary.overlapChunkGroup > 0) {
      this.overlappedSequenceFiles += 1;
    }
    this.overlappedChunkGroupsInSequenceFile += summary.overlapChunkGroup;
    this.totalChunkGroupsInSequenceFile += summary.totalChunkGroups;
    this.overlappedChunksInSequenceFile += summary.overlapChunk;
    this.totalChunksInSequenceFile += summary.totalChunks;
    this.totalSequenceFile += 1;
    this.totalSequenceFileSize += summary.fileSize;
    this.sequenceMinStartTime = Math.min(this.sequenceMinStartTime, summary.minStartTime);
    this.sequenceMaxEndTime = Math.max(this.sequenceMaxEndTime, summary.maxEndTime);
  }

  public void mergeUnSeqSpaceStatistics(UnseqSpaceStatistics statistics) {
    this.totalUnsequenceFile += statistics.unsequenceFileNum;
    this.totalUnsequenceFileSize += statistics.unsequenceFileSize;
    this.totalChunksInUnSequenceFile += statistics.unsequenceChunkNum;
    this.totalChunkGroupsInUnSequenceFile += statistics.unsequenceChunkGroupNum;
    this.unSequenceMinStartTime = Math.min(this.unSequenceMinStartTime, statistics.minStartTime);
    this.unSequenceMaxEndTime = Math.max(this.unSequenceMaxEndTime, statistics.maxEndTime);
  }
}
