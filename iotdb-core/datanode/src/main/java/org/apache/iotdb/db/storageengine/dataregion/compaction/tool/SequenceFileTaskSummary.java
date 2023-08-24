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

import java.util.Objects;

public class SequenceFileTaskSummary {
  public long overlapChunk = 0;
  public long overlapChunkGroup = 0;
  public long totalChunks = 0;
  public long totalChunkGroups = 0;
  public long fileSize = 0;

  public long minStartTime = Long.MAX_VALUE;
  public long maxEndTime = Long.MIN_VALUE;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SequenceFileTaskSummary that = (SequenceFileTaskSummary) o;
    return overlapChunk == that.overlapChunk
        && overlapChunkGroup == that.overlapChunkGroup
        && totalChunks == that.totalChunks
        && totalChunkGroups == that.totalChunkGroups
        && fileSize == that.fileSize;
  }

  public void setMaxEndTime(long maxEndTime) {
    this.maxEndTime = Math.max(this.maxEndTime, maxEndTime);
  }

  public void setMinStartTime(long minStartTime) {
    this.minStartTime = Math.min(this.minStartTime, minStartTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(overlapChunk, overlapChunkGroup, totalChunks, totalChunkGroups, fileSize);
  }
}
