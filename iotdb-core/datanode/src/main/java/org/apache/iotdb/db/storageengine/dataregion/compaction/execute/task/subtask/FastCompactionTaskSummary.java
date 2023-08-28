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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;

@SuppressWarnings("squid:S1104")
public class FastCompactionTaskSummary extends CompactionTaskSummary {
  public int chunkNoneOverlap;
  public int chunkNoneOverlapButDeserialize;
  public int chunkOverlapOrModified;

  public int pageNoneOverlap;
  public int pageOverlapOrModified;
  public int pageFakeOverlap;
  public int pageNoneOverlapButDeserialize;

  public void increase(FastCompactionTaskSummary summary) {
    this.chunkNoneOverlap += summary.chunkNoneOverlap;
    this.chunkNoneOverlapButDeserialize += summary.chunkNoneOverlapButDeserialize;
    this.chunkOverlapOrModified += summary.chunkOverlapOrModified;
    this.pageNoneOverlap += summary.pageNoneOverlap;
    this.pageOverlapOrModified += summary.pageOverlapOrModified;
    this.pageFakeOverlap += summary.pageFakeOverlap;
    this.pageNoneOverlapButDeserialize += summary.pageNoneOverlapButDeserialize;
    this.processChunkNum += summary.processChunkNum;
    this.processPointNum += summary.processPointNum;
    this.directlyFlushChunkNum += summary.directlyFlushChunkNum;
    this.mergedChunkNum += summary.mergedChunkNum;
    this.deserializeChunkCount += summary.deserializeChunkCount;
  }

  @Override
  public String toString() {
    return String.format(
        "CHUNK_NONE_OVERLAP num is %d, CHUNK_NONE_OVERLAP_BUT_DESERIALIZE num is %d,"
            + " CHUNK_OVERLAP_OR_MODIFIED num is %d, PAGE_NONE_OVERLAP num is %d,"
            + " PAGE_NONE_OVERLAP_BUT_DESERIALIZE num is %d, PAGE_OVERLAP_OR_MODIFIED num is %d,"
            + " PAGE_FAKE_OVERLAP num is %d.",
        chunkNoneOverlap,
        chunkNoneOverlapButDeserialize,
        chunkOverlapOrModified,
        pageNoneOverlap,
        pageNoneOverlapButDeserialize,
        pageOverlapOrModified,
        pageFakeOverlap);
  }
}
