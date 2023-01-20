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
package org.apache.iotdb.db.engine.compaction.execute.task.subtask;

import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;

public class FastCompactionTaskSummary extends CompactionTaskSummary {
  public int CHUNK_NONE_OVERLAP;
  public int CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
  public int CHUNK_OVERLAP_OR_MODIFIED;

  public int PAGE_NONE_OVERLAP;
  public int PAGE_OVERLAP_OR_MODIFIED;
  public int PAGE_FAKE_OVERLAP;
  public int PAGE_NONE_OVERLAP_BUT_DESERIALIZE;

  public void increase(FastCompactionTaskSummary summary) {
    this.CHUNK_NONE_OVERLAP += summary.CHUNK_NONE_OVERLAP;
    this.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE += summary.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
    this.CHUNK_OVERLAP_OR_MODIFIED += summary.CHUNK_OVERLAP_OR_MODIFIED;
    this.PAGE_NONE_OVERLAP += summary.PAGE_NONE_OVERLAP;
    this.PAGE_OVERLAP_OR_MODIFIED += summary.PAGE_OVERLAP_OR_MODIFIED;
    this.PAGE_FAKE_OVERLAP += summary.PAGE_FAKE_OVERLAP;
    this.PAGE_NONE_OVERLAP_BUT_DESERIALIZE += summary.PAGE_NONE_OVERLAP_BUT_DESERIALIZE;
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
        CHUNK_NONE_OVERLAP,
        CHUNK_NONE_OVERLAP_BUT_DESERIALIZE,
        CHUNK_OVERLAP_OR_MODIFIED,
        PAGE_NONE_OVERLAP,
        PAGE_NONE_OVERLAP_BUT_DESERIALIZE,
        PAGE_OVERLAP_OR_MODIFIED,
        PAGE_FAKE_OVERLAP);
  }
}
