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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.flushcontroller;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.BatchCompactionCannotAlignedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchCompactionPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FollowingBatchCompactionAlignedChunkWriter;

import org.apache.tsfile.file.metadata.IChunkMetadata;

public class FollowedBatchedCompactionFlushController extends AbstractCompactionFlushController {

  public BatchCompactionPlan batchCompactionPlan;
  public FollowingBatchCompactionAlignedChunkWriter chunkWriter;
  public int currentCompactChunk;

  public FollowedBatchedCompactionFlushController(
      BatchCompactionPlan batchCompactionPlan,
      FollowingBatchCompactionAlignedChunkWriter chunkWriter) {
    super(0, 0);
    this.batchCompactionPlan = batchCompactionPlan;
    this.chunkWriter = chunkWriter;
    this.currentCompactChunk = 0;
  }

  @Override
  public boolean shouldSealChunkWriter() {
    return chunkWriter.checkIsChunkSizeOverThreshold(0, 0, true);
  }

  @Override
  public boolean shouldCompactChunkByDirectlyFlush(IChunkMetadata chunkToFlush) {
    CompactChunkPlan compactChunkPlan =
        batchCompactionPlan.getCompactChunkPlan(currentCompactChunk);
    if (compactChunkPlan.isCompactedByDirectlyFlush()
        && chunkToFlush.getStartTime() != compactChunkPlan.getTimeRange().getMin()) {
      throw new BatchCompactionCannotAlignedException(
          chunkToFlush, compactChunkPlan, batchCompactionPlan);
    }
    return compactChunkPlan.isCompactedByDirectlyFlush();
  }

  @Override
  public void nextChunk() {
    currentCompactChunk++;
    if (currentCompactChunk < batchCompactionPlan.compactedChunkNum()) {
      chunkWriter.setCompactChunkPlan(batchCompactionPlan.getCompactChunkPlan(currentCompactChunk));
    }
  }
}
