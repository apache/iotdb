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

import org.apache.tsfile.file.metadata.IChunkMetadata;

public abstract class AbstractCompactionFlushController {

  // if unsealed chunk size is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  protected long chunkSizeLowerBoundInCompaction;

  // if point num of unsealed chunk is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  protected long chunkPointNumLowerBoundInCompaction;

  public AbstractCompactionFlushController(
      long chunkSizeLowerBoundInCompaction, long chunkPointNumLowerBoundInCompaction) {
    this.chunkSizeLowerBoundInCompaction = chunkSizeLowerBoundInCompaction;
    this.chunkPointNumLowerBoundInCompaction = chunkPointNumLowerBoundInCompaction;
  }

  public abstract boolean shouldSealChunkWriter();

  public abstract boolean shouldCompactChunkByDirectlyFlush(IChunkMetadata chunkMetadata);

  public abstract void nextChunk();
}
