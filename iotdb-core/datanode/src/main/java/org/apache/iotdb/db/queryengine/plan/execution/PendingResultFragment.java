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

package org.apache.iotdb.db.queryengine.plan.execution;

import java.nio.ByteBuffer;

/**
 * The tail of a serialized TsBlock that did not fit into the previous RPC response and is waiting
 * to be sent in the following response(s). The block is already serialized and removed from the
 * result handle, so this fragment only holds a reference to it and never copies the payload.
 *
 * <p>The memory the block retains stays reserved in the memory pool until the block is fully sent.
 * The owner binds a {@code memoryReleaser} which is invoked by {@link #clear()}, so callers only
 * interact with this holder and never release the memory themselves.
 */
public class PendingResultFragment {

  private final Runnable memoryReleaser;

  private ByteBuffer block;
  private int nextOffset;

  public PendingResultFragment(Runnable memoryReleaser) {
    this.memoryReleaser = memoryReleaser;
  }

  /** Records that {@code block} has been sent up to {@code nextOffset} bytes. */
  public void reset(ByteBuffer block, int nextOffset) {
    this.block = block;
    this.nextOffset = nextOffset;
  }

  public ByteBuffer getBlock() {
    return block;
  }

  public int getNextOffset() {
    return nextOffset;
  }

  /**
   * Discards the pending fragment and releases the memory of the blocks that have been fully sent.
   * It is also called right after a complete block has been added to a response, when nothing is
   * pending. Calling it repeatedly is a no-op.
   */
  public void clear() {
    block = null;
    nextOffset = 0;
    memoryReleaser.run();
  }
}
