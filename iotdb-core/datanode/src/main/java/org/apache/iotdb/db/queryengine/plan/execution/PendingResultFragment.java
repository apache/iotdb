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
 * <p>The memory of the pending block is reserved from the query memory pool through an {@link
 * Account} owned by the query, and released when the fragment is {@link #clear() cleared}. The
 * account lives with the query rather than with the result handle, so the reservation covers the
 * whole lifetime of the fragment on the local, the remote and the in-memory result paths.
 */
public class PendingResultFragment {

  /** Reserves and releases the memory of a pending block; {@code null} disables accounting. */
  public interface Account {

    /** Returns the bytes actually reserved, which may be fewer than requested. */
    long reserve(long bytes);

    void free(long bytes);
  }

  private final Account account;

  private ByteBuffer block;
  private int nextOffset;
  private long reservedBytes;

  public PendingResultFragment(Account account) {
    this.account = account;
  }

  /**
   * Records that {@code block} has been sent up to {@code nextOffset} bytes. When a new block
   * becomes pending, its serialized length is reserved from the query memory pool.
   */
  public synchronized void reset(ByteBuffer block, int nextOffset) {
    if (block != this.block) {
      release();
      this.block = block;
      if (account != null) {
        reservedBytes = account.reserve(block.remaining());
      }
    }
    this.nextOffset = nextOffset;
  }

  public synchronized ByteBuffer getBlock() {
    return block;
  }

  public synchronized int getNextOffset() {
    return nextOffset;
  }

  /**
   * Discards the pending fragment and releases the memory of the pending block. It is also called
   * right after a complete block has been added to a response, when nothing is pending. Calling it
   * repeatedly is a no-op.
   */
  public synchronized void clear() {
    block = null;
    nextOffset = 0;
    release();
  }

  private void release() {
    if (reservedBytes > 0) {
      account.free(reservedBytes);
      reservedBytes = 0;
    }
  }
}
