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
 *
 * <p>Reserving under memory pressure waits outside every lock of this class, and {@link #clear()}
 * cancels such a wait, so cleanup never blocks behind a pending reservation.
 */
public class PendingResultFragment {

  /** Reserves and releases the memory of a pending block; {@code null} disables accounting. */
  public interface Account {

    /**
     * Reserves {@code bytes}, waiting for memory if the pool is exhausted. Throws {@link
     * IllegalStateException} if the wait is cancelled by {@link #release()}.
     */
    void reserve(long bytes);

    /** Releases all bytes reserved through {@link #reserve(long)}, cancelling a waiting one. */
    void release();
  }

  private final Account account;

  private ByteBuffer block;
  private int nextOffset;

  public PendingResultFragment(Account account) {
    this.account = account;
  }

  /** Records that {@code block} has been sent up to {@code nextOffset} bytes. */
  public void reset(ByteBuffer block, int nextOffset) {
    boolean newBlock;
    boolean hadPending;
    synchronized (this) {
      hadPending = this.block != null;
      newBlock = block != this.block;
      this.block = block;
      this.nextOffset = nextOffset;
    }
    if (!newBlock || account == null) {
      return;
    }
    if (hadPending) {
      account.release();
    }
    account.reserve(block.remaining());
    boolean cleared;
    synchronized (this) {
      cleared = this.block == null;
    }
    if (cleared) {
      // the fragment was cleared while the reservation waited, which released it already
      account.release();
    }
  }

  public synchronized ByteBuffer getBlock() {
    return block;
  }

  public synchronized int getNextOffset() {
    return nextOffset;
  }

  /**
   * Discards the pending fragment and releases the memory of the pending block, cancelling a
   * reservation that is still waiting. It is also called right after a complete block has been
   * added to a response, when nothing is pending. Calling it repeatedly is a no-op.
   */
  public void clear() {
    synchronized (this) {
      block = null;
      nextOffset = 0;
    }
    if (account != null) {
      account.release();
    }
  }
}
