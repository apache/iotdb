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
 */
public class PendingResultFragment {

  private ByteBuffer block;
  private int nextOffset;

  /** Records that {@code block} has been sent up to {@code nextOffset} bytes. */
  public synchronized void reset(ByteBuffer block, int nextOffset) {
    if (block != this.block) {
      this.block = block;
    }
    this.nextOffset = nextOffset;
  }

  /** Returns the block that is partially sent, or null if no fragment is pending. */
  public synchronized ByteBuffer getBlock() {
    return block;
  }

  /** Returns the start offset of the next fragment. */
  public synchronized int getNextOffset() {
    return nextOffset;
  }

  public synchronized void clear() {
    block = null;
    nextOffset = 0;
  }
}
