/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;

public interface ISinkHandle extends AutoCloseable {

  /** Get the total amount of memory used by buffered tsblocks. */
  long getBufferRetainedSizeInBytes();

  /** Get the number of buffered tsblocks. */
  int getNumOfBufferedTsBlocks();

  /** Get a future that will be completed when the output buffer is not full. */
  ListenableFuture<Void> isFull();

  /**
   * Send a list of tsblocks to an unpartitioned output buffer. If no-more-tsblocks has been set,
   * the send tsblock call is ignored. This can happen with limit queries. A {@link
   * RuntimeException} will be thrown if any exception happened * during the data transmission.
   */
  void send(List<TsBlock> tsBlocks) throws IOException;

  /**
   * Send a {@link TsBlock} to a specific partition. If no-more-tsblocks has been set, the send
   * tsblock call is ignored. This can happen with limit queries. A {@link RuntimeException} will be
   * thrown if any exception happened * during the data transmission.
   */
  void send(int partition, List<TsBlock> tsBlocks) throws IOException;

  /**
   * Notify the handle that no more tsblocks will be sent. Any future calls to send a tsblock should
   * be ignored.
   */
  void setNoMoreTsBlocks();

  /** If the handle is closed. */
  public boolean isClosed();

  /**
   * If no more tsblocks will be sent and all the tsblocks have been fetched by downstream fragment
   * instances.
   */
  public boolean isFinished();

  /**
   * Close the handle. The output buffer will not be cleared until all tsblocks are fetched by
   * downstream instances. A {@link RuntimeException} will be thrown if any exception happened
   * during the data transmission.
   */
  @Override
  void close() throws IOException;

  /** Abort the sink handle, discarding all tsblocks which may still be in memory buffer. */
  void abort();
}
