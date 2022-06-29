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

package org.apache.iotdb.consensus.multileader.wal;

import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** This interface provides search interface for consensus requests via index. */
public interface ConsensusReqReader {

  long DEFAULT_SEARCH_INDEX = -1;

  /**
   * Gets the consensus request at the specified position.
   *
   * @param index index of the consensus request to return
   * @return the consensus request at the specified position, null if the request doesn't exist
   */
  IConsensusRequest getReq(long index);

  /**
   * Gets the consensus requests from the specified start position.
   *
   * @param startIndex index of the start consensus request
   * @param num number of consensus requests to return, the number of actual returned consensus
   *     requests may less than this value
   * @return the consensus requests from the specified start position
   */
  List<IConsensusRequest> getReqs(long startIndex, int num);

  /**
   * Gets the consensus requests iterator from the specified start position.
   *
   * @param startIndex index of the start consensus request
   * @return the consensus requests iterator from the specified start position.
   */
  ReqIterator getReqIterator(long startIndex);

  /** This iterator provides blocking and non-blocking interfaces to read consensus request. */
  interface ReqIterator {
    /** Like {@link Iterator#hasNext()} */
    boolean hasNext();

    /**
     * Like {@link Iterator#next()}
     *
     * @throws java.util.NoSuchElementException if the iteration has no more elements, wait a moment
     *     or call {@link this#waitForNextReady} for more elements
     */
    IndexedConsensusRequest next();

    /**
     * Wait for the next element in the iteration ready, blocked until next element is available.
     */
    void waitForNextReady() throws InterruptedException;

    /**
     * Wait for the next element in the iteration ready, blocked until next element is available or
     * a specified amount of time has elapsed.
     */
    void waitForNextReady(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * Skips to target position of next element in the iteration <br>
     * Notice: The correctness of forward skipping should be guaranteed by the caller
     *
     * @param targetIndex target position of next element in the iteration
     */
    void skipTo(long targetIndex);
  }

  /** Get current search index */
  long getCurrentSearchIndex();
}
