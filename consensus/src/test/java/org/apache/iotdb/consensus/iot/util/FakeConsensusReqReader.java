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

package org.apache.iotdb.consensus.iot.util;

import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FakeConsensusReqReader implements ConsensusReqReader, DataSet {

  private final RequestSets requestSets;

  public FakeConsensusReqReader(RequestSets requestSets) {
    this.requestSets = requestSets;
  }

  @Override
  public void setSafelyDeletedSearchIndex(long safelyDeletedSearchIndex) {}

  @Override
  public ReqIterator getReqIterator(long startIndex) {
    return new FakeConsensusReqIterator(startIndex);
  }

  @Override
  public long getCurrentSearchIndex() {
    return requestSets.getLocalRequestNumber();
  }

  @Override
  public long getTotalSize() {
    return 0;
  }

  private class FakeConsensusReqIterator implements ConsensusReqReader.ReqIterator {

    private long nextSearchIndex;

    public FakeConsensusReqIterator(long startIndex) {
      this.nextSearchIndex = startIndex;
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public IndexedConsensusRequest next() {
      synchronized (requestSets) {
        for (IndexedConsensusRequest indexedConsensusRequest : requestSets.getRequestSet()) {
          if (indexedConsensusRequest.getSearchIndex() == nextSearchIndex) {
            nextSearchIndex++;
            return new IndexedConsensusRequest(
                indexedConsensusRequest.getSearchIndex(), indexedConsensusRequest.getRequests());
          }
        }
        return null;
      }
    }

    @Override
    public void waitForNextReady() throws InterruptedException {
      while (!hasNext()) {
        requestSets.waitForNextReady();
      }
    }

    @Override
    public void waitForNextReady(long time, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      while (!hasNext()) {
        requestSets.waitForNextReady(time, unit);
      }
    }

    @Override
    public void skipTo(long targetIndex) {
      nextSearchIndex = targetIndex;
    }
  }
}
