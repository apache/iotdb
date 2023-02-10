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

import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RequestSets {
  private final Set<IndexedConsensusRequest> requestSet;
  private long localRequestNumber = 0;

  public RequestSets(Set<IndexedConsensusRequest> requests) {
    this.requestSet = requests;
  }

  public void add(IndexedConsensusRequest request, boolean local) {
    if (local) {
      localRequestNumber++;
    }
    requestSet.add(request);
  }

  public Set<IndexedConsensusRequest> getRequestSet() {
    return requestSet;
  }

  public void waitForNextReady() throws InterruptedException {}

  public boolean waitForNextReady(long time, TimeUnit unit) throws InterruptedException {
    return true;
  }

  public long getLocalRequestNumber() {
    return localRequestNumber;
  }
}
