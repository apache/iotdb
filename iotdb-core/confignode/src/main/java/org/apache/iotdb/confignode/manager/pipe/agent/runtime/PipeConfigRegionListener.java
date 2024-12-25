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

package org.apache.iotdb.confignode.manager.pipe.agent.runtime;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningFilter;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningQueue;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.concurrent.atomic.AtomicBoolean;

public class PipeConfigRegionListener {

  private final ConfigRegionListeningQueue listeningQueue = new ConfigRegionListeningQueue();
  private int listeningQueueReferenceCount = 0;

  private final AtomicBoolean isLeaderReady = new AtomicBoolean(false);

  public synchronized ConfigRegionListeningQueue listener() {
    return listeningQueue;
  }

  public synchronized void increaseReference(final PipeParameters parameters)
      throws IllegalPathException {
    if (!ConfigRegionListeningFilter.parseListeningPlanTypeSet(parameters).isEmpty()) {
      listeningQueueReferenceCount++;
      if (listeningQueueReferenceCount == 1) {
        listeningQueue.open();
      }
    }
  }

  public synchronized void decreaseReference(final PipeParameters parameters)
      throws IllegalPathException {
    if (!ConfigRegionListeningFilter.parseListeningPlanTypeSet(parameters).isEmpty()) {
      listeningQueueReferenceCount--;
      if (listeningQueueReferenceCount == 0) {
        listeningQueue.close();
      }
    }
  }

  public synchronized boolean isLeaderReady() {
    return isLeaderReady.get();
  }

  // Leader ready flag has the following effect
  // 1. The linked list starts serving only after leader gets ready
  // 2. Config pipe task is only created after leader gets ready
  public synchronized void notifyLeaderReady() {
    isLeaderReady.set(true);
  }

  public synchronized void notifyLeaderUnavailable() {
    isLeaderReady.set(false);
  }
}
