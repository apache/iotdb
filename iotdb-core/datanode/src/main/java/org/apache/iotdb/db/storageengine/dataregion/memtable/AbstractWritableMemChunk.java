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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.utils.datastructure.TVList;

import java.util.Iterator;

public class AbstractWritableMemChunk {
  protected static long RETRY_INTERVAL_MS = 200L;
  protected static long MAX_WAIT_QUERY_MS = 60 * 1000L;

  /**
   * Release the TVList if there is no query on it. Otherwise, it should set the first query as the
   * owner. TVList is released until all queries finish. If it throws memory-not-enough exception
   * during owner transfer, retry the release process after 200ms. If the problem is still not
   * resolved in 60s, it starts to abort first query in the list, kick it out of the list and retry.
   * Finally, the method must ensure success because it's part of flushing.
   *
   * @param tvList
   */
  protected void maybeReleaseTvList(TVList tvList) {
    long maxWaitQueryDurationInMs = MAX_WAIT_QUERY_MS;
    long retryStartTimeInMs = 0;
    try {
      retryStartTimeInMs = System.currentTimeMillis();
      tryReleaseTvList(tvList);
    } catch (MemoryNotEnoughException ex) {
      maxWaitQueryDurationInMs -= (System.currentTimeMillis() - retryStartTimeInMs);
      if (maxWaitQueryDurationInMs < 0) {
        // Abort first query in the list. When all queries in the list have been aborted,
        // tryReleaseTvList will ensure succeed finally.
        tvList.lockQueryList();
        try {
          // fail the first query
          Iterator<QueryContext> iterator = tvList.getQueryContextSet().iterator();
          if (iterator.hasNext()) {
            FragmentInstanceContext firstQuery = (FragmentInstanceContext) iterator.next();
            firstQuery.failed(
                new MemoryNotEnoughException(
                    "Memory not enough to clone the tvlist during flush phase"));
            iterator.remove();
          }
        } finally {
          tvList.unlockQueryList();
        }
      }

      // sleep 100ms to retry
      try {
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      tryReleaseTvList(tvList);
    }
  }

  private void tryReleaseTvList(TVList tvList) {
    tvList.lockQueryList();
    try {
      if (tvList.getQueryContextSet().isEmpty()) {
        tvList.clear();
      } else {
        QueryContext firstQuery = tvList.getQueryContextSet().iterator().next();
        // transfer memory from write process to read process. Here it reserves read memory and
        // releaseFlushedMemTable will release write memory.
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(tvList.calculateRamSize());
        }
        // update current TVList owner to first query in the list
        tvList.setOwnerQuery(firstQuery);
      }
    } finally {
      tvList.unlockQueryList();
    }
  }
}
