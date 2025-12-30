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

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.db.queryengine.common.QueryId;

import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ThreadSafeMemoryReservationManager extends NotThreadSafeMemoryReservationManager {
  public ThreadSafeMemoryReservationManager(QueryId queryId, String contextHolder) {
    super(queryId, contextHolder);
  }

  @Override
  public synchronized void reserveMemoryCumulatively(long size) {
    super.reserveMemoryCumulatively(size);
  }

  @Override
  public synchronized void reserveMemoryImmediately() {
    super.reserveMemoryImmediately();
  }

  @Override
  public synchronized void releaseMemoryCumulatively(long size) {
    super.releaseMemoryCumulatively(size);
  }

  @Override
  public synchronized void releaseAllReservedMemory() {
    super.releaseAllReservedMemory();
  }

  @Override
  public synchronized Pair<Long, Long> releaseMemoryVirtually(final long size) {
    return super.releaseMemoryVirtually(size);
  }

  @Override
  public synchronized void reserveMemoryVirtually(
      final long bytesToBeReserved, final long bytesAlreadyReserved) {
    super.reserveMemoryVirtually(bytesToBeReserved, bytesAlreadyReserved);
  }
}
