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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

import java.util.concurrent.TimeUnit;

public final class DeviceEntryIOContext {

  private final MPPQueryContext queryContext;
  private final long timeoutStartNanos;
  private final long remainingTimeoutNanos;

  public DeviceEntryIOContext(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
    this.timeoutStartNanos = System.nanoTime();
    long elapsedMillis = Math.max(0, System.currentTimeMillis() - queryContext.getStartTime());
    long remainingTimeoutMillis = Math.max(0, queryContext.getTimeOut() - elapsedMillis);
    this.remainingTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(remainingTimeoutMillis);
  }

  public void checkTimeout() {
    if (System.nanoTime() - timeoutStartNanos >= remainingTimeoutNanos) {
      throw new QueryTimeoutRuntimeException(
          queryContext.getStartTime(), System.currentTimeMillis(), queryContext.getTimeOut());
    }
  }

  public void recordDiskIO(long bytes, long startNanos) {
    queryContext.recordDeviceEntryDiskIO(bytes, System.nanoTime() - startNanos);
    checkTimeout();
  }

  public void recordSegment() {
    queryContext.recordDeviceEntrySegment();
  }

  public void recordSortedRun() {
    queryContext.recordDeviceEntrySortedRun();
  }
}
