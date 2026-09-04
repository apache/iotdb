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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

public final class DeviceEntryIOContext {

  private static final int IO_BATCH_SIZE = 1000;

  private final MPPQueryContext queryContext;

  private int checkTimeoutCount;

  public DeviceEntryIOContext(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
  }

  public void checkTimeout() {
    if (++checkTimeoutCount >= IO_BATCH_SIZE) {
      queryContext.checkTimeOut();
      checkTimeoutCount = 0;
    }
  }

  public void recordDiskIODuringFetchSchema(long bytes, long startNanos) {
    recordDiskIO(bytes, startNanos, true);
  }

  public void recordDiskIODuringDistributionPlan(long bytes, long startNanos) {
    recordDiskIO(bytes, startNanos, false);
  }

  private void recordDiskIO(long bytes, long startNanos, boolean duringFetchSchema) {
    long timeCost = System.nanoTime() - startNanos;
    if (duringFetchSchema) {
      queryContext.recordDeviceEntryDiskIODuringFetchSchema(bytes, timeCost);
    } else {
      queryContext.recordDeviceEntryDiskIODuringDistributionPlan(bytes, timeCost);
    }
    checkTimeout();
  }
}
