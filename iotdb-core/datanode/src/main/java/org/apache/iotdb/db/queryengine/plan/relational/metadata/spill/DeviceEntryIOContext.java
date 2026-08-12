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

  private final MPPQueryContext queryContext;
  private final boolean duringFetchSchema;

  public DeviceEntryIOContext(MPPQueryContext queryContext, boolean duringFetchSchema) {
    this.queryContext = queryContext;
    this.duringFetchSchema = duringFetchSchema;
  }

  public void checkTimeout() {
    queryContext.checkTimeOut();
  }

  public void recordDiskIO(long bytes, long startNanos) {
    long timeCost = System.nanoTime() - startNanos;
    if (duringFetchSchema) {
      queryContext.recordDeviceEntryDiskIODuringFetchSchema(bytes, timeCost);
    }
    checkTimeout();
  }

  public void recordDeviceEntryCount(long count) {
    if (duringFetchSchema) {
      queryContext.recordDeviceEntryCount(count);
    }
  }
}
