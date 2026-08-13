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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;

/**
 * DISK_IO is rate-limited by legacy throttle at the RPC layer; this limiter is a no-op capacity
 * placeholder so ResourceType.DISK_IO still fits the shared acquire API.
 */
public class DiskIoLimiter implements ResourceLimiter {

  @Override
  public LimiterAcquireResult tryAcquire(
      long userId, OperationType op, long amount, ResourceQuotaRange range, NodeQuotaState node) {
    // DISK_IO uses throttle rate limiter; amount is checked via OperationQuota at RPC layer.
    return LimiterAcquireResult.ok();
  }

  @Override
  public void release(long userId, OperationType op, long amount, NodeQuotaState node) {}

  @Override
  public long getInUse(long userId, OperationType op, NodeQuotaState node) {
    // DISK_IO is rate-based (legacy throttle limiter keyed by userName); no capacity in-use here.
    return 0;
  }

  @Override
  public boolean isEnforced() {
    // DISK_IO is enforced by OperationQuota / throttle rate limiter at the RPC layer.
    return false;
  }

  public static ThrottleType throttleTypeFor(OperationType op) {
    return op == OperationType.READ ? ThrottleType.READ_SIZE : ThrottleType.WRITE_SIZE;
  }
}
