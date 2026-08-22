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
import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;

public class DiskIoLimiter implements ResourceLimiter {

  @Override
  public LimiterAcquireResult tryAcquire(
      String user, long amount, ResourceQuotaRange range, NodeQuotaState node) {
    // DISK_IO uses throttle rate limiter; amount is checked via OperationQuota at RPC layer.
    return LimiterAcquireResult.ok();
  }

  @Override
  public void release(String user, long amount, NodeQuotaState node) {}

  @Override
  public long getInUse(String user, NodeQuotaState node) {
    QuotaLimiter limiter =
        DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().getUserLimiter(user);
    if (limiter == null) {
      return 0;
    }
    return limiter.getReadAvailable();
  }

  @Override
  public boolean isEnforced() {
    // DISK_IO is enforced by OperationQuota / throttle rate limiter at the RPC layer.
    return false;
  }

  public void checkDiskIo(String user, OperationType op, long amount)
      throws RpcThrottlingException {
    QuotaLimiter limiter =
        DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().getUserLimiter(user);
    if (limiter == null) {
      return;
    }
    if (op == OperationType.READ) {
      limiter.checkQuota(0, 0, 0, amount);
    } else {
      limiter.checkQuota(0, amount, 0, 0);
    }
  }

  public static ThrottleType throttleTypeFor(OperationType op) {
    return op == OperationType.READ ? ThrottleType.READ_SIZE : ThrottleType.WRITE_SIZE;
  }
}
