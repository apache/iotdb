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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

/**
 * Capacity-type limiter (CPU / MEMORY / TEMP_DISK).
 *
 * <p>Unlimited or unconfigured users still participate in node capacity and minGap scheduling so
 * that users with a configured min are not starved.
 */
public class CapacityResourceLimiter implements ResourceLimiter {

  private static final ResourceQuotaRange UNLIMITED_RANGE =
      new ResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, IoTDBConstant.UNLIMITED_VALUE);

  @Override
  public LimiterAcquireResult tryAcquire(
      String user, long amount, ResourceQuotaRange range, NodeQuotaState node) {
    ResourceQuotaRange effective = range == null ? UNLIMITED_RANGE : range;
    long cur = node.inUse(user);

    if (effective.getMaxValue() != IoTDBConstant.UNLIMITED_VALUE
        && cur + amount > effective.getMaxValue()) {
      return LimiterAcquireResult.reject(
          StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_3D400C08);
    }

    if (node.totalInUse() + amount > node.getNodeCapacity()) {
      return LimiterAcquireResult.reject(
          StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_89601D9A);
    }

    long freeAfter = node.getNodeCapacity() - node.totalInUse() - amount;
    long minGapTotal = calcMinGapTotal(node, user, amount);
    if (minGapTotal <= freeAfter) {
      node.addInUse(user, amount);
      return LimiterAcquireResult.ok();
    }

    if (effective.getMinValue() != IoTDBConstant.UNLIMITED_VALUE && cur < effective.getMinValue()) {
      node.addInUse(user, amount);
      return LimiterAcquireResult.ok();
    }
    return LimiterAcquireResult.reject(
        StorageEngineMessages.EXCEPTION_MIN_GAP_RESERVATION_B84C4EE4);
  }

  @Override
  public void release(String user, long amount, NodeQuotaState node) {
    node.removeInUse(user, amount);
  }

  @Override
  public long getInUse(String user, NodeQuotaState node) {
    return node.inUse(user);
  }

  @Override
  public boolean isEnforced() {
    return true;
  }

  static long calcMinGapTotal(NodeQuotaState node, String requestUser, long amount) {
    long totalGap = 0;
    for (String u : node.allUsers()) {
      long inUse = node.inUse(u);
      if (u.equals(requestUser)) {
        inUse += amount;
      }
      long min = node.min(u);
      if (min != IoTDBConstant.UNLIMITED_VALUE && inUse < min) {
        totalGap += (min - inUse);
      }
    }
    return totalGap;
  }
}
