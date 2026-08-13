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
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

/**
 * Capacity-type limiter (CPU / MEMORY / TEMP_DISK).
 *
 * <p>Unlimited or unconfigured users still participate in node capacity and minGap scheduling so
 * that users with a configured min are not starved. Read/write max and min are enforced per
 * operation side; node capacity is shared across both sides.
 */
public class CapacityResourceLimiter implements ResourceLimiter {

  private static final ResourceQuotaRange UNLIMITED_RANGE =
      new ResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, IoTDBConstant.UNLIMITED_VALUE);

  /** Human-readable resource label included in reject messages. */
  protected String getResourceLabel() {
    return "resource";
  }

  /**
   * Enforce user max, then node capacity, then cross-user minGap (allow if requester is still under
   * its own min).
   */
  @Override
  public LimiterAcquireResult tryAcquire(
      long userId, OperationType op, long amount, ResourceQuotaRange range, NodeQuotaState node) {
    ResourceQuotaRange effective = range == null ? UNLIMITED_RANGE : range;
    long cur = node.inUse(op, userId);
    String resourceLabel = getResourceLabel();

    if (effective.getMaxValue() != IoTDBConstant.UNLIMITED_VALUE
        && cur + amount > effective.getMaxValue()) {
      return LimiterAcquireResult.reject(
          String.format(
              StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_FOR_ARG_F411FFB3, resourceLabel));
    }

    if (node.totalInUse() + amount > node.getNodeCapacity()) {
      return LimiterAcquireResult.reject(
          String.format(
              StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_FOR_ARG_7D36CEB7,
              resourceLabel));
    }

    long freeAfter = node.getNodeCapacity() - node.totalInUse() - amount;
    long minGapTotal = calcMinGapTotal(node, op, userId, amount);
    if (minGapTotal <= freeAfter) {
      node.addInUse(op, userId, amount);
      return LimiterAcquireResult.ok();
    }

    if (effective.getMinValue() != IoTDBConstant.UNLIMITED_VALUE && cur < effective.getMinValue()) {
      long minHeadroom = effective.getMinValue() - cur;
      if (amount <= minHeadroom) {
        node.addInUse(op, userId, amount);
        return LimiterAcquireResult.ok();
      }
    }
    return LimiterAcquireResult.reject(
        String.format(
            StorageEngineMessages.EXCEPTION_MIN_GAP_RESERVATION_FOR_ARG_6F3E0949, resourceLabel));
  }

  @Override
  public void release(long userId, OperationType op, long amount, NodeQuotaState node) {
    node.removeInUse(op, userId, amount);
  }

  @Override
  public long getInUse(long userId, OperationType op, NodeQuotaState node) {
    return node.inUse(op, userId);
  }

  @Override
  public boolean isEnforced() {
    return true;
  }

  /**
   * Sum remaining min reservations across all users and both operation sides. Node capacity is
   * shared, so a write min must still be reserved while a read acquire is evaluated.
   */
  static long calcMinGapTotal(
      NodeQuotaState node, OperationType requestOp, long requestUserId, long amount) {
    long totalGap = 0;
    for (OperationType op : OperationType.values()) {
      for (long u : node.allUsers()) {
        long inUse = node.inUse(op, u);
        if (op == requestOp && u == requestUserId) {
          inUse += amount;
        }
        long min = node.min(op, u);
        if (min != IoTDBConstant.UNLIMITED_VALUE && inUse < min) {
          totalGap += (min - inUse);
        }
      }
    }
    return totalGap;
  }
}
