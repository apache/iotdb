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

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Per-node capacity tracking for one resource type. Read and write usage/ranges are tracked
 * separately so that {@code read_*} and {@code write_*} USER QUOTA limits do not overwrite each
 * other; node capacity itself remains a shared pool across both operation sides.
 */
public class NodeQuotaState {

  private final int nodeId;
  private volatile long nodeCapacity;
  private final Map<OperationType, Map<Long, Long>> inUseByOpUser =
      new EnumMap<>(OperationType.class);
  private final Map<OperationType, Map<Long, ResourceQuotaRange>> rangeByOpUser =
      new EnumMap<>(OperationType.class);

  public NodeQuotaState(int nodeId, long nodeCapacity) {
    this.nodeId = nodeId;
    this.nodeCapacity = nodeCapacity;
    for (OperationType op : OperationType.values()) {
      inUseByOpUser.put(op, new HashMap<>());
      rangeByOpUser.put(op, new HashMap<>());
    }
  }

  public int getNodeId() {
    return nodeId;
  }

  public long getNodeCapacity() {
    return nodeCapacity;
  }

  /**
   * Hot-reload node capacity. If {@code totalInUse()} already exceeds {@code nodeCapacity},
   * in-flight tokens may remain; new acquires are rejected until usage drops below the new limit.
   */
  public void setNodeCapacity(long nodeCapacity) {
    this.nodeCapacity = nodeCapacity;
  }

  public long inUse(OperationType op, long userId) {
    return inUseByOpUser.get(op).getOrDefault(userId, 0L);
  }

  public void addInUse(OperationType op, long userId, long amount) {
    inUseByOpUser.get(op).merge(userId, amount, Long::sum);
  }

  public void removeInUse(OperationType op, long userId, long amount) {
    Map<Long, Long> map = inUseByOpUser.get(op);
    long newValue = Math.max(0, inUse(op, userId) - amount);
    if (newValue == 0) {
      map.remove(userId);
    } else {
      map.put(userId, newValue);
    }
  }

  public long min(OperationType op, long userId) {
    ResourceQuotaRange range = rangeByOpUser.get(op).get(userId);
    return range == null ? IoTDBConstant.UNLIMITED_VALUE : range.getMinValue();
  }

  public void updateRange(OperationType op, long userId, ResourceQuotaRange range) {
    Map<Long, ResourceQuotaRange> map = rangeByOpUser.get(op);
    if (range == null || range.isUnlimited()) {
      map.remove(userId);
    } else {
      map.put(userId, range);
    }
  }

  /** Remove configured ranges only; keep inUse so in-flight tokens can still release safely. */
  public void clearUserRange(long userId) {
    for (OperationType op : OperationType.values()) {
      rangeByOpUser.get(op).remove(userId);
    }
  }

  public void clearUser(long userId) {
    clearUserRange(userId);
    for (OperationType op : OperationType.values()) {
      inUseByOpUser.get(op).remove(userId);
    }
  }

  public Set<Long> allUsers() {
    Set<Long> users = new HashSet<>();
    for (OperationType op : OperationType.values()) {
      users.addAll(inUseByOpUser.get(op).keySet());
      users.addAll(rangeByOpUser.get(op).keySet());
    }
    return users;
  }

  public long totalInUse() {
    long total = 0L;
    for (OperationType op : OperationType.values()) {
      for (Long v : inUseByOpUser.get(op).values()) {
        total += v;
      }
    }
    return total;
  }

  public long remaining() {
    return nodeCapacity - totalInUse();
  }

  public Map<Long, Long> getInUseByUser(OperationType op) {
    return Collections.unmodifiableMap(inUseByOpUser.get(op));
  }

  public Map<Long, ResourceQuotaRange> getRangeByUser(OperationType op) {
    return Collections.unmodifiableMap(rangeByOpUser.get(op));
  }
}
