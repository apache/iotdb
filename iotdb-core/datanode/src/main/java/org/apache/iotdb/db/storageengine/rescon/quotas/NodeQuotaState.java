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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NodeQuotaState {

  private final int nodeId;
  private final long nodeCapacity;
  private final Map<String, Long> inUseByUser = new HashMap<>();
  private final Map<String, ResourceQuotaRange> rangeByUser = new HashMap<>();

  public NodeQuotaState(int nodeId, long nodeCapacity) {
    this.nodeId = nodeId;
    this.nodeCapacity = nodeCapacity;
  }

  public int getNodeId() {
    return nodeId;
  }

  public long getNodeCapacity() {
    return nodeCapacity;
  }

  public long inUse(String user) {
    return inUseByUser.getOrDefault(user, 0L);
  }

  public void addInUse(String user, long amount) {
    inUseByUser.put(user, inUse(user) + amount);
  }

  public void removeInUse(String user, long amount) {
    long newValue = Math.max(0, inUse(user) - amount);
    if (newValue == 0) {
      inUseByUser.remove(user);
    } else {
      inUseByUser.put(user, newValue);
    }
  }

  public long min(String user) {
    ResourceQuotaRange range = rangeByUser.get(user);
    return range == null ? IoTDBConstant.UNLIMITED_VALUE : range.getMinValue();
  }

  public void updateRange(String user, ResourceQuotaRange range) {
    if (range == null || range.isUnlimited()) {
      rangeByUser.remove(user);
    } else {
      rangeByUser.put(user, range);
    }
  }

  /** Remove configured ranges only; keep inUse so in-flight tokens can still release safely. */
  public void clearUserRange(String user) {
    rangeByUser.remove(user);
  }

  public void clearUser(String user) {
    clearUserRange(user);
    inUseByUser.remove(user);
  }

  public Set<String> allUsers() {
    Set<String> users = new java.util.HashSet<>(inUseByUser.keySet());
    users.addAll(rangeByUser.keySet());
    return users;
  }

  public long totalInUse() {
    return inUseByUser.values().stream().mapToLong(Long::longValue).sum();
  }

  public long remaining() {
    return nodeCapacity - totalInUse();
  }

  public Map<String, Long> getInUseByUser() {
    return Collections.unmodifiableMap(inUseByUser);
  }

  public Map<String, ResourceQuotaRange> getRangeByUser() {
    return Collections.unmodifiableMap(rangeByUser);
  }
}
