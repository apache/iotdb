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
package org.apache.iotdb.lsm.example;

import java.util.HashMap;
import java.util.Map;

// 管理working memtable , immutable memtables,框架用户自定义
public class MemTableManager {

  // 可写的memtable
  private MemTable working;

  // 只读的memtables
  private Map<Integer, MemTable> immutables;

  // 记录已插入的最大的deviceid
  private int maxDeviceID;

  public MemTableManager() {
    working = new MemTable();
    immutables = new HashMap<>();
    maxDeviceID = 0;
  }

  public MemTable getWorking() {
    return working;
  }

  public void setWorking(MemTable working) {
    this.working = working;
  }

  public Map<Integer, MemTable> getImmutables() {
    return immutables;
  }

  public void setImmutables(Map<Integer, MemTable> immutables) {
    this.immutables = immutables;
  }

  public int getMaxDeviceID() {
    return maxDeviceID;
  }

  public void setMaxDeviceID(int maxDeviceID) {
    this.maxDeviceID = maxDeviceID;
  }

  @Override
  public String toString() {
    return "MemTableManager{"
        + "working="
        + working.toString()
        + ", immutables="
        + immutables.toString()
        + '}';
  }
}
