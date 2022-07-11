/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.analyze.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class StorageGroupCacheResult<V> {
  private boolean success = true;
  private List<String> failedDevices = new ArrayList<>();
  protected Map<String, V> map = new HashMap<>();;

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public List<String> getFailedDevices() {
    return failedDevices;
  }

  public void addFailedDevice(String failedDevice) {
    this.failedDevices.add(failedDevice);
  }

  public Map<String, V> getMap() {
    return map;
  }

  public abstract void put(String device, String storageGroup);

  /** clear when failed */
  public void clear() {
    this.success = false;
    this.map = new HashMap<>();
  }

  /** reset storageGroupCacheResult */
  public void reset() {
    this.success = true;
    this.failedDevices = new ArrayList<>();
    this.map = new HashMap<>();
  }
}
