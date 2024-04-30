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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class StorageGroupCacheResult<V> {
  /** the result */
  private boolean success = true;

  /** the list of devices that miss */
  private List<String> missedDevices = new ArrayList<>();

  /** result map, Notice: this map will be empty when failed */
  protected Map<String, V> map = new HashMap<>();

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public List<String> getMissedDevices() {
    return missedDevices;
  }

  public void addMissedDevice(String missedDevice) {
    this.missedDevices.add(missedDevice);
  }

  public Map<String, V> getMap() {
    return map;
  }

  public abstract void put(String device, String storageGroupName);

  /** set failed and clear the map */
  public void setFailed() {
    this.success = false;
    this.map = new HashMap<>();
  }

  /** reset storageGroupCacheResult */
  public void reset() {
    this.success = true;
    this.missedDevices = new ArrayList<>();
    this.map = new HashMap<>();
  }
}
