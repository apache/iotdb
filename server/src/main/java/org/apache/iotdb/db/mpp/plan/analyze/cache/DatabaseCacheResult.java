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

public abstract class DatabaseCacheResult<V> {
  /** the result of database cache search. */
  private boolean success = true;
  /** the list of missed devices. */
  private List<String> missedDevices = new ArrayList<>();
  /** the result map, notice that this map will be empty when failed. */
  protected Map<String, V> resultMap = new HashMap<>();

  public boolean isSuccess() {
    return success;
  }

  public void setFailed() {
    this.success = false;
  }

  public void addMissedDevice(String missedDevice) {
    this.missedDevices.add(missedDevice);
  }

  public abstract void put(String device, String databaseName);

  public List<String> getMissedDevices() {
    return missedDevices;
  }

  public Map<String, V> getResultMap() {
    return resultMap;
  }

  public void reset() {
    this.success = true;
    this.missedDevices = new ArrayList<>();
    this.resultMap = new HashMap<>();
  }
}
