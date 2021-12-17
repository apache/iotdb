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

package org.apache.iotdb.db.metadata.idtable.entry;

import java.util.HashMap;
import java.util.Map;

/** device entry in id table */
public class DeviceEntry {
  /** for device ID reuse in memtable */
  IDeviceID deviceID;

  /** measurement schema map */
  Map<String, SchemaEntry> measurementMap;

  boolean isAligned;

  public DeviceEntry(IDeviceID deviceID) {
    this.deviceID = deviceID;
    measurementMap = new HashMap<>();
  }

  /**
   * get schema entry of the measurement
   *
   * @param measurementName name of the measurement
   * @return if exist, schema entry of the measurement. if not exist, null
   */
  public SchemaEntry getSchemaEntry(String measurementName) {
    return measurementMap.get(measurementName);
  }

  /**
   * put new schema entry of the measurement
   *
   * @param measurementName name of the measurement
   * @param schemaEntry schema entry of the measurement
   */
  public void putSchemaEntry(String measurementName, SchemaEntry schemaEntry) {
    measurementMap.put(measurementName, schemaEntry);
  }

  /**
   * whether the device entry contains the measurement
   *
   * @param measurementName name of the measurement
   * @return whether the device entry contains the measurement
   */
  public boolean contains(String measurementName) {
    return measurementMap.containsKey(measurementName);
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }
}
