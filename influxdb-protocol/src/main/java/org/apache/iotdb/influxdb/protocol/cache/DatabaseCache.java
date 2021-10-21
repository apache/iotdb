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

package org.apache.iotdb.influxdb.protocol.cache;

import java.util.HashMap;
import java.util.Map;

public class DatabaseCache {

  // TODO avoid oom
  private Map<String, Map<String, Map<String, Integer>>> databaseTagOrders = new HashMap<>();

  // Database currently selected by influxdb
  private String currentDatabase;

  public DatabaseCache() {}

  public DatabaseCache(
      Map<String, Map<String, Map<String, Integer>>> databaseTagOrders, String currentDatabase) {
    this.databaseTagOrders = databaseTagOrders;
    this.currentDatabase = currentDatabase;
  }

  public Map<String, Map<String, Map<String, Integer>>> getDatabaseTagOrders() {
    return databaseTagOrders;
  }

  public void setDatabaseTagOrders(
      Map<String, Map<String, Map<String, Integer>>> databaseTagOrders) {
    this.databaseTagOrders = databaseTagOrders;
  }

  public String getCurrentDatabase() {
    return currentDatabase;
  }

  public void updateDatabaseOrders(
      String databaseName, Map<String, Map<String, Integer>> measurementTagOrders) {
    this.databaseTagOrders.put(databaseName, measurementTagOrders);
  }

  public Map<String, Map<String, Integer>> getMeasurementOrders(String databaseName) {
    if (databaseTagOrders.containsKey(databaseName)) {
      return databaseTagOrders.get(databaseName);
    }
    return new HashMap<>();
  }

  public void setCurrentDatabase(String currentDatabase) {
    this.currentDatabase = currentDatabase;
  }
}
