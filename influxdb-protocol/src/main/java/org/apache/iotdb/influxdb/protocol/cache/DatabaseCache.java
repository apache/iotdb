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
  // Tag list and order under current measurement
  private Map<String, Integer> tagOrders = new HashMap<>();

  private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();

  // Database currently selected by influxdb
  private String database;

  public DatabaseCache() {}

  public DatabaseCache(
      Map<String, Integer> tagOrders,
      String database,
      Map<String, Map<String, Integer>> measurementTagOrder) {
    this.tagOrders = tagOrders;
    this.database = database;
    this.measurementTagOrder = measurementTagOrder;
  }

  public Map<String, Integer> getTagOrders() {
    return tagOrders;
  }

  public void setTagOrders(Map<String, Integer> tagOrders) {
    this.tagOrders = tagOrders;
  }

  public Map<String, Map<String, Integer>> getMeasurementTagOrder() {
    return measurementTagOrder;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setMeasurementTagOrder(Map<String, Map<String, Integer>> measurementTagOrder) {
    this.measurementTagOrder = measurementTagOrder;
  }
}
