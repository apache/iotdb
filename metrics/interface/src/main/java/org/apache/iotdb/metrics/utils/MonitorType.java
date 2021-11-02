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

package org.apache.iotdb.metrics.utils;

import java.util.HashMap;
import java.util.Map;

public enum MonitorType {
  dropwizard("dropwizard"),
  micrometer("micrometer");

  private String name;

  MonitorType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  private static final Map<String, MonitorType> lookup = new HashMap<>();

  static {
    for (MonitorType monitorType : MonitorType.values()) {
      lookup.put(monitorType.getName(), monitorType);
    }
  }

  public static MonitorType get(String name) {
    return lookup.get(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
