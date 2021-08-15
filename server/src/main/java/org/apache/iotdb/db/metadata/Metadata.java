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
package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/** This class stores all the metadata info for every deviceId and every timeseries. */
public class Metadata implements Serializable {

  private Map<String, List<String>> deviceIdMap;

  public Metadata(Map<String, List<String>> deviceIdMap) {
    this.deviceIdMap = deviceIdMap;
  }

  public Map<String, List<String>> getDeviceMap() {
    return deviceIdMap;
  }

  /** combine multiple metadatas */
  public static Metadata combineMetadatas(Metadata[] metadatas) {
    Map<String, List<String>> deviceIdMap = new HashMap<>();

    if (metadatas == null || metadatas.length == 0) {
      return new Metadata(deviceIdMap);
    }

    for (int i = 0; i < metadatas.length; i++) {
      Map<String, List<String>> subDeviceIdMap = metadatas[i].deviceIdMap;
      for (Entry<String, List<String>> entry : subDeviceIdMap.entrySet()) {
        List<String> list = deviceIdMap.getOrDefault(entry.getKey(), new ArrayList<>());
        list.addAll(entry.getValue());

        if (!deviceIdMap.containsKey(entry.getKey())) {
          deviceIdMap.put(entry.getKey(), list);
        }
      }
      metadatas[i] = null;
    }

    return new Metadata(deviceIdMap);
  }

  @Override
  public String toString() {
    return deviceIdMap.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }

    Metadata metadata = (Metadata) obj;
    return deviceIdMapEquals(deviceIdMap, metadata.deviceIdMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceIdMap);
  }

  /** only used to check if deviceIdMap is equal to another deviceIdMap */
  private boolean deviceIdMapEquals(
      Map<String, List<String>> map1, Map<String, List<String>> map2) {
    if (!map1.keySet().equals(map2.keySet())) {
      return false;
    }

    for (Entry<String, List<String>> entry : map1.entrySet()) {
      List list1 = entry.getValue();
      List list2 = map2.get(entry.getKey());

      if (!listEquals(list1, list2)) {
        return false;
      }
    }
    return true;
  }

  private boolean listEquals(List list1, List list2) {
    Set set1 = new HashSet();
    set1.addAll(list1);
    Set set2 = new HashSet();
    set2.addAll(list2);

    return set1.equals(set2);
  }
}
