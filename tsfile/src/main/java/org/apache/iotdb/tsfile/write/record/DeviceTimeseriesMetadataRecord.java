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
package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DeviceTimeseriesMetadataRecord {

  private final TreeMap<String, TimeseriesMetadata> map;

  public DeviceTimeseriesMetadataRecord() {
    this.map = new TreeMap<>();
  }

  public TreeMap<String, TimeseriesMetadata> getMap() {
    return map;
  }

  public Collection<TimeseriesMetadata> getTimeseriesMetadatas() {
    return map.values();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public TimeseriesMetadata getLastValue() {
    return map.lastEntry().getValue();
  }

  public boolean containsMeasurement(String measurementId) {
    return map.containsKey(measurementId);
  }

  public void addMeasurementTimeseriesMetadataRecord(
      String measurementId, TimeseriesMetadata timeseriesMetadata) {
    map.put(measurementId, timeseriesMetadata);
  }

  public TimeseriesMetadata getStoredStatistics(String measurementId) {
    return map.get(measurementId);
  }

  public Set<Map.Entry<String, TimeseriesMetadata>> getEntrySet() {
    return map.entrySet();
  }
}
