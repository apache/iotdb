/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.metadata;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.PathErrorException;

/**
 * This class stores all the metadata info for every deviceId and every timeseries.
 */
public class Metadata {

  private Map<String, List<ColumnSchema>> seriesMap;
  private Map<String, List<String>> deviceIdMap;

  public Metadata(Map<String, List<ColumnSchema>> seriesMap,
      Map<String, List<String>> deviceIdMap) {
    this.seriesMap = seriesMap;
    this.deviceIdMap = deviceIdMap;
  }

  /**
   * function for getting series for one type.
   */
  public List<ColumnSchema> getSeriesForOneType(String type) throws PathErrorException {
    if (this.seriesMap.containsKey(type)) {
      return seriesMap.get(type);
    } else {
      throw new PathErrorException("Input deviceIdType is not exist. " + type);
    }
  }

  /**
   * function for getting devices for one type.
   */
  public List<String> getDevicesForOneType(String type) throws PathErrorException {
    if (this.seriesMap.containsKey(type)) {
      return deviceIdMap.get(type);
    } else {
      throw new PathErrorException("Input deviceIdType is not exist. " + type);
    }
  }

  public Map<String, List<ColumnSchema>> getSeriesMap() {
    return seriesMap;
  }

  public Map<String, List<String>> getDeviceMap() {
    return deviceIdMap;
  }

  @Override
  public String toString() {
    return seriesMap.toString() + "\n" + deviceIdMap.toString();
  }

}
