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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is specifically for standalone schema validation during data insertion. Since the
 * schema fetch is mainly based on device path, the schema is directly grouped by device rater than
 * organized as a trie.
 */
public class DeviceGroupSchemaTree implements ISchemaTree {

  private final Map<PartialPath, DeviceSchemaInfo> deviceSchemaInfoMap = new HashMap<>();

  @Override
  public DeviceSchemaInfo searchDeviceSchemaInfo(
      PartialPath devicePath, List<String> measurements) {
    return deviceSchemaInfoMap.get(devicePath).getSubDeviceSchemaInfo(measurements);
  }

  @Override
  public boolean isEmpty() {
    return deviceSchemaInfoMap.isEmpty();
  }

  public void addDeviceInfo(DeviceSchemaInfo deviceSchemaInfo) {
    deviceSchemaInfoMap.put(deviceSchemaInfo.getDevicePath(), deviceSchemaInfo);
  }

  public void merge(DeviceGroupSchemaTree schemaTree) {
    deviceSchemaInfoMap.putAll(schemaTree.deviceSchemaInfoMap);
  }

  @Override
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(
      PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(PartialPath pathPattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getBelongedDatabase(String pathName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getBelongedDatabase(PartialPath path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getDatabases() {
    throw new UnsupportedOperationException();
  }
}
