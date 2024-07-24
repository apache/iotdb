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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.DeviceAttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DeviceInCacheFilterVisitor extends SchemaFilterVisitor<DeviceEntry> {

  private final Map<String, Integer> attributeIndexMap = new HashMap<>();

  DeviceInCacheFilterVisitor(List<String> attributeColumns) {
    for (int i = 0; i < attributeColumns.size(); i++) {
      attributeIndexMap.put(attributeColumns.get(i), i);
    }
  }

  @Override
  protected boolean visitNode(SchemaFilter filter, DeviceEntry deviceEntry) {
    return false;
  }

  @Override
  public boolean visitPreciseFilter(PreciseFilter filter, DeviceEntry deviceEntry) {
    IDeviceID deviceID = deviceEntry.getDeviceID();
    // the first segment is "tableName", skip it
    int index = filter.getIndex() + 1;
    // if index out of array bound, means that value will be null
    if (deviceID.segmentNum() <= index) {
      return filter.getValue() == null;
    } else {
      return Objects.equals(deviceID.segment(index), filter.getValue());
    }
  }

  @Override
  public boolean visitDeviceAttributeFilter(DeviceAttributeFilter filter, DeviceEntry deviceEntry) {
    return Objects.equals(
        deviceEntry.getAttributeColumnValues().get(attributeIndexMap.get(filter.getKey())),
        filter.getValue());
  }
}
