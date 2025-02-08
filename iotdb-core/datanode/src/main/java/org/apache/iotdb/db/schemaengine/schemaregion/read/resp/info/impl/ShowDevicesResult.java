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

package org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class ShowDevicesResult extends ShowSchemaResult implements IDeviceSchemaInfo {
  private Boolean isAligned;
  private int templateId;

  private Function<String, Binary> attributeProvider;

  private String[] rawNodes = null;

  public ShowDevicesResult(final String name, final Boolean isAligned, final int templateId) {
    super(name);
    this.isAligned = isAligned;
    this.templateId = templateId;
  }

  public ShowDevicesResult(
      final String name, final Boolean isAligned, final int templateId, final String[] rawNodes) {
    super(name);
    this.isAligned = isAligned;
    this.templateId = templateId;
    this.rawNodes = rawNodes;
  }

  public Boolean isAligned() {
    return isAligned;
  }

  public int getTemplateId() {
    return templateId;
  }

  public void setAttributeProvider(final Function<String, Binary> attributeProvider) {
    this.attributeProvider = attributeProvider;
  }

  @Override
  public Binary getAttributeValue(final String attributeKey) {
    return attributeProvider.apply(attributeKey);
  }

  public String[] getRawNodes() {
    return rawNodes;
  }

  @Override
  public PartialPath getPartialPath() {
    return rawNodes == null ? super.getPartialPath() : new PartialPath(rawNodes);
  }

  public static ShowDevicesResult convertDeviceEntry2ShowDeviceResult(
      final DeviceEntry entry, final List<String> attributeColumns) {
    final ShowDevicesResult result =
        new ShowDevicesResult(
            entry.getDeviceID().toString(), null, -1, (String[]) entry.getDeviceID().getSegments());
    final Map<String, Binary> attributeProviderMap = new HashMap<>();
    for (int i = 0; i < attributeColumns.size(); ++i) {
      attributeProviderMap.put(attributeColumns.get(i), entry.getAttributeColumnValues().get(i));
    }
    result.setAttributeProvider(attributeProviderMap::get);
    return result;
  }

  @Override
  public String toString() {
    return "ShowDevicesResult{"
        + "name='"
        + path
        + ", rawNodes = "
        + Arrays.toString(rawNodes)
        + ", isAligned = "
        + isAligned
        + ", templateId = "
        + templateId
        + "}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShowDevicesResult result = (ShowDevicesResult) o;
    return Objects.equals(path, result.path)
        && isAligned == result.isAligned
        && templateId == result.templateId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, isAligned, templateId);
  }
}
