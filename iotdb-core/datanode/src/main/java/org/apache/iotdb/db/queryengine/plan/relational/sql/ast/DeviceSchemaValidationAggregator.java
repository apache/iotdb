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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;

import org.apache.tsfile.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class DeviceSchemaValidationAggregator {

  private final Map<String, Map<String, CoalescedDeviceSchemaValidation>> validationMap =
      new LinkedHashMap<>();

  void add(final ITableDeviceSchemaValidation validation) {
    final List<Object[]> deviceIdList = validation.getDeviceIdList();
    if (deviceIdList.isEmpty()) {
      return;
    }

    final List<String> attributeColumnNameList = validation.getAttributeColumnNameList();
    final List<Object[]> attributeValueList = validation.getAttributeValueList();
    final String database = validation.getDatabase();
    final String tableName = validation.getTableName();
    final CoalescedDeviceSchemaValidation coalescedValidation =
        validationMap
            .computeIfAbsent(database, key -> new LinkedHashMap<>())
            .computeIfAbsent(
                tableName, key -> new CoalescedDeviceSchemaValidation(database, tableName));

    for (int i = 0; i < deviceIdList.size(); i++) {
      coalescedValidation.add(
          deviceIdList.get(i), attributeColumnNameList, attributeValueList.get(i));
    }
  }

  void forEach(final Consumer<ITableDeviceSchemaValidation> consumer) {
    validationMap.values().forEach(tableMap -> tableMap.values().forEach(consumer::accept));
  }

  private static class CoalescedDeviceSchemaValidation implements ITableDeviceSchemaValidation {

    private final String database;
    private final String tableName;
    private final Map<DeviceIdKey, Map<Integer, Object>> deviceAttributeValueMap =
        new LinkedHashMap<>();
    private final List<String> attributeColumnNameList = new ArrayList<>();
    private final Map<String, Integer> attributeColumnIndexMap = new LinkedHashMap<>();

    private CoalescedDeviceSchemaValidation(final String database, final String tableName) {
      this.database = database;
      this.tableName = tableName;
    }

    private void add(
        final Object[] deviceId,
        final List<String> attributeColumnNames,
        final Object[] attributeValues) {
      final Map<Integer, Object> attributeValueMap =
          deviceAttributeValueMap.computeIfAbsent(
              new DeviceIdKey(deviceId), key -> new HashMap<>());
      for (int i = 0; i < attributeColumnNames.size(); i++) {
        final int attributeColumnIndex =
            attributeColumnIndexMap.computeIfAbsent(
                attributeColumnNames.get(i),
                attributeColumnName -> {
                  attributeColumnNameList.add(attributeColumnName);
                  return attributeColumnNameList.size() - 1;
                });
        if (i < attributeValues.length
            && attributeValues[i] != null
            && attributeValues[i] != Constants.NONE) {
          attributeValueMap.put(attributeColumnIndex, attributeValues[i]);
        }
      }
    }

    @Override
    public String getDatabase() {
      return database;
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    @Override
    public List<Object[]> getDeviceIdList() {
      final List<Object[]> deviceIdList = new ArrayList<>(deviceAttributeValueMap.size());
      deviceAttributeValueMap.keySet().forEach(key -> deviceIdList.add(key.deviceId));
      return deviceIdList;
    }

    @Override
    public List<String> getAttributeColumnNameList() {
      return attributeColumnNameList;
    }

    @Override
    public List<Object[]> getAttributeValueList() {
      final List<Object[]> attributeValueList = new ArrayList<>(deviceAttributeValueMap.size());
      for (final Map<Integer, Object> attributeValueMap : deviceAttributeValueMap.values()) {
        final Object[] attributeValues = new Object[attributeColumnNameList.size()];
        Arrays.fill(attributeValues, Constants.NONE);
        attributeValueMap.forEach((index, value) -> attributeValues[index] = value);
        attributeValueList.add(attributeValues);
      }
      return attributeValueList;
    }
  }

  private static class DeviceIdKey {

    private final Object[] deviceId;

    private DeviceIdKey(final Object[] deviceId) {
      this.deviceId = (Object[]) deviceId.clone();
    }

    @Override
    public boolean equals(final Object object) {
      return this == object
          || object instanceof DeviceIdKey
              && Arrays.equals(deviceId, ((DeviceIdKey) object).deviceId);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(deviceId);
    }
  }
}
