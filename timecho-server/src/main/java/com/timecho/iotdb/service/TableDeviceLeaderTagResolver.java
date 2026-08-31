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
package com.timecho.iotdb.service;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderReq;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Arranges named TAG values in the source table's descriptor order. */
final class TableDeviceLeaderTagResolver {

  private TableDeviceLeaderTagResolver() {}

  static TTableDeviceLeaderReq resolve(
      final TTableDeviceLeaderReq request, final ITableCache tableCache) {
    Objects.requireNonNull(request);
    Objects.requireNonNull(tableCache);

    final List<String> tagColumnNames = request.getTagColumnNames();
    if (tagColumnNames == null) {
      return request;
    }

    final List<Boolean> isSetTag = request.getIsSetTag();
    final List<String> deviceId = request.getDeviceId();
    if (isSetTag == null
        || deviceId == null
        || isSetTag.isEmpty()
        || !Boolean.TRUE.equals(isSetTag.get(0))
        || deviceId.isEmpty()
        || deviceId.get(0) == null) {
      throw new TableDeviceLeaderResolver.InvalidRequestException();
    }

    final String logicalTableName = deviceId.get(0).toLowerCase(Locale.ENGLISH);
    final TsTable logicalTable = tableCache.getTable(request.getDbName(), logicalTableName, true);
    if (logicalTable == null) {
      throw new TableDeviceLeaderResolver.InvalidRequestException();
    }

    final String sourceDatabase;
    final String sourceTableName;
    final TsTable sourceTable;
    final WritableView writableView;
    if (logicalTable instanceof WritableView) {
      writableView = (WritableView) logicalTable;
      sourceDatabase = writableView.getSourceTableDatabase();
      sourceTableName = writableView.getSourceTableName();
      sourceTable = tableCache.getTable(sourceDatabase, sourceTableName, true);
      if (sourceTable == null) {
        throw new TableDeviceLeaderResolver.InvalidRequestException();
      }
    } else {
      writableView = null;
      sourceDatabase = request.getDbName();
      sourceTableName = logicalTableName;
      sourceTable = logicalTable;
    }

    final List<String> namedValues = new ArrayList<>();
    int deviceIdIndex = 1;
    for (int i = 1; i < isSetTag.size(); i++) {
      if (Boolean.TRUE.equals(isSetTag.get(i))) {
        if (deviceIdIndex >= deviceId.size() || deviceId.get(deviceIdIndex) == null) {
          throw new TableDeviceLeaderResolver.InvalidRequestException();
        }
        namedValues.add(deviceId.get(deviceIdIndex++));
      }
    }
    if (deviceIdIndex != deviceId.size() || namedValues.size() != tagColumnNames.size()) {
      throw new TableDeviceLeaderResolver.InvalidRequestException();
    }

    final List<TsTableColumnSchema> sourceTagSchemas = sourceTable.getTagColumnSchemaList();
    final boolean[] sourceTagSet = new boolean[sourceTagSchemas.size()];
    final String[] sourceTagValues = new String[sourceTagSchemas.size()];
    for (int i = 0; i < tagColumnNames.size(); i++) {
      final String tagColumnName = tagColumnNames.get(i);
      if (tagColumnName == null) {
        throw new TableDeviceLeaderResolver.InvalidRequestException();
      }
      if (writableView != null) {
        final TsTableColumnSchema viewTagSchema = writableView.getColumnSchema(tagColumnName);
        if (viewTagSchema == null
            || viewTagSchema.getColumnCategory() != TsTableColumnCategory.TAG) {
          throw new TableDeviceLeaderResolver.InvalidRequestException();
        }
      }
      final String sourceTagName =
          writableView == null ? tagColumnName : writableView.getOriginalColumnName(tagColumnName);
      final int sourceTagOrdinal = findTagOrdinal(sourceTagSchemas, sourceTagName);
      if (sourceTagOrdinal < 0 || sourceTagSet[sourceTagOrdinal]) {
        throw new TableDeviceLeaderResolver.InvalidRequestException();
      }
      sourceTagSet[sourceTagOrdinal] = true;
      sourceTagValues[sourceTagOrdinal] = namedValues.get(i);
    }

    final List<String> sourceDeviceId = new ArrayList<>(sourceTagSchemas.size() + 1);
    final List<Boolean> sourceIsSetTag = new ArrayList<>(sourceTagSchemas.size() + 1);
    sourceDeviceId.add(sourceTableName);
    sourceIsSetTag.add(true);
    for (int i = 0; i < sourceTagSchemas.size(); i++) {
      sourceIsSetTag.add(sourceTagSet[i]);
      if (sourceTagSet[i]) {
        sourceDeviceId.add(sourceTagValues[i]);
      }
    }
    return new TTableDeviceLeaderReq(
        sourceDatabase, sourceDeviceId, sourceIsSetTag, request.getTime());
  }

  private static int findTagOrdinal(
      final List<TsTableColumnSchema> sourceTagSchemas, final String sourceTagName) {
    if (sourceTagName == null) {
      return -1;
    }
    for (int i = 0; i < sourceTagSchemas.size(); i++) {
      if (sourceTagName.equalsIgnoreCase(sourceTagSchemas.get(i).getColumnName())) {
        return i;
      }
    }
    return -1;
  }
}
