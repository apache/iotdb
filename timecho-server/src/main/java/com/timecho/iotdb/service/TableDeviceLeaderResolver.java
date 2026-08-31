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
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderReq;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Resolves a logical table-model device to the physical device used for partitioning. */
final class TableDeviceLeaderResolver {

  private TableDeviceLeaderResolver() {}

  static ResolvedDevice resolve(final TTableDeviceLeaderReq request, final ITableCache tableCache) {
    Objects.requireNonNull(request);
    Objects.requireNonNull(tableCache);

    final List<Boolean> isSetTag = request.getIsSetTag();
    final List<String> deviceId = request.getDeviceId();
    if (isSetTag == null
        || deviceId == null
        || isSetTag.isEmpty()
        || !Boolean.TRUE.equals(isSetTag.get(0))) {
      throw new InvalidRequestException();
    }

    final List<String> logicalSegments = new ArrayList<>(isSetTag.size());
    int deviceIdIndex = 0;
    for (int i = 0; i < isSetTag.size(); i++) {
      if (Boolean.TRUE.equals(isSetTag.get(i))) {
        if (deviceIdIndex >= deviceId.size() || deviceId.get(deviceIdIndex) == null) {
          throw new InvalidRequestException();
        }
        final String segment = deviceId.get(deviceIdIndex++);
        logicalSegments.add(i == 0 ? segment.toLowerCase(Locale.ENGLISH) : segment);
      } else {
        logicalSegments.add(null);
      }
    }
    if (deviceIdIndex != deviceId.size()) {
      throw new InvalidRequestException();
    }

    final String logicalTableName = logicalSegments.get(0);
    final TsTable logicalTable = tableCache.getTable(request.getDbName(), logicalTableName, true);
    final IDeviceID logicalDevice =
        IDeviceID.Factory.DEFAULT_FACTORY.create(logicalSegments.toArray(new String[0]));
    if (!(logicalTable instanceof WritableView)) {
      return new ResolvedDevice(request.getDbName(), logicalDevice);
    }

    final WritableView writableView = (WritableView) logicalTable;
    if (writableView.getTagNum() != logicalSegments.size() - 1) {
      throw new InvalidRequestException();
    }
    final String sourceDatabase = writableView.getSourceTableDatabase();
    final String sourceTableName = writableView.getSourceTableName();
    // TAG values are intentionally kept in the order supplied by the caller. The overload that
    // accepts tag column names is responsible for arranging them in source-table DESC order before
    // this resolver is invoked.
    final List<String> sourceSegments = new ArrayList<>(logicalSegments);
    sourceSegments.set(0, sourceTableName);
    return new ResolvedDevice(
        sourceDatabase,
        IDeviceID.Factory.DEFAULT_FACTORY.create(sourceSegments.toArray(new String[0])));
  }

  static final class InvalidRequestException extends IllegalArgumentException {
    InvalidRequestException() {}
  }

  static final class ResolvedDevice {
    private final String database;
    private final IDeviceID deviceId;

    private ResolvedDevice(final String database, final IDeviceID deviceId) {
      this.database = database;
      this.deviceId = deviceId;
    }

    String getDatabase() {
      return database;
    }

    IDeviceID getDeviceId() {
      return deviceId;
    }
  }
}
