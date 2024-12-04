package org.apache.iotdb.db.queryengine.plan.relational.metadata;

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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.rpc.TSStatusCode.QUERY_PROCESS_ERROR;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public enum InformationSchemaTable {
  QUERIES(
      new TableSchema(
          "queries",
          ImmutableList.of(
              new ColumnSchema(ColumnHeaderConstant.TIME, TIMESTAMP, false, TIME),
              new ColumnSchema(ColumnHeaderConstant.QUERY_ID, STRING, false, MEASUREMENT),
              new ColumnSchema(ColumnHeaderConstant.DATA_NODE_ID, INT32, false, MEASUREMENT),
              new ColumnSchema(ColumnHeaderConstant.ELAPSED_TIME, FLOAT, false, MEASUREMENT),
              new ColumnSchema(ColumnHeaderConstant.STATEMENT, STRING, false, MEASUREMENT),
              new ColumnSchema(ColumnHeaderConstant.SQL_DIALECT, STRING, false, MEASUREMENT))));

  public static final String INFORMATION_SCHEMA = "information_schema";

  private final TableSchema tableSchema;

  InformationSchemaTable(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public TableSchema getTableMetadata() {
    return tableSchema;
  }

  public String getSchemaTableName() {
    return tableSchema.getTableName();
  }

  public InformationSchemaTable fromStringValue(String value) {
    return valueOf(value);
  }

  public static Optional<TableSchema> getTableSchemaFromStringValue(String value) {
    try {
      return Optional.of(valueOf(value.toUpperCase(Locale.ENGLISH)).tableSchema);
    } catch (IllegalArgumentException e) {
      // No matched table
      return Optional.empty();
    }
  }

  public static List<TDataNodeLocation> getTargetDataNodes(String value) {
    switch (value) {
      case "queries":
        return getRunningDataNodeLocations();
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static List<TDataNodeLocation> getRunningDataNodeLocations() {
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetDataNodeLocationsResp showDataNodesResp = client.getRunningDataNodeLocations();
      if (showDataNodesResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBRuntimeException(
            "An error occurred when executing getRunningDataNodeLocations():"
                + showDataNodesResp.getStatus().getMessage(),
            QUERY_PROCESS_ERROR.getStatusCode());
      }
      return showDataNodesResp.getDataNodeLocationList();
    } catch (ClientManagerException | TException e) {
      throw new IoTDBRuntimeException(
          "An error occurred when executing getRunningDataNodeLocations():" + e.getMessage(),
          QUERY_PROCESS_ERROR.getStatusCode());
    }
  }
}
