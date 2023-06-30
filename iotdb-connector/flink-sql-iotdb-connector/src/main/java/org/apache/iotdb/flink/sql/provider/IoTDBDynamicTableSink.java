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
package org.apache.iotdb.flink.sql.provider;

import org.apache.iotdb.flink.sql.function.IoTDBSinkFunction;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class IoTDBDynamicTableSink implements DynamicTableSink {
  private final ReadableConfig options;
  private final TableSchema schema;

  public IoTDBDynamicTableSink(ReadableConfig options, TableSchema schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_BEFORE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    try {
      return SinkFunctionProvider.of(new IoTDBSinkFunction(options, new SchemaWrapper(schema)));
    } catch (IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DynamicTableSink copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return null;
  }
}
