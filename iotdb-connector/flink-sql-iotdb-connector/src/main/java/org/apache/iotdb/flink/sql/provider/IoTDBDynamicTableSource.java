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

import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.function.IoTDBBoundedScanFunction;
import org.apache.iotdb.flink.sql.function.IoTDBCDCSourceFunction;
import org.apache.iotdb.flink.sql.function.IoTDBLookupFunction;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;

public class IoTDBDynamicTableSource implements LookupTableSource, ScanTableSource {
  private final ReadableConfig options;
  private final TableSchema schema;

  public IoTDBDynamicTableSource(ReadableConfig options, TableSchema schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    return TableFunctionProvider.of(new IoTDBLookupFunction(options, new SchemaWrapper(schema)));
  }

  @Override
  public DynamicTableSource copy() {
    return new IoTDBDynamicTableSource(options, schema);
  }

  @Override
  public String asSummaryString() {
    return "IoTDB Dynamic Table Source";
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    if (options.get(Options.MODE) == Options.Mode.CDC) {
      return SourceFunctionProvider.of(
          new IoTDBCDCSourceFunction(options, new SchemaWrapper(schema)), false);
    } else {
      return InputFormatProvider.of(
          new IoTDBBoundedScanFunction(options, new SchemaWrapper(schema)));
    }
  }
}
