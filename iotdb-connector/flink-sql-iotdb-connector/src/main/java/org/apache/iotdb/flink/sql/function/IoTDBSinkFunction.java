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
package org.apache.iotdb.flink.sql.function;

import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IoTDBSinkFunction implements SinkFunction<RowData> {
  private final List<Tuple2<String, DataType>> schema;
  private final List<String> nodeUrls;
  private final String user;
  private final String password;
  private final String device;
  private final boolean aligned;
  private final List<String> measurements;
  private final List<TSDataType> dataTypes;
  private static final Map<DataType, TSDataType> TYPE_MAP = new HashMap<>();

  private static Session session;

  static {
    TYPE_MAP.put(DataTypes.INT(), TSDataType.INT32);
    TYPE_MAP.put(DataTypes.BIGINT(), TSDataType.INT64);
    TYPE_MAP.put(DataTypes.FLOAT(), TSDataType.FLOAT);
    TYPE_MAP.put(DataTypes.DOUBLE(), TSDataType.DOUBLE);
    TYPE_MAP.put(DataTypes.BOOLEAN(), TSDataType.BOOLEAN);
    TYPE_MAP.put(DataTypes.STRING(), TSDataType.TEXT);
  }

  public IoTDBSinkFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    // Get schema
    this.schema = schemaWrapper.getSchema();
    // Get options
    nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).split(","));
    user = options.get(Options.USER);
    password = options.get(Options.PASSWORD);
    device = options.get(Options.DEVICE);
    aligned = options.get(Options.ALIGNED);
    // Get measurements and data types from schema
    measurements =
        schema.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
    dataTypes = schema.stream().map(field -> TYPE_MAP.get(field.f1)).collect(Collectors.toList());
  }

  @Override
  public void invoke(RowData rowData, Context context) throws Exception {
    // Open the session if the session has not been opened
    if (session == null) {
      session = new Session.Builder().nodeUrls(nodeUrls).username(user).password(password).build();
      session.open(false);
    }
    // Load data from RowData
    if (rowData.getRowKind().equals(RowKind.INSERT)
        || rowData.getRowKind().equals(RowKind.UPDATE_AFTER)) {
      long timestamp = rowData.getLong(0);
      ArrayList<String> measurementsOfRow = new ArrayList<>();
      ArrayList<TSDataType> dataTypesOfRow = new ArrayList<>();
      ArrayList<Object> values = new ArrayList<>();
      for (int i = 0; i < this.measurements.size(); i++) {
        Object value = Utils.getValue(rowData, schema.get(i).f1, i + 1);
        if (value == null) {
          continue;
        }
        measurementsOfRow.add(this.measurements.get(i));
        dataTypesOfRow.add(this.dataTypes.get(i));
        values.add(value);
      }
      // insert data
      if (aligned) {
        session.insertAlignedRecord(device, timestamp, measurementsOfRow, dataTypesOfRow, values);
      } else {
        session.insertRecord(device, timestamp, measurementsOfRow, dataTypesOfRow, values);
      }
    } else if (rowData.getRowKind().equals(RowKind.DELETE)) {
      ArrayList<String> paths = new ArrayList<>();
      for (String measurement : measurements) {
        paths.add(String.format("%s.%s", device, measurement));
      }
      session.deleteData(paths, rowData.getLong(0));
    } else if (rowData.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
      // do nothing
    }
  }

  @Override
  public void finish() throws Exception {
    if (session != null) {
      session.close();
    }
  }
}
