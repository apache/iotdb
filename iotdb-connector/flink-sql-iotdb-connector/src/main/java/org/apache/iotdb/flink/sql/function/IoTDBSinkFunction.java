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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.IllegalIoTDBPathException;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.enums.TSDataType;

import org.apache.commons.lang3.StringUtils;
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
  private final boolean aligned;
  private final Map<String, List<String>> deviceMeasurementMap;
  private final List<String> fields;
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
    aligned = options.get(Options.ALIGNED);
    // Get measurements and data types from schema
    fields = schema.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
    deviceMeasurementMap = parseFieldNames(fields);
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
      for (Map.Entry<String, List<String>> entry : deviceMeasurementMap.entrySet()) {
        ArrayList<String> measurementsOfRow = new ArrayList<>();
        ArrayList<TSDataType> dataTypesOfRow = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (String measurement : entry.getValue()) {
          int indexInSchema = fields.indexOf(String.format("%s.%s", entry.getKey(), measurement));
          Object value = Utils.getValue(rowData, schema.get(indexInSchema).f1, indexInSchema + 1);
          if (value == null) {
            continue;
          }
          measurementsOfRow.add(measurement);
          dataTypesOfRow.add(this.dataTypes.get(indexInSchema));
          values.add(value);
        }
        // insert data
        if (aligned) {
          session.insertAlignedRecord(
              entry.getKey(), timestamp, measurementsOfRow, dataTypesOfRow, values);
        } else {
          session.insertRecord(
              entry.getKey(), timestamp, measurementsOfRow, dataTypesOfRow, values);
        }
      }
    } else if (rowData.getRowKind().equals(RowKind.DELETE)) {
      session.deleteData(fields, rowData.getLong(0));
    }
  }

  @Override
  public void finish() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  private Map<String, List<String>> parseFieldNames(List<String> fieldNames) {
    HashMap<String, List<String>> resultMap = new HashMap<>();
    for (String fieldName : fieldNames) {
      try {
        String[] nodes = PathUtils.splitPathToDetachedNodes(fieldName);
        String measurement = nodes[nodes.length - 1];
        String device = StringUtils.join(Arrays.copyOfRange(nodes, 0, nodes.length - 1), '.');
        resultMap.putIfAbsent(device, new ArrayList<>());
        resultMap.get(device).add(measurement);
      } catch (IllegalPathException e) {
        throw new IllegalIoTDBPathException(e.getMessage());
      }
    }
    return resultMap;
  }
}
