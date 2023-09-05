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
package org.apache.iotdb.flink.sql.factory;

import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.IllegalIoTDBPathException;
import org.apache.iotdb.flink.sql.exception.IllegalOptionException;
import org.apache.iotdb.flink.sql.exception.IllegalSchemaException;
import org.apache.iotdb.flink.sql.exception.IllegalUrlPathException;
import org.apache.iotdb.flink.sql.exception.UnsupportedDataTypeException;
import org.apache.iotdb.flink.sql.provider.IoTDBDynamicTableSink;
import org.apache.iotdb.flink.sql.provider.IoTDBDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class IoTDBDynamicTableFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final HashSet<DataType> supportedDataTypes = new HashSet<>();

  static {
    supportedDataTypes.add(DataTypes.INT());
    supportedDataTypes.add(DataTypes.BIGINT());
    supportedDataTypes.add(DataTypes.FLOAT());
    supportedDataTypes.add(DataTypes.DOUBLE());
    supportedDataTypes.add(DataTypes.BOOLEAN());
    supportedDataTypes.add(DataTypes.STRING());
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig options = helper.getOptions();
    TableSchema schema = context.getCatalogTable().getSchema();

    validate(options, schema);

    return new IoTDBDynamicTableSource(options, schema);
  }

  @Override
  public String factoryIdentifier() {
    return "IoTDB";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    HashSet<ConfigOption<?>> requiredOptions = new HashSet<>();
    requiredOptions.add(Options.DEVICE);

    return requiredOptions;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    HashSet<ConfigOption<?>> optionalOptions = new HashSet<>();
    optionalOptions.add(Options.NODE_URLS);
    optionalOptions.add(Options.USER);
    optionalOptions.add(Options.PASSWORD);
    optionalOptions.add(Options.LOOKUP_CACHE_MAX_ROWS);
    optionalOptions.add(Options.LOOKUP_CACHE_TTL_SEC);
    optionalOptions.add(Options.ALIGNED);
    optionalOptions.add(Options.MODE);
    optionalOptions.add(Options.CDC_TASK_NAME);
    optionalOptions.add(Options.CDC_PORT);

    return optionalOptions;
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig options = helper.getOptions();
    TableSchema schema = context.getCatalogTable().getSchema();

    validate(options, schema);

    return new IoTDBDynamicTableSink(options, schema);
  }

  protected void validate(ReadableConfig options, TableSchema schema) {
    String[] fieldNames = schema.getFieldNames();
    DataType[] fieldDataTypes = schema.getFieldDataTypes();

    if (!"Time_".equals(fieldNames[0]) || !fieldDataTypes[0].equals(DataTypes.BIGINT())) {
      throw new IllegalSchemaException(
          "The first field's name must be `Time_`, and its data type must be BIGINT.");
    }
    for (String fieldName : fieldNames) {
      if (fieldName.contains("\\.")) {
        throw new IllegalIoTDBPathException(
            String.format(
                "The field name `%s` contains character `.`, it's not allowed in IoTDB.",
                fieldName));
      }
      if (Utils.isNumeric(fieldName)) {
        throw new IllegalIoTDBPathException(
            String.format(
                "The field name `%s` is a pure number, which is not allowed in IoTDB.", fieldName));
      }
    }

    for (DataType fieldDataType : fieldDataTypes) {
      if (!supportedDataTypes.contains(fieldDataType)) {
        throw new UnsupportedDataTypeException(
            "IoTDB doesn't support the data type: " + fieldDataType);
      }
    }

    String device = options.get(Options.DEVICE);
    if (!device.startsWith("root.")) {
      throw new IllegalIoTDBPathException("The option `device` must starts with 'root.'.");
    }
    for (String s : device.split("\\.")) {
      if (Utils.isNumeric(s)) {
        throw new IllegalIoTDBPathException(
            String.format(
                "The option `device` contains a purely number path: `%s`, it's not allowed in IoTDB.",
                s));
      }
    }

    String[] nodeUrls = options.get(Options.NODE_URLS).split(",");
    for (String nodeUrl : nodeUrls) {
      String[] split = nodeUrl.split(":");
      if (split.length != 2) {
        throw new IllegalUrlPathException("Every node's URL must be in the format of `host:port`.");
      }
      if (!Utils.isNumeric(split[1])) {
        throw new IllegalUrlPathException(
            String.format("The port in url %s must be a number.", nodeUrl));
      } else {
        int port = Integer.parseInt(split[1]);
        if (port > 65535) {
          throw new IllegalUrlPathException(
              String.format("The port in url %s must be smaller than 65536", nodeUrl));
        } else if (port < 1) {
          throw new IllegalUrlPathException(
              String.format("The port in url %s must be greater than 0.", nodeUrl));
        }
      }
    }

    Long lowerBound = options.get(Options.SCAN_BOUNDED_LOWER_BOUND);
    Long upperBound = options.get(Options.SCAN_BOUNDED_UPPER_BOUND);
    if (lowerBound > 0L && upperBound > 0L && upperBound < lowerBound) {
      throw new IllegalOptionException(
          "The value of option `scan.bounded.lower-bound` could not be greater than the value of option `scan.bounded.upper-bound`.");
    }

    if (options.get(Options.MODE) == Options.Mode.CDC
        && options.get(Options.CDC_TASK_NAME) == null) {
      throw new IllegalOptionException(
          "The option `cdc.task.name` is required when option `mode` equals `CDC`");
    }
  }
}
