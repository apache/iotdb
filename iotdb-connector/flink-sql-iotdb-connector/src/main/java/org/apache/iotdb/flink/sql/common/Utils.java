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
package org.apache.iotdb.flink.sql.common;

import org.apache.iotdb.flink.sql.exception.UnsupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.NullFieldException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Utils {
  public static Object getValue(Field value, String dataType) {
    try {
      if ("INT32".equals(dataType)) {
        return value.getIntV();
      } else if ("INT64".equals(dataType)) {
        return value.getLongV();
      } else if ("FLOAT".equals(dataType)) {
        return value.getFloatV();
      } else if ("DOUBLE".equals(dataType)) {
        return value.getDoubleV();
      } else if ("BOOLEAN".equals(dataType)) {
        return value.getBoolV();
      } else if ("TEXT".equals(dataType)) {
        return StringData.fromString(value.getStringValue());
      } else {
        throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + dataType);
      }
    } catch (NullFieldException e) {
      return null;
    }
  }

  public static Object getValue(Field value, DataType dataType) {
    if (dataType.equals(DataTypes.INT())) {
      return value.getIntV();
    } else if (dataType.equals(DataTypes.BIGINT())) {
      return value.getLongV();
    } else if (dataType.equals(DataTypes.FLOAT())) {
      return value.getFloatV();
    } else if (dataType.equals(DataTypes.DOUBLE())) {
      return value.getDoubleV();
    } else if (dataType.equals(DataTypes.BOOLEAN())) {
      return value.getBoolV();
    } else if (dataType.equals(DataTypes.STRING())) {
      return StringData.fromString(value.getStringValue());
    } else {
      throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + dataType);
    }
  }

  public static Object getValue(RowData value, DataType dataType, int index) {
    try {
      if (dataType.equals(DataTypes.INT())) {
        return value.getInt(index);
      } else if (dataType.equals(DataTypes.BIGINT())) {
        return value.getLong(index);
      } else if (dataType.equals(DataTypes.FLOAT())) {
        return value.getFloat(index);
      } else if (dataType.equals(DataTypes.DOUBLE())) {
        return value.getDouble(index);
      } else if (dataType.equals(DataTypes.BOOLEAN())) {
        return value.getBoolean(index);
      } else if (dataType.equals(DataTypes.STRING())) {
        return value.getString(index).toString();
      } else {
        throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + dataType);
      }
    } catch (NullPointerException e) {
      return null;
    }
  }

  public static boolean isNumeric(String s) {
    Pattern pattern = Pattern.compile("[0-9]*");
    return pattern.matcher(s).matches();
  }

  public static RowData convert(RowRecord record, List<String> columnTypes) {
    ArrayList<Object> values = new ArrayList<>();
    values.add(record.getTimestamp());
    List<Field> fields = record.getFields();
    for (int i = 0; i < fields.size(); i++) {
      values.add(getValue(fields.get(i), columnTypes.get(i + 1)));
    }
    GenericRowData rowData = GenericRowData.of(values.toArray());
    return rowData;
  }
}
