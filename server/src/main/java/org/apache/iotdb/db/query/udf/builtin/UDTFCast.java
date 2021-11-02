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

package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFCast implements UDTF {

  protected TSDataType dataType;
  protected TSDataType outType;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    dataType = parameters.getDataType(0);

    String typeStr = parameters.getString("type");
    if (typeStr == null) {
      throw new Exception("No output type is set");
    }
    outType = getType(typeStr);
    if (outType == null) {
      throw new Exception(typeStr + " is an unknown type");
    }
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(outType);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long time = row.getTime();
    if (dataType == TSDataType.INT32) {
      int value = row.getInt(0);
      if (outType == TSDataType.INT32) collector.putInt(time, value);
      else if (outType == TSDataType.INT64) collector.putLong(time, value);
      else if (outType == TSDataType.FLOAT) collector.putFloat(time, value);
      else if (outType == TSDataType.DOUBLE) collector.putDouble(time, value);
      else if (outType == TSDataType.BOOLEAN) collector.putBoolean(time, value != 0);
      else if (outType == TSDataType.TEXT) collector.putString(time, String.valueOf(value));
    } else if (dataType == TSDataType.INT64) {
      long value = row.getLong(0);
      if (outType == TSDataType.INT32) collector.putInt(time, (int) value);
      else if (outType == TSDataType.INT64) collector.putLong(time, value);
      else if (outType == TSDataType.FLOAT) collector.putFloat(time, value);
      else if (outType == TSDataType.DOUBLE) collector.putDouble(time, value);
      else if (outType == TSDataType.BOOLEAN) collector.putBoolean(time, value != 0);
      else if (outType == TSDataType.TEXT) collector.putString(time, String.valueOf(value));
    } else if (dataType == TSDataType.FLOAT) {
      float value = row.getFloat(0);
      if (outType == TSDataType.INT32) collector.putInt(time, (int) value);
      else if (outType == TSDataType.INT64) collector.putLong(time, (long) value);
      else if (outType == TSDataType.FLOAT) collector.putFloat(time, value);
      else if (outType == TSDataType.DOUBLE) collector.putDouble(time, value);
      else if (outType == TSDataType.BOOLEAN) collector.putBoolean(time, value != 0.0);
      else if (outType == TSDataType.TEXT) collector.putString(time, String.valueOf(value));
    } else if (dataType == TSDataType.DOUBLE) {
      double value = row.getDouble(0);
      if (outType == TSDataType.INT32) collector.putInt(time, (int) value);
      else if (outType == TSDataType.INT64) collector.putLong(time, (long) value);
      else if (outType == TSDataType.FLOAT) collector.putFloat(time, (float) value);
      else if (outType == TSDataType.DOUBLE) collector.putDouble(time, value);
      else if (outType == TSDataType.BOOLEAN) collector.putBoolean(time, value != 0.0);
      else if (outType == TSDataType.TEXT) collector.putString(time, String.valueOf(value));
    } else if (dataType == TSDataType.BOOLEAN) {
      boolean value = row.getBoolean(0);
      if (outType == TSDataType.INT32) collector.putInt(time, !value ? 0 : 1);
      else if (outType == TSDataType.INT64) collector.putLong(time, !value ? 0l : 1l);
      else if (outType == TSDataType.FLOAT) collector.putFloat(time, (float) (!value ? 0.0 : 1.0));
      else if (outType == TSDataType.DOUBLE) collector.putDouble(time, !value ? 0.0 : 1.0);
      else if (outType == TSDataType.BOOLEAN) collector.putBoolean(time, value);
      else if (outType == TSDataType.TEXT) collector.putString(time, String.valueOf(value));
    } else if (dataType == TSDataType.TEXT) {
      String value = row.getString(0);

      if (outType == TSDataType.BOOLEAN)
        collector.putBoolean(time, !(value.equals("false") || value.equals("")));
      else if (outType == TSDataType.TEXT) collector.putString(time, value);
      else {
        if (isNumeric(value)) {
          double doubleValue = Double.valueOf(value);
          if (outType == TSDataType.INT32) collector.putInt(time, (int) doubleValue);
          else if (outType == TSDataType.INT64) collector.putLong(time, (long) doubleValue);
          else if (outType == TSDataType.FLOAT) collector.putFloat(time, (float) doubleValue);
          else if (outType == TSDataType.DOUBLE) collector.putDouble(time, doubleValue);
        }
      }
    }
  }

  private boolean isNumeric(String s) {
    try {
      Double.valueOf(s);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private TSDataType getType(String type) {
    switch (type) {
      case "INT32":
        return TSDataType.INT32;
      case "INT64":
        return TSDataType.INT64;
      case "FLOAT":
        return TSDataType.FLOAT;
      case "DOUBLE":
        return TSDataType.DOUBLE;
      case "BOOLEAN":
        return TSDataType.BOOLEAN;
      case "TEXT":
        return TSDataType.TEXT;
      default:
        return null;
    }
  }
}
