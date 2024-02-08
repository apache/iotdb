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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFAttributeNotProvidedException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesNumberNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public class UDTFCast implements UDTF {

  private TSDataType sourceDataType;
  private TSDataType targetDataType;

  @Override
  public void validate(UDFParameterValidator validator)
      throws UDFInputSeriesNumberNotValidException, UDFAttributeNotProvidedException {
    validator.validateInputSeriesNumber(1).validateRequiredAttribute("type");
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    sourceDataType = parameters.getDataType(0);
    targetDataType = TSDataType.valueOf(parameters.getString("type"));

    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(targetDataType);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (sourceDataType) {
      case INT32:
        cast(row.getTime(), row.getInt(0), collector);
        return;
      case INT64:
        cast(row.getTime(), row.getLong(0), collector);
        return;
      case FLOAT:
        cast(row.getTime(), row.getFloat(0), collector);
        return;
      case DOUBLE:
        cast(row.getTime(), row.getDouble(0), collector);
        return;
      case BOOLEAN:
        cast(row.getTime(), row.getBoolean(0), collector);
        return;
      case TEXT:
        cast(row.getTime(), row.getBinary(0), collector);
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, int value, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (targetDataType) {
      case INT32:
        collector.putInt(time, value);
        return;
      case INT64:
        collector.putLong(time, value);
        return;
      case FLOAT:
        collector.putFloat(time, value);
        return;
      case DOUBLE:
        collector.putDouble(time, value);
        return;
      case BOOLEAN:
        collector.putBoolean(time, value != 0);
        return;
      case TEXT:
        collector.putString(time, String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, long value, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (targetDataType) {
      case INT32:
        collector.putInt(time, (int) value);
        return;
      case INT64:
        collector.putLong(time, value);
        return;
      case FLOAT:
        collector.putFloat(time, value);
        return;
      case DOUBLE:
        collector.putDouble(time, value);
        return;
      case BOOLEAN:
        collector.putBoolean(time, value != 0L);
        return;
      case TEXT:
        collector.putString(time, String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, float value, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (targetDataType) {
      case INT32:
        collector.putInt(time, (int) value);
        return;
      case INT64:
        collector.putLong(time, (long) value);
        return;
      case FLOAT:
        collector.putFloat(time, value);
        return;
      case DOUBLE:
        collector.putDouble(time, value);
        return;
      case BOOLEAN:
        collector.putBoolean(time, value != 0f);
        return;
      case TEXT:
        collector.putString(time, String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, double value, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (targetDataType) {
      case INT32:
        collector.putInt(time, (int) value);
        return;
      case INT64:
        collector.putLong(time, (long) value);
        return;
      case FLOAT:
        collector.putFloat(time, (float) value);
        return;
      case DOUBLE:
        collector.putDouble(time, value);
        return;
      case BOOLEAN:
        collector.putBoolean(time, value != 0.0);
        return;
      case TEXT:
        collector.putString(time, String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, boolean value, PointCollector collector)
      throws IOException, QueryProcessException {
    switch (targetDataType) {
      case INT32:
        collector.putInt(time, value ? 1 : 0);
        return;
      case INT64:
        collector.putLong(time, value ? 1L : 0L);
        return;
      case FLOAT:
        collector.putFloat(time, value ? 1.0f : 0.0f);
        return;
      case DOUBLE:
        collector.putDouble(time, value ? 1.0 : 0.0);
        return;
      case BOOLEAN:
        collector.putBoolean(time, value);
        return;
      case TEXT:
        collector.putString(time, String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void cast(long time, Binary value, PointCollector collector)
      throws IOException, QueryProcessException {
    String stringValue = value.getStringValue();
    switch (targetDataType) {
      case INT32:
        try {
          collector.putInt(time, (int) Double.parseDouble(stringValue));
        } catch (Exception ignored) {
          // skip
        }
        return;
      case INT64:
        try {
          collector.putLong(time, (long) Double.parseDouble(stringValue));
        } catch (Exception ignored) {
          // skip
        }
        return;
      case FLOAT:
        try {
          collector.putFloat(time, (float) Double.parseDouble(stringValue));
        } catch (Exception ignored) {
          // skip
        }
        return;
      case DOUBLE:
        try {
          collector.putDouble(time, Double.parseDouble(stringValue));
        } catch (Exception ignored) {
          // skip
        }
        return;
      case BOOLEAN:
        collector.putBoolean(time, !("false".equals(stringValue) || "".equals(stringValue)));
        return;
      case TEXT:
        collector.putBinary(time, value);
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
