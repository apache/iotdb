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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.commons.utils.BlobUtils;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UDTFConst implements UDTF {

  private static final Set<String> VALID_TYPES = new HashSet<>();

  static {
    VALID_TYPES.add(TSDataType.INT32.name());
    VALID_TYPES.add(TSDataType.DATE.name());
    VALID_TYPES.add(TSDataType.INT64.name());
    VALID_TYPES.add(TSDataType.TIMESTAMP.name());
    VALID_TYPES.add(TSDataType.FLOAT.name());
    VALID_TYPES.add(TSDataType.DOUBLE.name());
    VALID_TYPES.add(TSDataType.BOOLEAN.name());
    VALID_TYPES.add(TSDataType.TEXT.name());
    VALID_TYPES.add(TSDataType.STRING.name());
    VALID_TYPES.add(TSDataType.BLOB.name());
    VALID_TYPES.add(TSDataType.OBJECT.name());
  }

  private TSDataType dataType;

  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private boolean booleanValue;
  private Binary binaryValue;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFParameterNotValidException {
    validator
        .validateRequiredAttribute("value")
        .validateRequiredAttribute("type")
        .validate(
            type -> VALID_TYPES.contains((String) type),
            "the given value type is not supported.",
            validator.getParameters().getString("type"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    dataType = TSDataType.valueOf(parameters.getString("type"));
    switch (dataType) {
      case INT32:
        intValue = Integer.parseInt(parameters.getString("value"));
        break;
      case DATE:
        intValue = DateUtils.parseDateExpressionToInt(parameters.getString("value"));
        break;
      case INT64:
      case TIMESTAMP:
        longValue = Long.parseLong(parameters.getString("value"));
        break;
      case FLOAT:
        floatValue = Float.parseFloat(parameters.getString("value"));
        break;
      case DOUBLE:
        doubleValue = Double.parseDouble(parameters.getString("value"));
        break;
      case BOOLEAN:
        booleanValue = Boolean.parseBoolean(parameters.getString("value"));
        break;
      case TEXT:
      case STRING:
        binaryValue = BytesUtils.valueOf(parameters.getString("value"));
        break;
      case BLOB:
      case OBJECT:
        binaryValue = new Binary(BlobUtils.parseBlobString(parameters.getString("value")));
        break;
      default:
        throw new UnsupportedOperationException();
    }

    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    switch (dataType) {
      case INT32:
      case DATE:
        collector.putInt(row.getTime(), intValue);
        break;
      case INT64:
      case TIMESTAMP:
        collector.putLong(row.getTime(), longValue);
        break;
      case FLOAT:
        collector.putFloat(row.getTime(), floatValue);
        break;
      case DOUBLE:
        collector.putDouble(row.getTime(), doubleValue);
        break;
      case BOOLEAN:
        collector.putBoolean(row.getTime(), booleanValue);
        break;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        collector.putBinary(row.getTime(), UDFBinaryTransformer.transformToUDFBinary(binaryValue));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public Object transform(Row row) throws IOException {
    switch (dataType) {
      case INT32:
      case DATE:
        return intValue;
      case INT64:
      case TIMESTAMP:
        return longValue;
      case FLOAT:
        return floatValue;
      case DOUBLE:
        return doubleValue;
      case BOOLEAN:
        return booleanValue;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        return UDFBinaryTransformer.transformToUDFBinary(binaryValue);
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    int count = columns[0].getPositionCount();

    switch (dataType) {
      case INT32:
      case DATE:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeInt(intValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      case INT64:
      case TIMESTAMP:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeLong(longValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      case FLOAT:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeFloat(floatValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      case DOUBLE:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeDouble(doubleValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      case BOOLEAN:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeBoolean(booleanValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        for (int i = 0; i < count; i++) {
          boolean hasWritten = false;
          for (int j = 0; j < columns.length - 1; j++) {
            if (!columns[j].isNull(i)) {
              builder.writeBinary(binaryValue);
              hasWritten = true;
              break;
            }
          }
          if (!hasWritten) {
            builder.appendNull();
          }
        }
        return;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
