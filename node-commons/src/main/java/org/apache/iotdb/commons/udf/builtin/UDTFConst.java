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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;

import java.util.HashSet;
import java.util.Set;

public class UDTFConst implements UDTF {

  private static final Set<String> VALID_TYPES = new HashSet<>();

  static {
    VALID_TYPES.add(TSDataType.INT32.name());
    VALID_TYPES.add(TSDataType.INT64.name());
    VALID_TYPES.add(TSDataType.FLOAT.name());
    VALID_TYPES.add(TSDataType.DOUBLE.name());
    VALID_TYPES.add(TSDataType.BOOLEAN.name());
    VALID_TYPES.add(TSDataType.TEXT.name());
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
      case INT64:
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
        binaryValue = Binary.valueOf(parameters.getString("value"));
        break;
      default:
        throw new UnsupportedOperationException();
    }

    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    switch (dataType) {
      case INT32:
        collector.putInt(row.getTime(), intValue);
        break;
      case INT64:
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
        collector.putBinary(row.getTime(), UDFBinaryTransformer.transformToUDFBinary(binaryValue));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
