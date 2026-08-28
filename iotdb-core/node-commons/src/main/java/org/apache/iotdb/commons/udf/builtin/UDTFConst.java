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
import org.apache.tsfile.read.common.type.Type;
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

  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private boolean booleanValue;
  private Binary binaryValue;
  private TypeServices.ConstantRowCollector rowCollector;
  private TypeServices.ConstantRowMapper rowMapper;
  private TypeServices.ConstantColumnValueWriter columnValueWriter;

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
    TSDataType dataType = TSDataType.valueOf(parameters.getString("type"));
    Type type = Type.fromTsDataType(dataType);
    TypeServices.CONSTANT_PARSER_SERVICE.call(type).parse(this, parameters);
    rowCollector = TypeServices.CONSTANT_ROW_COLLECTOR_SERVICE.call(type);
    rowMapper = TypeServices.CONSTANT_ROW_MAPPER_SERVICE.call(type);
    columnValueWriter = TypeServices.CONSTANT_COLUMN_VALUE_WRITER_SERVICE.call(type);

    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    rowCollector.collect(this, row, collector);
  }

  @Override
  public Object transform(Row row) throws IOException {
    return rowMapper.map(this);
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    writeConstant(columns, builder);
  }

  void parseInt(UDFParameters parameters) {
    intValue = Integer.parseInt(parameters.getString("value"));
  }

  void parseDate(UDFParameters parameters) {
    intValue = DateUtils.parseDateExpressionToInt(parameters.getString("value"));
  }

  void parseLong(UDFParameters parameters) {
    longValue = Long.parseLong(parameters.getString("value"));
  }

  void parseFloat(UDFParameters parameters) {
    floatValue = Float.parseFloat(parameters.getString("value"));
  }

  void parseDouble(UDFParameters parameters) {
    doubleValue = Double.parseDouble(parameters.getString("value"));
  }

  void parseBoolean(UDFParameters parameters) {
    booleanValue = Boolean.parseBoolean(parameters.getString("value"));
  }

  void parseText(UDFParameters parameters) {
    binaryValue = BytesUtils.valueOf(parameters.getString("value"));
  }

  void parseBlob(UDFParameters parameters) {
    binaryValue = new Binary(BlobUtils.parseBlobString(parameters.getString("value")));
  }

  int intValue() {
    return intValue;
  }

  long longValue() {
    return longValue;
  }

  float floatValue() {
    return floatValue;
  }

  double doubleValue() {
    return doubleValue;
  }

  boolean booleanValue() {
    return booleanValue;
  }

  org.apache.iotdb.udf.api.type.Binary binaryValue() {
    return UDFBinaryTransformer.transformToUDFBinary(binaryValue);
  }

  Binary tsFileBinaryValue() {
    return binaryValue;
  }

  private void writeConstant(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      boolean hasWritten = false;
      for (int j = 0; j < columns.length - 1; j++) {
        if (!columns[j].isNull(i)) {
          columnValueWriter.write(this, builder);
          hasWritten = true;
          break;
        }
      }
      if (!hasWritten) {
        builder.appendNull();
      }
    }
  }
}
