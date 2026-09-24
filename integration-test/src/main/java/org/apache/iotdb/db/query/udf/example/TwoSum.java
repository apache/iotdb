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

package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.read.common.type.service.TypeService;

public class TwoSum implements UDTF {

  private static final TypeService<TwoSumTransformer> TWO_SUM_TRANSFORMER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> row -> row.getInt(0) + row.getInt(1);
            case INT64 -> row -> row.getLong(0) + row.getLong(1);
            case FLOAT -> row -> row.getFloat(0) + row.getFloat(1);
            case DOUBLE -> row -> row.getDouble(0) + row.getDouble(1);
            default ->
                row -> {
                  throw new Exception();
                };
          };

  private org.apache.tsfile.read.common.type.Type dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateInputSeriesDataType(1, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    final Type udfDataType = parameters.getDataType(0);
    dataType =
        org.apache.tsfile.read.common.type.Type.fromTsDataType(
            UDFDataTypeTransformer.transformToTsDataType(udfDataType));
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(udfDataType);
  }

  @Override
  public Object transform(Row row) throws Exception {
    return TWO_SUM_TRANSFORMER_SERVICE.call(dataType).transform(row);
  }

  @FunctionalInterface
  private interface TwoSumTransformer {
    Object transform(Row row) throws Exception;
  }
}
