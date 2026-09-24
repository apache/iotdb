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
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class UDAFSum implements UDAF {

  private static final TypeService<ColumnValueGetter> COLUMN_VALUE_GETTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> Column::getInt;
            case INT64 -> Column::getLong;
            case FLOAT -> Column::getFloat;
            case DOUBLE -> Column::getDouble;
            default ->
                (column, index) -> {
                  throw new UnSupportedDataTypeException(
                          String.format(
                              "Unsupported data type in aggregation AVG : %s", type.getTypeEnum()))
                      .setChecked(false);
                };
          };

  static class SumState implements State {
    double sum = 0;

    boolean initResult = false;

    @Override
    public void reset() {
      sum = 0;
      initResult = false;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES + 1);
      buffer.putDouble(sum);
      buffer.put(initResult ? (byte) 1 : (byte) 0);

      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      sum = buffer.getDouble();
      initResult = (buffer.get() == (byte) 1);
    }
  }

  private org.apache.tsfile.read.common.type.Type dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDAFConfigurations configurations) {
    dataType =
        org.apache.tsfile.read.common.type.Type.fromTsDataType(
            UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0)));
    configurations.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public State createState() {
    return new SumState();
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) {
    SumState sumState = (SumState) state;
    final Column column = columns[0];
    final ColumnValueGetter valueGetter = COLUMN_VALUE_GETTER_SERVICE.call(dataType);
    final int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column.isNull(i)) {
        sumState.initResult = true;
        sumState.sum += valueGetter.getValue(column, i);
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    SumState sumState = (SumState) state;
    SumState sumRhs = (SumState) rhs;

    sumState.initResult |= sumRhs.initResult;
    sumState.sum += sumRhs.sum;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    SumState sumState = (SumState) state;

    if (sumState.initResult) {
      resultValue.setDouble(sumState.sum);
    } else {
      resultValue.setNull();
    }
  }

  @Override
  public void removeState(State state, State removed) {
    SumState sumState = (SumState) state;
    SumState sumRhs = (SumState) removed;

    sumState.sum -= sumRhs.sum;
  }

  @FunctionalInterface
  private interface ColumnValueGetter {
    double getValue(Column column, int index);
  }
}
