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

package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

/** This is an internal example of the UDAF implementation. */
public class UDAFExample implements UDAF {
  /**
   * CREATE DATABASE root.sg; CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;
   * INSERT INTO root.sg(time, s1) VALUES (0, 1), (1, 3), (2, 5);
   *
   * <p>CREATE FUNCTION avg_udaf AS 'org.apache.iotdb.udf.UDAFExample'; SHOW FUNCTIONS; SELECT s1
   * FROM root.sg; SELECT avg_udaf(s1) FROM root.sg;
   */
  static class AvgState implements State {
    double sum;

    long count;

    @Override
    public void reset() {
      sum = 0;
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES + Long.BYTES);
      buffer.putDouble(sum);
      buffer.putLong(count);

      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      sum = buffer.getDouble();
      count = buffer.getLong();
    }
  }

  private ColumnNumericReader valueReader;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDAFConfigurations configurations) {
    valueReader =
        COLUMN_NUMERIC_READER_SERVICE.call(
            org.apache.tsfile.read.common.type.Type.fromTsDataType(
                TSDataType.getTsDataType(parameters.getDataType(0).getType())));
    configurations.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public State createState() {
    return new AvgState();
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) {
    AvgState avgState = (AvgState) state;

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[0].isNull(i)) {
        avgState.count++;
        avgState.sum += valueReader.read(columns[0], i);
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    AvgState avgState = (AvgState) state;
    AvgState avgRhs = (AvgState) rhs;

    avgState.count += avgRhs.count;
    avgState.sum += avgRhs.sum;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    AvgState avgState = (AvgState) state;

    if (avgState.count != 0) {
      resultValue.setDouble(avgState.sum / avgState.count);
    } else {
      resultValue.setNull();
    }
  }

  @Override
  public void removeState(State state, State removed) {
    AvgState avgState = (AvgState) state;
    AvgState avgRhs = (AvgState) removed;

    avgState.count -= avgRhs.count;
    avgState.sum -= avgRhs.sum;
  }

  private static final TypeService<ColumnNumericReader> COLUMN_NUMERIC_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> Column::getInt;
            case INT64 -> Column::getLong;
            case FLOAT -> Column::getFloat;
            case DOUBLE -> Column::getDouble;
            default ->
                (column, index) -> {
                  throw new UnSupportedDataTypeException("Unsupported data type: " + type);
                };
          };

  @FunctionalInterface
  private interface ColumnNumericReader {
    double read(Column column, int index);
  }
}
