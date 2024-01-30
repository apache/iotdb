package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

public class UDAFCount implements UDAF {
  static class CountState implements State {
    long count;

    @Override
    public void reset() {
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(count);

      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = buffer.getLong();
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN, Type.TEXT);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDAFConfigurations configurations) {
    configurations.setOutputDataType(Type.INT64);
  }

  @Override
  public State createState() {
    return new CountState();
  }

  @Override
  public void addInput(State state, Column[] column, BitMap bitMap) {
    CountState countState = (CountState) state;

    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        countState.count++;
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    CountState countState = (CountState) state;
    CountState countRhs = (CountState) rhs;

    countState.count += countRhs.count;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    CountState countState = (CountState) state;
    resultValue.setLong(countState.count);
  }

  @Override
  public void removeState(State state, State removed) {
    CountState countState = (CountState) state;
    CountState countRhs = (CountState) removed;

    countState.count -= countRhs.count;
  }
}
