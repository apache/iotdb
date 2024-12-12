package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.config.AggregateFunctionConfig;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

public class MyCount implements AggregateFunction {

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
  public void validate(FunctionParameters parameters) throws UDFException {
    if (parameters.getChildExpressionsSize() == 0) {
      throw new UDFException("MyCount accepts at least one parameter");
    }
  }

  @Override
  public void beforeStart(FunctionParameters parameters, AggregateFunctionConfig configurations) {
    configurations.setOutputDataType(Type.INT64);
  }

  @Override
  public State createState() {
    return new CountState();
  }

  @Override
  public void addInput(State state, Record input) {
    CountState countState = (CountState) state;
    for (int i = 0; i < input.size(); i++) {
      if (!input.isNull(i)) {
        countState.count++;
        break;
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    ((CountState) state).count += ((CountState) rhs).count;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    resultValue.setLong(((CountState) state).count);
  }
}
