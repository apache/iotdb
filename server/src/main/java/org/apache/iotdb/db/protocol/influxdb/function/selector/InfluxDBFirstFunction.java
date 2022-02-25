package org.apache.iotdb.db.protocol.influxdb.function.selector;

import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionValue;
import org.apache.iotdb.db.query.expression.Expression;

import java.util.List;

public class InfluxDBFirstFunction extends InfluxDBSelector {
  private Object value;

  public InfluxDBFirstFunction(List<Expression> expressionList) {
    super(expressionList);
    this.setTimestamp(Long.MIN_VALUE);
  }

  public InfluxDBFirstFunction(List<Expression> expressionList, String path) {
    super(expressionList, path);
  }

  @Override
  public InfluxDBFunctionValue calculate() {
    return new InfluxDBFunctionValue(value, this.getTimestamp());
  }

  @Override
  public InfluxDBFunctionValue calculateByIoTDBFunc() {
    return null;
  }

  @Override
  public void updateValueAndRelateValues(
      InfluxDBFunctionValue functionValue, List<Object> relatedValues) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (timestamp <= this.getTimestamp()) {
      this.value = value;
      this.setTimestamp(timestamp);
      this.setRelatedValues(relatedValues);
    }
  }
}
