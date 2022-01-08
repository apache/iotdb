package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CalcTimeSeries implements TimeSeries {

  private final TimeSeries source;
  private final List<RelTSExpression> expressions;

  public static abstract class RelTSExpression {

    protected List<RelInput> inputs;

    public RelTSExpression(List<RelInput> inputs) {
      this.inputs = inputs;
    }

    public abstract Object apply(List<Object> inputs);

    public abstract TSDataType getResultType();

  }

  public static class NoopExpression extends RelTSExpression {

    public NoopExpression(RelInput input) {
      super(Collections.singletonList(input));
    }

    @Override
    public Object apply(List<Object> inputs) {
      return inputs.get(0);
    }

    @Override
    public TSDataType getResultType() {
      return inputs.get(0).getDataType();
    }
  }

  public static class DoubleExpression extends RelTSExpression {

    public DoubleExpression(RelInput input) {
      super(Collections.singletonList(input));
      if (input.getDataType() != TSDataType.INT32) {
        throw new IllegalArgumentException("Only supported for INT32!");
      }
    }

    @Override
    public Object apply(List<Object> inputs) {
      return 2 * (long)inputs.get(0);
    }

    @Override
    public TSDataType getResultType() {
      return inputs.get(0).getDataType();
    }
  }

  public static class AbsExpression extends RelTSExpression {

    public AbsExpression(RelInput input) {
      super(Collections.singletonList(input));
      if (input.getDataType() != TSDataType.INT32) {
        throw new IllegalArgumentException("Only supported for INT32!");
      }
    }

    @Override
    public Object apply(List<Object> inputs) {
      return Math.abs((long)inputs.get(0));
    }

    @Override
    public TSDataType getResultType() {
      return inputs.get(0).getDataType();
    }
  }

  public CalcTimeSeries(TimeSeries source, List<RelTSExpression> expressions) {
    this.source = source;
    this.expressions = expressions;
  }

  @Override
  public TSDataType[] getSpecification() {
    return expressions.stream().map(RelTSExpression::getResultType).toArray(TSDataType[]::new);
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public Object[] next() {
    Object[] next = source.next();

    if (next == null) {
      return null;
    }

    Object[] prototype = new Object[next.length + expressions.size()];

    System.arraycopy(next, 0, prototype, 0, next.length);

    // Manipulate in place
    for (int i = 0; i < expressions.size(); i++) {
      RelTSExpression expression = expressions.get(i);
      List<Object> inputs = expression.inputs.stream().map(input -> next[input.getInput()]).collect(Collectors.toList());
      prototype[next.length + i] = expression.apply(inputs);
    }

    return prototype;
  }
}
