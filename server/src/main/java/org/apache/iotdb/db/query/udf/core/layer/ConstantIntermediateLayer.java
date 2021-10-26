package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.query.expression.unary.ConstantExpression;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.reader.ConstantLayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;

public class ConstantIntermediateLayer extends IntermediateLayer {

  public ConstantIntermediateLayer(
      ConstantExpression expression, long queryId, float memoryBudgetInMB) {
    super(expression, queryId, memoryBudgetInMB);
  }

  @Override
  public LayerPointReader constructPointReader() {
    return new ConstantLayerPointReader((ConstantExpression) expression);
  }

  @Override
  public LayerRowReader constructRowReader() {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingTimeWindowReader(
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }
}
