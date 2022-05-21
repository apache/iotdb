package org.apache.iotdb.db.mpp.plan.expression.ternary;

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary.BetweenTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary.TernaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

public class BetweenExpression extends TernaryExpression {
  private final boolean isNotBetween;

  public boolean isNotBetween() {
    return isNotBetween;
  }

  public BetweenExpression(
      Expression firstExpression,
      Expression secondExpression,
      Expression thirdExpression,
      boolean isNotBetween) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = isNotBetween;
  }

  public BetweenExpression(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = false;
  }

  public BetweenExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
    this.isNotBetween = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  protected TernaryTransformer constructTransformer(
      LayerPointReader firstParentLayerPointReader,
      LayerPointReader secondParentLayerPointReader,
      LayerPointReader thirdParentLayerPointReader) {
    return new BetweenTransformer(
        firstParentLayerPointReader, secondParentLayerPointReader, thirdParentLayerPointReader);
  }

  @Override
  protected String operator() {
    return "between";
  }

  @Override
  public TSDataType inferTypes(TypeProvider typeProvider) {
    return TSDataType.BOOLEAN;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.BETWEEN;
  }

  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNotBetween, byteBuffer);
  }

  public Expression getExpression() {
    return this;
  }
}
