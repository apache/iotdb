package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.ConstantIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Constant expression */
public class ConstantExpression extends Expression {

  private final Object value;
  private final TSDataType dataType;

  public ConstantExpression(TSDataType dataType, String str) throws QueryProcessException {
    this.dataType = dataType;
    this.value = CommonUtils.parseValue(dataType, str);
  }

  public Object getValue() {
    return value;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public boolean isPureConstantExpression() {
    return true;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    resultExpressions.add(this);
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    // Do nothing
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    // Do nothing
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    // Do nothing
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFPlan udtfPlan,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner) {
    expressionDataTypeMap.put(this, this.getDataType());
    IntermediateLayer intermediateLayer =
        new ConstantIntermediateLayer(this, queryId, memoryAssigner.assign());
    expressionIntermediateLayerMap.put(this, intermediateLayer);
    return intermediateLayer;
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
