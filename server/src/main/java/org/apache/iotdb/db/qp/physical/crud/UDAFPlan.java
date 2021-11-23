package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;

public class UDAFPlan extends RawDataQueryPlan {

  // Construct an innerAggregationPlan using resultColumns of UDAFPlan
  private AggregationPlan innerAggregationPlan;
  private Map<Expression,Integer> expressionToInnerResultIndexMap;

  public UDAFPlan() {
    super();
    setOperatorType(OperatorType.UDAF);
  }

  public Map<Expression, Integer> getExpressionToInnerResultIndexMap() {
    return expressionToInnerResultIndexMap;
  }

  public void setExpressionToInnerResultIndexMap(
      Map<Expression, Integer> expressionToInnerResultIndexMap) {
    this.expressionToInnerResultIndexMap = expressionToInnerResultIndexMap;
  }

  public void setInnerAggregationPlan(
      AggregationPlan innerAggregationPlan) {
    this.innerAggregationPlan = innerAggregationPlan;
  }

  public AggregationPlan getInnerAggregationPlan() {
    return innerAggregationPlan;
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    Set<String> columnForDisplaySet = new HashSet<>();
    for(ResultColumn resultColumn:resultColumns){
      String columnForDisplay = resultColumn.getResultColumnName();
      if (!columnForDisplaySet.contains(columnForDisplay)) {
        int datasetOutputIndex = getPathToIndex().size();
        setColumnNameToDatasetOutputIndex(columnForDisplay, datasetOutputIndex);
        columnForDisplaySet.add(columnForDisplay);
    }
    }
  }
}
