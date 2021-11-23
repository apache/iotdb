package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.unary.ConstantOperand;
import org.apache.iotdb.db.query.udf.core.reader.ConstantLayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UDAFQueryExecutor {
  protected UDAFPlan udafPlan;
  private Map<Expression, Integer> expressionToInnerResultIndexMap;
  private Map<Expression, TSDataType> expressionToDataTypeMap;
  private Set<ResultColumn> deduplicatedResultColumns;
  private List<Field> innerFields;

  public UDAFQueryExecutor(UDAFPlan udafPlan) {
    this.udafPlan = udafPlan;
    init();
  }

  private void init() {
    this.expressionToInnerResultIndexMap = udafPlan.getExpressionToInnerResultIndexMap();
    deduplicatedResultColumns = new LinkedHashSet<>();
    expressionToDataTypeMap = new HashMap<>();
    for (ResultColumn resultColumn : udafPlan.getResultColumns()) {
      deduplicatedResultColumns.add(resultColumn);
    }
  }

  public SingleDataSet convertInnerAggregationDataset(SingleDataSet singleDataSet)
      throws QueryProcessException, IOException {
    RowRecord innerRowRecord = singleDataSet.nextWithoutConstraint();
    innerFields = innerRowRecord.getFields();
    RowRecord record = new RowRecord(0);
    ArrayList<TSDataType> dataTypes = new ArrayList<>();
    ArrayList<PartialPath> paths = new ArrayList<>();
    for (ResultColumn resultColumn : deduplicatedResultColumns) {
      Expression expression = resultColumn.getExpression();
      Pair<TSDataType, Object> pair = calcUDAFExpression(expression);
      dataTypes.add(pair.left);
      if (!expressionToDataTypeMap.containsKey(expression)) {
        expressionToDataTypeMap.put(expression, pair.left);
      }
      paths.add(null);
      record.addField(pair.right, pair.left);
    }
    for (ResultColumn resultColumn : this.udafPlan.getResultColumns()) {
      resultColumn.setDataType(expressionToDataTypeMap.get(resultColumn.getExpression()));
    }
    SingleDataSet dataSet = new SingleDataSet(paths, dataTypes);
    dataSet.setRecord(record);
    return dataSet;
  }

  private Pair<TSDataType, Object> calcUDAFExpression(Expression expression)
      throws QueryProcessException, IOException {
    TSDataType dataType;
    Object value = null;
    if (expression instanceof BinaryExpression) {
      dataType = TSDataType.DOUBLE;
      Expression leftExpression = ((BinaryExpression) expression).getLeftExpression();
      Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
      value = 0.0;
      if (expression instanceof AdditionExpression) {
        value =
            Double.valueOf(calcUDAFExpression(leftExpression).right.toString())
                + Double.valueOf(calcUDAFExpression(rightExpression).right.toString());
      } else if (expression instanceof SubtractionExpression) {
        value =
            Double.valueOf(calcUDAFExpression(leftExpression).right.toString())
                - Double.valueOf(calcUDAFExpression(rightExpression).right.toString());
      } else if (expression instanceof MultiplicationExpression) {
        value =
            Double.valueOf(calcUDAFExpression(leftExpression).right.toString())
                * Double.valueOf(calcUDAFExpression(rightExpression).right.toString());
      } else if (expression instanceof DivisionExpression) {
        value =
            Double.valueOf(calcUDAFExpression(leftExpression).right.toString())
                / Double.valueOf(calcUDAFExpression(rightExpression).right.toString());
      } else if (expression instanceof ModuloExpression) {
        value =
            Double.valueOf(calcUDAFExpression(leftExpression).right.toString())
                % Double.valueOf(calcUDAFExpression(rightExpression).right.toString());
      }
    } else if (expression instanceof ConstantOperand) {
      ConstantLayerPointReader constantLayerPointReader =
          new ConstantLayerPointReader((ConstantOperand) expression);
      dataType = constantLayerPointReader.getDataType();
      switch (dataType) {
        case INT32:
          value = constantLayerPointReader.currentInt();
          break;
        case INT64:
          value = constantLayerPointReader.currentLong();
          break;
        case FLOAT:
          value = constantLayerPointReader.currentFloat();
          break;
        case DOUBLE:
          value = constantLayerPointReader.currentDouble();
          break;
        case BOOLEAN:
          value = constantLayerPointReader.currentBoolean();
          break;
        case TEXT:
          value = constantLayerPointReader.currentBinary();
          break;
      }
    } else {
      // FunctionExpression
      Field field = innerFields.get(expressionToInnerResultIndexMap.get(expression));
      dataType = field.getDataType();
      switch (dataType) {
        case INT32:
          value = field.getIntV();
          break;
        case INT64:
          value = field.getLongV();
          break;
        case FLOAT:
          value = field.getFloatV();
          break;
        case DOUBLE:
          value = field.getDoubleV();
          break;
        case BOOLEAN:
          value = field.getBoolV();
          break;
        case TEXT:
          value = field.getBinaryV();
          break;
      }
    }
    return new Pair<>(dataType, value);
  }
}
