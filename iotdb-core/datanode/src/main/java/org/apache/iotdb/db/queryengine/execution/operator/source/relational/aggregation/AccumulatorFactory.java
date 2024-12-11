/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.queryengine.execution.aggregation.VarianceAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAvgAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedCountAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedExtremeAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedFirstAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedFirstByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedLastAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedLastByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedMaxAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedMaxByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedMinAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedMinByAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedModeAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedSumAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedVarianceAccumulator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.FIRST_BY;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST_BY;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isTimeColumn;

public class AccumulatorFactory {

  public static TableAccumulator createAccumulator(
      String functionName,
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending,
      String timeColumnName) {
    if (aggregationType == TAggregationType.UDAF) {
      // If UDAF accumulator receives raw input, it needs to check input's attribute
      throw new UnsupportedOperationException();
    } else if ((LAST_BY.getFunctionName().equals(functionName)
            || FIRST_BY.getFunctionName().equals(functionName))
        && inputExpressions.size() > 1) {
      boolean xIsTimeColumn = false;
      boolean yIsTimeColumn = false;
      if (isTimeColumn(inputExpressions.get(1), timeColumnName)) {
        yIsTimeColumn = true;
      } else if (isTimeColumn(inputExpressions.get(0), timeColumnName)) {
        xIsTimeColumn = true;
      }
      if (LAST_BY.getFunctionName().equals(functionName)) {
        return ascending
            ? new LastByAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn)
            : new LastByDescAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn);
      } else {
        return ascending
            ? new FirstByAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn)
            : new FirstByDescAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn);
      }
    } else {
      return createBuiltinAccumulator(
          aggregationType, inputDataTypes, inputExpressions, inputAttributes, ascending);
    }
  }

  public static GroupedAccumulator createGroupedAccumulator(
      String functionName,
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending) {
    if (aggregationType == TAggregationType.UDAF) {
      // If UDAF accumulator receives raw input, it needs to check input's attribute
      throw new UnsupportedOperationException();
    } else {
      return createBuiltinGroupedAccumulator(
          aggregationType, inputDataTypes, inputExpressions, inputAttributes, ascending);
    }
  }

  private static GroupedAccumulator createBuiltinGroupedAccumulator(
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending) {
    switch (aggregationType) {
      case COUNT:
        return new GroupedCountAccumulator();
      case AVG:
        return new GroupedAvgAccumulator(inputDataTypes.get(0));
      case SUM:
        return new GroupedSumAccumulator(inputDataTypes.get(0));
      case LAST:
        return new GroupedLastAccumulator(inputDataTypes.get(0));
      case FIRST:
        return new GroupedFirstAccumulator(inputDataTypes.get(0));
      case MAX:
        return new GroupedMaxAccumulator(inputDataTypes.get(0));
      case MIN:
        return new GroupedMinAccumulator(inputDataTypes.get(0));
      case EXTREME:
        return new GroupedExtremeAccumulator(inputDataTypes.get(0));
      case LAST_BY:
        return new GroupedLastByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case FIRST_BY:
        return new GroupedFirstByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case MAX_BY:
        return new GroupedMaxByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case MIN_BY:
        return new GroupedMinByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case MODE:
        return new GroupedModeAccumulator(inputDataTypes.get(0));
      case STDDEV:
      case STDDEV_SAMP:
        return new GroupedVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.STDDEV_SAMP);
      case STDDEV_POP:
        return new GroupedVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.STDDEV_POP);
      case VARIANCE:
      case VAR_SAMP:
        return new GroupedVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.VAR_SAMP);
      case VAR_POP:
        return new GroupedVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.VAR_POP);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  public static TableAccumulator createBuiltinAccumulator(
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending) {
    switch (aggregationType) {
      case COUNT:
        return new CountAccumulator();
      case AVG:
        return new AvgAccumulator(inputDataTypes.get(0));
      case SUM:
        return new SumAccumulator(inputDataTypes.get(0));
      case LAST:
        return ascending
            ? new LastAccumulator(inputDataTypes.get(0))
            : new LastDescAccumulator(inputDataTypes.get(0));
      case FIRST:
        return ascending
            ? new FirstAccumulator(inputDataTypes.get(0))
            : new FirstDescAccumulator(inputDataTypes.get(0));
      case MAX:
        return new MaxAccumulator(inputDataTypes.get(0));
      case MIN:
        return new MinAccumulator(inputDataTypes.get(0));
      case LAST_BY:
        return ascending
            ? new LastByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1), false, false)
            : new LastByDescAccumulator(inputDataTypes.get(0), inputDataTypes.get(1), false, false);
      case FIRST_BY:
        return ascending
            ? new FirstByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1), false, false)
            : new FirstByDescAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), false, false);
      case MAX_BY:
        return new TableMaxByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case MIN_BY:
        return new TableMinByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case EXTREME:
        return new ExtremeAccumulator(inputDataTypes.get(0));
      case MODE:
        return new TableModeAccumulator(inputDataTypes.get(0));
      case STDDEV:
      case STDDEV_SAMP:
        return new TableVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.STDDEV_SAMP);
      case STDDEV_POP:
        return new TableVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.STDDEV_POP);
      case VARIANCE:
      case VAR_SAMP:
        return new TableVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.VAR_SAMP);
      case VAR_POP:
        return new TableVarianceAccumulator(
            inputDataTypes.get(0), VarianceAccumulator.VarianceType.VAR_POP);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  public static boolean isMultiInputAggregation(TAggregationType aggregationType) {
    switch (aggregationType) {
      case MAX_BY:
      case MIN_BY:
        return true;
      default:
        return false;
    }
  }

  public static TableAccumulator createBuiltinMultiInputAccumulator(
      TAggregationType aggregationType, List<TSDataType> inputDataTypes) {
    switch (aggregationType) {
      case MAX_BY:
        checkState(inputDataTypes.size() == 2, "Wrong inputDataTypes size.");
        // return new MaxByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      case MIN_BY:
        checkState(inputDataTypes.size() == 2, "Wrong inputDataTypes size.");
        // return new MinByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1));
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  private static TableAccumulator createBuiltinSingleInputAccumulator(
      TAggregationType aggregationType,
      TSDataType tsDataType,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending) {
    switch (aggregationType) {
      case COUNT:
        return new CountAccumulator();
      case AVG:
        return new AvgAccumulator(tsDataType);
        /*case SUM:
          return new SumAccumulator(tsDataType);
        case EXTREME:
          return new ExtremeAccumulator(tsDataType);
        case MAX_TIME:
          return ascending ? new MaxTimeAccumulator() : new MaxTimeDescAccumulator();
        case MIN_TIME:
          return ascending ? new MinTimeAccumulator() : new MinTimeDescAccumulator();
        case MAX_VALUE:
          return new MaxValueAccumulator(tsDataType);
        case MIN_VALUE:
          return new MinValueAccumulator(tsDataType);
        case LAST_VALUE:
          return ascending
              ? new LastValueAccumulator(tsDataType)
              : new LastValueDescAccumulator(tsDataType);
        case FIRST_VALUE:
          return ascending
              ? new FirstValueAccumulator(tsDataType)
              : new FirstValueDescAccumulator(tsDataType);
        case COUNT_IF:
          return new CountIfAccumulator(
              initKeepEvaluator(inputExpressions.get(1)),
              Boolean.parseBoolean(inputAttributes.getOrDefault("ignoreNull", "true")));
        case TIME_DURATION:
          return new TimeDurationAccumulator();
        case MODE:
          return createModeAccumulator(tsDataType);
        case COUNT_TIME:
          return new CountTimeAccumulator();
        case STDDEV:
        case STDDEV_SAMP:
          return new VarianceAccumulator(tsDataType, VarianceAccumulator.VarianceType.STDDEV_SAMP);
        case STDDEV_POP:
          return new VarianceAccumulator(tsDataType, VarianceAccumulator.VarianceType.STDDEV_POP);
        case VARIANCE:
        case VAR_SAMP:
          return new VarianceAccumulator(tsDataType, VarianceAccumulator.VarianceType.VAR_SAMP);
        case VAR_POP:
          return new VarianceAccumulator(tsDataType, VarianceAccumulator.VarianceType.VAR_POP);*/
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  /*private Accumulator createModeAccumulator(TSDataType tsDataType) {
    switch (tsDataType) {
      case BOOLEAN:
        return new BooleanModeAccumulator();
      case TEXT:
        return new BinaryModeAccumulator();
      case INT32:
        return new IntModeAccumulator();
      case INT64:
        return new LongModeAccumulator();
      case FLOAT:
        return new FloatModeAccumulator();
      case DOUBLE:
        return new DoubleModeAccumulator();
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
      default:
        throw new IllegalArgumentException("Unknown data type: " + tsDataType);
    }
  }*/

  @FunctionalInterface
  public interface KeepEvaluator {
    boolean apply(long keep);
  }
}
