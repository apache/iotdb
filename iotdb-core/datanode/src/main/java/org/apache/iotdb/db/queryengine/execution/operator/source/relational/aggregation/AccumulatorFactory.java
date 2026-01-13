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
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.execution.aggregation.VarianceAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.BinaryGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.BlobGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.BooleanGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.DoubleGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.FloatGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedApproxCountDistinctAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedApproxPercentileAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedApproxPercentileWithWeightAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAvgAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedCountAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedCountAllAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedCountIfAccumulator;
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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedUserDefinedAggregateAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedVarianceAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.IntGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.LongGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.MarkDistinctHash;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.relational.AggregateFunction;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.FIRST_BY;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST_BY;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isMeasurementColumn;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isTimeColumn;
import static org.apache.tsfile.read.common.type.IntType.INT32;

public class AccumulatorFactory {

  public static TableAccumulator createAccumulator(
      String functionName,
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending,
      boolean isAggTableScan,
      String timeColumnName,
      Set<String> measurementColumnNames,
      boolean distinct) {
    TableAccumulator result;

    if (aggregationType == TAggregationType.UDAF) {
      // If UDAF accumulator receives raw input, it needs to check input's attribute
      result = createUDAFAccumulator(functionName, inputDataTypes, inputAttributes);
    } else if ((LAST_BY.getFunctionName().equals(functionName)
            || FIRST_BY.getFunctionName().equals(functionName))
        && inputExpressions.size() > 1) {
      boolean xIsTimeColumn = isTimeColumn(inputExpressions.get(0), timeColumnName);
      boolean yIsTimeColumn = isTimeColumn(inputExpressions.get(1), timeColumnName);
      // When used in AggTableScanOperator, we can finish calculation of
      // LastDesc/LastByDesc/First/First_by after the result has been initialized
      if (LAST_BY.getFunctionName().equals(functionName)) {
        result =
            ascending
                ? new LastByAccumulator(
                    inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn)
                : new LastByDescAccumulator(
                    inputDataTypes.get(0),
                    inputDataTypes.get(1),
                    xIsTimeColumn,
                    yIsTimeColumn,
                    isMeasurementColumn(inputExpressions.get(0), measurementColumnNames),
                    isMeasurementColumn(inputExpressions.get(1), measurementColumnNames),
                    isAggTableScan);
      } else {
        result =
            ascending
                ? new FirstByAccumulator(
                    inputDataTypes.get(0),
                    inputDataTypes.get(1),
                    xIsTimeColumn,
                    yIsTimeColumn,
                    isAggTableScan)
                : new FirstByDescAccumulator(
                    inputDataTypes.get(0), inputDataTypes.get(1), xIsTimeColumn, yIsTimeColumn);
      }
    } else if (LAST.getFunctionName().equals(functionName)) {
      return ascending
          ? new LastAccumulator(inputDataTypes.get(0))
          : new LastDescAccumulator(
              inputDataTypes.get(0),
              isTimeColumn(inputExpressions.get(0), timeColumnName),
              isMeasurementColumn(inputExpressions.get(0), measurementColumnNames),
              isAggTableScan);
    } else {
      result =
          createBuiltinAccumulator(
              aggregationType,
              inputDataTypes,
              inputExpressions,
              inputAttributes,
              ascending,
              isAggTableScan);
    }

    if (distinct) {
      result =
          new DistinctAccumulator(
              result,
              inputDataTypes.stream()
                  .map(InternalTypeManager::fromTSDataType)
                  .collect(Collectors.toList()));
    }

    return result;
  }

  public static GroupedAccumulator createGroupedAccumulator(
      String functionName,
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending,
      boolean distinct) {
    GroupedAccumulator result;

    if (aggregationType == TAggregationType.UDAF) {
      // If UDAF accumulator receives raw input, it needs to check input's attribute
      result = createGroupedUDAFAccumulator(functionName, inputDataTypes, inputAttributes);
    } else {
      result =
          createBuiltinGroupedAccumulator(
              aggregationType, inputDataTypes, inputExpressions, inputAttributes, ascending);
    }

    if (distinct) {
      result =
          new DistinctGroupedAccumulator(
              result,
              inputDataTypes.stream()
                  .map(InternalTypeManager::fromTSDataType)
                  .collect(Collectors.toList()));
    }

    return result;
  }

  private static TableAccumulator createUDAFAccumulator(
      String functionName, List<TSDataType> inputDataTypes, Map<String, String> inputAttributes) {
    AggregateFunction aggregateFunction = TableUDFUtils.getAggregateFunction(functionName);
    FunctionArguments functionArguments =
        new FunctionArguments(
            UDFDataTypeTransformer.transformToUDFDataTypeList(inputDataTypes), inputAttributes);
    aggregateFunction.beforeStart(functionArguments);
    return new UserDefinedAggregateFunctionAccumulator(
        aggregateFunction.analyze(functionArguments),
        aggregateFunction,
        inputDataTypes.stream().map(TypeFactory::getType).collect(Collectors.toList()));
  }

  private static GroupedAccumulator createGroupedUDAFAccumulator(
      String functionName, List<TSDataType> inputDataTypes, Map<String, String> inputAttributes) {
    AggregateFunction aggregateFunction = TableUDFUtils.getAggregateFunction(functionName);
    FunctionArguments functionArguments =
        new FunctionArguments(
            UDFDataTypeTransformer.transformToUDFDataTypeList(inputDataTypes), inputAttributes);
    aggregateFunction.beforeStart(functionArguments);
    return new GroupedUserDefinedAggregateAccumulator(
        aggregateFunction,
        inputDataTypes.stream().map(TypeFactory::getType).collect(Collectors.toList()));
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
      case COUNT_ALL:
        return new GroupedCountAllAccumulator();
      case COUNT_IF:
        return new GroupedCountIfAccumulator();
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
      case APPROX_COUNT_DISTINCT:
        return new GroupedApproxCountDistinctAccumulator(inputDataTypes.get(0));
      case APPROX_MOST_FREQUENT:
        return getGroupedApproxMostFrequentAccumulator(inputDataTypes.get(0));
      case APPROX_PERCENTILE:
        if (inputDataTypes.size() == 2) {
          return new GroupedApproxPercentileAccumulator(inputDataTypes.get(0));
        } else {
          return new GroupedApproxPercentileWithWeightAccumulator(inputDataTypes.get(0));
        }
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  public static TableAccumulator createBuiltinAccumulator(
      TAggregationType aggregationType,
      List<TSDataType> inputDataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending,
      boolean isAggTableScan) {
    switch (aggregationType) {
      case COUNT:
        return new CountAccumulator();
      case COUNT_ALL:
        return new CountAllAccumulator();
      case COUNT_IF:
        return new CountIfAccumulator();
      case AVG:
        return new AvgAccumulator(inputDataTypes.get(0));
      case SUM:
        return new SumAccumulator(inputDataTypes.get(0));
      case LAST:
        return ascending
            ? new LastAccumulator(inputDataTypes.get(0))
            : new LastDescAccumulator(inputDataTypes.get(0), false, false, isAggTableScan);
      case FIRST:
        return ascending
            ? new FirstAccumulator(inputDataTypes.get(0), isAggTableScan)
            : new FirstDescAccumulator(inputDataTypes.get(0));
      case MAX:
        return new MaxAccumulator(inputDataTypes.get(0));
      case MIN:
        return new MinAccumulator(inputDataTypes.get(0));
      case LAST_BY:
        return ascending
            ? new LastByAccumulator(inputDataTypes.get(0), inputDataTypes.get(1), false, false)
            : new LastByDescAccumulator(
                inputDataTypes.get(0),
                inputDataTypes.get(1),
                false,
                false,
                false,
                false,
                isAggTableScan);
      case FIRST_BY:
        return ascending
            ? new FirstByAccumulator(
                inputDataTypes.get(0), inputDataTypes.get(1), false, false, isAggTableScan)
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
      case APPROX_COUNT_DISTINCT:
        return new ApproxCountDistinctAccumulator(inputDataTypes.get(0));
      case APPROX_MOST_FREQUENT:
        return getApproxMostFrequentAccumulator(inputDataTypes.get(0));
      case APPROX_PERCENTILE:
        if (inputDataTypes.size() == 2) {
          return new ApproxPercentileAccumulator(inputDataTypes.get(0));
        } else {
          return new ApproxPercentileWithWeightAccumulator(inputDataTypes.get(0));
        }
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  public static GroupedAccumulator getGroupedApproxMostFrequentAccumulator(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return new BooleanGroupedApproxMostFrequentAccumulator();
      case INT32:
      case DATE:
        return new IntGroupedApproxMostFrequentAccumulator();
      case INT64:
      case TIMESTAMP:
        return new LongGroupedApproxMostFrequentAccumulator();
      case FLOAT:
        return new FloatGroupedApproxMostFrequentAccumulator();
      case DOUBLE:
        return new DoubleGroupedApproxMostFrequentAccumulator();
      case TEXT:
      case STRING:
        return new BinaryGroupedApproxMostFrequentAccumulator();
      case BLOB:
        return new BlobGroupedApproxMostFrequentAccumulator();
      case OBJECT:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", type));
    }
  }

  public static TableAccumulator getApproxMostFrequentAccumulator(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return new BooleanApproxMostFrequentAccumulator();
      case INT32:
      case DATE:
        return new IntApproxMostFrequentAccumulator();
      case INT64:
      case TIMESTAMP:
        return new LongApproxMostFrequentAccumulator();
      case FLOAT:
        return new FloatApproxMostFrequentAccumulator();
      case DOUBLE:
        return new DoubleApproxMostFrequentAccumulator();
      case TEXT:
      case STRING:
        return new BinaryApproxMostFrequentAccumulator();
      case BLOB:
        return new BlobApproxMostFrequentAccumulator();
      case OBJECT:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s", type));
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

  private static class DistinctAccumulator implements TableAccumulator {
    private final TableAccumulator accumulator;
    private MarkDistinctHash hash;

    private final List<Type> inputTypes;

    private DistinctAccumulator(TableAccumulator accumulator, List<Type> inputTypes) {
      this.accumulator = requireNonNull(accumulator, "accumulator is null");
      this.hash = new MarkDistinctHash(inputTypes, false, UpdateMemory.NOOP);
      this.inputTypes = inputTypes;
    }

    @Override
    public long getEstimatedSize() {
      return hash.getEstimatedSize() + accumulator.getEstimatedSize();
    }

    @Override
    public TableAccumulator copy() {
      throw new UnsupportedOperationException(
          "Distinct aggregation function state can not be copied");
    }

    @Override
    public void addInput(Column[] arguments, AggregationMask mask) {
      // 1. filter out positions based on mask, if present
      Column[] filtered = mask.filterBlock(arguments);

      // 2. compute a distinct mask column
      Column distinctMask = hash.markDistinctRows(filtered);

      // 3. update original mask to the new distinct mask
      mask.reset(filtered[0].getPositionCount());
      mask.applyMaskBlock(distinctMask);
      if (mask.isSelectNone()) {
        return;
      }

      // 4. feed a TsBlock with a new mask to the underlying aggregation
      accumulator.addInput(filtered, mask);
    }

    @Override
    public void addIntermediate(Column argument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateIntermediate(ColumnBuilder columnBuilder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateFinal(ColumnBuilder columnBuilder) {
      accumulator.evaluateFinal(columnBuilder);
    }

    @Override
    public boolean hasFinalResult() {
      return accumulator.hasFinalResult();
    }

    @Override
    public void addStatistics(Statistics[] statistics) {
      throw new UnsupportedOperationException("Distinct aggregation function can not be push down");
    }

    @Override
    public void reset() {
      accumulator.reset();
      hash = new MarkDistinctHash(inputTypes, false, UpdateMemory.NOOP);
    }
  }

  private static class DistinctGroupedAccumulator implements GroupedAccumulator {
    private final GroupedAccumulator accumulator;
    private MarkDistinctHash hash;

    private final List<Type> inputTypes;

    private DistinctGroupedAccumulator(GroupedAccumulator accumulator, List<Type> inputTypes) {
      this.accumulator = requireNonNull(accumulator, "accumulator is null");
      this.inputTypes =
          ImmutableList.<Type>builder()
              .add(INT32) // group id column
              .addAll(inputTypes)
              .build();
      this.hash = new MarkDistinctHash(this.inputTypes, false, UpdateMemory.NOOP);
    }

    @Override
    public long getEstimatedSize() {
      return hash.getEstimatedSize() + accumulator.getEstimatedSize();
    }

    @Override
    public void setGroupCount(long groupCount) {
      accumulator.setGroupCount(groupCount);
    }

    @Override
    public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
      // 1. filter out positions based on mask
      groupIds = maskGroupIds(groupIds, mask);
      Column[] filtered = mask.filterBlock(arguments);

      // 2. compute a distinct mask column (including the group id)
      Column distinctMask =
          hash.markDistinctRows(
              Stream.concat(
                      Stream.of(new IntColumn(groupIds.length, Optional.empty(), groupIds)),
                      Arrays.stream(filtered))
                  .toArray(Column[]::new));

      // 3. update original mask to the new distinct mask
      mask.reset(filtered[0].getPositionCount());
      mask.applyMaskBlock(distinctMask);
      if (mask.isSelectNone()) {
        return;
      }

      // 4. feed a TsBlock with a new mask to the underlying aggregation
      accumulator.addInput(groupIds, filtered, mask);
    }

    private static int[] maskGroupIds(int[] groupIds, AggregationMask mask) {
      if (mask.isSelectAll() || mask.isSelectNone()) {
        return groupIds;
      }

      int[] newGroupIds = new int[mask.getSelectedPositionCount()];
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < newGroupIds.length; i++) {
        newGroupIds[i] = groupIds[selectedPositions[i]];
      }
      return newGroupIds;
    }

    @Override
    public void addIntermediate(int[] groupIds, Column argument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
      accumulator.evaluateFinal(groupId, columnBuilder);
    }

    @Override
    public void prepareFinal() {}

    @Override
    public void reset() {
      accumulator.reset();
      hash = new MarkDistinctHash(inputTypes, false, UpdateMemory.NOOP);
    }
  }
}
