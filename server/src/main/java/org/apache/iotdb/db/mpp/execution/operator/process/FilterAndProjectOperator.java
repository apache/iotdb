/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.ConstantColumn;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterAndProjectOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterAndProjectOperator.class);

  private final Expression predicate;

  private final Expression[] outputExpressions;
  private final Operator inputOperator;

  private final boolean hasNonMappableUDF;

  private final boolean isAscending;

  private final OperatorContext operatorContext;

  private final Map<String, List<InputLocation>> predicateInputLocations;

  private UDTFContext predicateUDTFContext;

  private UDTFContext outputUDTFContext;

  private ColumnTransformer[] outputColumnTransformers;

  private ColumnTransformer predicateColumnTransformer;

  // key: Subexpressions of predicate; Value: Related ColumnTransformer
  private Map<Expression, ColumnTransformer> predicateMap = new HashMap<>();

  // key: Subexpressions of outputExpressions; Value: Related ColumnTransformer
  private Map<Expression, ColumnTransformer> outputMap;

  //
  private Map<Expression, Integer> commonSubexpressionsIndexMap;

  private Set<Expression> commonSubexpressions;

  // record the datatype of the value column in the output TsBlock of filter
  private List<TSDataType> predicateOutputDataTypes;

  private List<TSDataType> outputDataTypes;

  public FilterAndProjectOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Expression predicate,
      Expression[] outputExpressions,
      Set<Expression> commonSubexpressions,
      TypeProvider typeProvider,
      Map<String, List<InputLocation>> predicateInputLocations,
      ZoneId zoneId,
      boolean hasNonMappableUDF,
      boolean isAscending)
      throws QueryProcessException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.predicateOutputDataTypes = inputDataTypes;
    this.predicate = predicate;
    this.outputExpressions = outputExpressions;
    this.commonSubexpressions = commonSubexpressions;
    this.predicateInputLocations = predicateInputLocations;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.isAscending = isAscending;

    initFilter(zoneId, typeProvider);

    // init ColumnTransformer of outputExpressions if non-mappable UDF does not exist
    if (!hasNonMappableUDF) {
      initTransformer(zoneId, typeProvider);
    }
  }

  private void initFilter(ZoneId zoneId, TypeProvider typeProvider) throws QueryProcessException {
    this.predicateUDTFContext = new UDTFContext(zoneId);
    predicateUDTFContext.constructUdfExecutors(new Expression[] {predicate});
    // init ColumnTransformer of predicate
    predicateColumnTransformer =
        predicate.constructColumnTransformer(
            operatorContext.getOperatorId(),
            predicateUDTFContext,
            predicateMap,
            typeProvider,
            new HashSet<>());
    addReferenceCountOfCommonExpressions();

    // add datatype of common subexpressions in output datatypes
    // record the map of expression->columnIndex at the same time
    commonSubexpressionsIndexMap = new HashMap<>();
    for (Expression expression : commonSubexpressions) {
      commonSubexpressionsIndexMap.put(expression, predicateOutputDataTypes.size());
      predicateOutputDataTypes.add(typeProvider.getType(expression.getExpressionString()));
    }
  }

  private void initTransformer(ZoneId zoneId, TypeProvider typeProvider) {
    this.outputUDTFContext = new UDTFContext(zoneId);
    outputUDTFContext.constructUdfExecutors(outputExpressions);
    outputColumnTransformers = new ColumnTransformer[outputExpressions.length];
    outputMap = new HashMap<>();
    for (int i = 0; i < outputExpressions.length; i++) {
      outputColumnTransformers[i] =
          outputExpressions[i].constructColumnTransformer(
              operatorContext.getOperatorId(),
              outputUDTFContext,
              outputMap,
              typeProvider,
              commonSubexpressions);
    }
    // init output datatypes
    outputDataTypes =
        Arrays.stream(outputExpressions)
            .map(expression -> typeProvider.getType(expression.getExpressionString()))
            .collect(Collectors.toList());
  }

  // result of commonExpressions should be kept
  private void addReferenceCountOfCommonExpressions() {
    for (Expression expression : commonSubexpressions) {
      if (predicateMap.containsKey(expression)) {
        predicateMap.get(expression).addReferenceCount();
      }
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    // reset ColumnTransformer for next input
    predicateColumnTransformer.reset();

    if (!hasNonMappableUDF) {
      for (ColumnTransformer columnTransformer : outputColumnTransformers) {
        columnTransformer.reset();
      }
    }

    TsBlock input = inputOperator.next();
    TsBlock filterResult = getFilterTsBlock(input);

    // contains non-mappable udf, we leave calculation for TransformOperator
    if (hasNonMappableUDF) {
      return filterResult;
    }
    return getTransformedTsBlock(filterResult);
  }

  /**
   * Return the TsBlock that contains both initial input columns and columns of common
   * subexpressions after filtering
   *
   * @param input
   * @return
   */
  private TsBlock getFilterTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    // feed Predicate ColumnTransformer, including TimeStampColumnTransformer and constant
    for (Expression expression : predicateMap.keySet()) {
      if (predicateInputLocations.containsKey(expression.getExpressionString())) {
        predicateMap
            .get(expression)
            .initializeColumnCache(
                input.getColumn(
                    predicateInputLocations
                        .get(expression.getExpressionString())
                        .get(0)
                        .getValueColumnIndex()));
      }
      if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        predicateMap.get(expression).initializeColumnCache(originTimeColumn);
      }
      if (expression.getExpressionType().equals(ExpressionType.CONSTANT)) {
        predicateMap
            .get(expression)
            .initializeColumnCache(new ConstantColumn((ConstantOperand) expression));
      }
    }

    predicateColumnTransformer.tryEvaluate();

    Column filterColumn = predicateColumnTransformer.getColumn();

    final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
    tsBlockBuilder.buildValueColumnBuilders(predicateOutputDataTypes);
    final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    List<Column> resultColumns = new ArrayList<>();
    for (int i = 0, n = input.getValueColumnCount(); i < n; i++) {
      resultColumns.add(input.getColumn(i));
    }

    // todo: remove this if, add calculated common sub expressions anyway
    if (!hasNonMappableUDF) {
      // get result of calculated common sub expressions
      for (Expression expression : commonSubexpressions) {
        resultColumns.add(predicateMap.get(expression).getColumn());
      }
    }

    // construct result TsBlock of filter
    int rowCount = 0;
    for (int i = 0, n = filterColumn.getPositionCount(), m = resultColumns.size(); i < n; i++) {
      if (filterColumn.getBoolean(i)) {
        rowCount++;
        timeBuilder.writeLong(originTimeColumn.getLong(i));
        for (int j = 0; j < m; j++) {
          if (resultColumns.get(j).isNull(i)) {
            columnBuilders[j].appendNull();
          } else {
            columnBuilders[j].write(resultColumns.get(j), i);
          }
        }
      }
    }

    tsBlockBuilder.declarePositions(rowCount);
    return tsBlockBuilder.build();
  }

  private TsBlock getTransformedTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    // feed pre calculated data
    for (Expression expression : outputMap.keySet()) {
      if (predicateInputLocations.containsKey(expression.getExpressionString())) {
        outputMap
            .get(expression)
            .initializeColumnCache(
                input.getColumn(
                    predicateInputLocations
                        .get(expression.getExpressionString())
                        .get(0)
                        .getValueColumnIndex()));
      }
      if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        outputMap.get(expression).initializeColumnCache(originTimeColumn);
      }
      if (expression.getExpressionType().equals(ExpressionType.CONSTANT)) {
        outputMap
            .get(expression)
            .initializeColumnCache(new ConstantColumn((ConstantOperand) expression));
      }
      if (commonSubexpressions.contains(expression)) {
        outputMap
            .get(expression)
            .initializeColumnCache(input.getColumn(commonSubexpressionsIndexMap.get(expression)));
      }
    }

    List<Column> resultColumns = new ArrayList<>();
    for (ColumnTransformer columnTransformer : outputColumnTransformers) {
      columnTransformer.tryEvaluate();
      resultColumns.add(columnTransformer.getColumn());
    }

    final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
    tsBlockBuilder.buildValueColumnBuilders(outputDataTypes);
    final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    // construct result TsBlock
    int positionCount = input.getPositionCount();
    for (int i = 0; i < positionCount; i++) {
      timeBuilder.writeLong(originTimeColumn.getLong(i));
      for (int j = 0, m = resultColumns.size(); j < m; j++) {
        if (resultColumns.get(j).isNull(i)) {
          columnBuilders[j].appendNull();
        } else {
          columnBuilders[j].write(resultColumns.get(j), i);
        }
      }
    }
    tsBlockBuilder.declarePositions(positionCount);
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return inputOperator.hasNext();
  }

  @Override
  public boolean isFinished() {
    return inputOperator.isFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }
}
