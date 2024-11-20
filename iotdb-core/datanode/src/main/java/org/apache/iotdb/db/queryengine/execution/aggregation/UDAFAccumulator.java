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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDFParametersFactory;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class UDAFAccumulator implements Accumulator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UDAFAccumulator.class);

  private final String functionName;
  private final UDAFConfigurations configurations;

  private State state;

  private UDAF udaf;

  public UDAFAccumulator(
      String functionName,
      List<Expression> childrenExpressions,
      TSDataType childrenExpressionDataTypes,
      Map<String, String> attributes,
      boolean isInputRaw) {
    this.functionName = functionName;
    this.configurations = new UDAFConfigurations();

    List<String> childExpressionStrings =
        childrenExpressions.stream()
            .map(Expression::getExpressionString)
            .collect(Collectors.toList());
    beforeStart(
        childExpressionStrings,
        Collections.singletonList(childrenExpressionDataTypes),
        attributes,
        isInputRaw);
  }

  private void beforeStart(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes,
      boolean isInputRaw) {
    reflectAndValidateUDF(childExpressions, childExpressionDataTypes, attributes, isInputRaw);
    configurations.check();
  }

  private void reflectAndValidateUDF(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes,
      boolean isInputRaw) {
    udaf = UDFManagementService.getInstance().reflect(functionName, UDAF.class);
    state = udaf.createState();

    final UDFParameters parameters =
        UDFParametersFactory.buildUdfParameters(
            childExpressions, childExpressionDataTypes, attributes);

    // Only validate for raw input
    // There is no need to validate for partial input
    if (isInputRaw) {
      try {
        // Double validates in workers
        udaf.validate(new UDFParameterValidator(parameters));
      } catch (Exception e) {
        onError("validate(UDFParameterValidator)", e);
      }
    }

    try {
      udaf.beforeStart(parameters, configurations);
    } catch (Exception e) {
      onError("beforeStart(UDFParameters, UDAFConfigurations)", e);
    }
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    // To be consistent with UDTF
    // Left rotate the first time column to the end of input columns
    Column timeColumn = columns[0];
    for (int i = 0; i < columns.length - 1; i++) {
      columns[i] = columns[i + 1];
    }
    columns[columns.length - 1] = timeColumn;

    udaf.addInput(state, columns, bitMap);
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of UDAF should be 1");

    State otherState = udaf.createState();
    Binary otherStateBinary = partialResult[0].getBinary(0);
    otherState.deserialize(otherStateBinary.getValues());

    udaf.combineState(state, otherState);
  }

  // Currently, UDAF does not support optimization
  // by statistics in TsFile
  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  // Currently, query engine won't generate
  // Final -> Final AggregationStep for UDAF
  @Override
  public void setFinal(Column finalResult) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of UDAF should be 1");

    byte[] bytes = state.serialize();
    columnBuilders[0].writeBinary(new Binary(bytes));
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    ResultValue resultValue = new ResultValue(columnBuilder);
    udaf.outputFinal(state, resultValue);
  }

  @Override
  public void removeIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of UDAF should be 1");

    State removedState = udaf.createState();
    Binary removedStateBinary = partialResult[0].getBinary(0);
    removedState.deserialize(removedStateBinary.getValues());

    udaf.removeState(state, removedState);
  }

  @Override
  public void reset() {
    state.reset();
  }

  // Like some built-in aggregations(`sum`, `count` and `avg`)
  // UDAF always return false for this method
  @Override
  public boolean hasFinalResult() {
    return false;
  }

  // We serialize state into binary form
  // So the intermediate data type for UDAF must be TEXT
  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return UDFDataTypeTransformer.transformToTsDataType(configurations.getOutputDataType());
  }

  private void onError(String methodName, Exception e) {
    LOGGER.warn(
        "Error occurred during executing UDAF, please check whether the implementation of UDF is correct according to the udf-api description.",
        e);
    throw new RuntimeException(
        String.format(
                "Error occurred during executing UDAF#%s: %s, please check whether the implementation of UDF is correct according to the udf-api description.",
                methodName, System.lineSeparator())
            + e);
  }

  public UDAFConfigurations getConfigurations() {
    return configurations;
  }
}
