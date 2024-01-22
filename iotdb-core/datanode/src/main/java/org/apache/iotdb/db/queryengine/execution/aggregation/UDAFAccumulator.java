package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
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
      Map<String, String> attributes) {
    this.functionName = functionName;
    this.configurations = new UDAFConfigurations(ZoneId.systemDefault());

    List<String> childExpressionStrings =
        childrenExpressions.stream()
            .map(Expression::getExpressionString)
            .collect(Collectors.toList());
    beforeStart(
        childExpressionStrings, Collections.singletonList(childrenExpressionDataTypes), attributes);
  }

  private void beforeStart(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    reflectAndValidateUDF(childExpressions, childExpressionDataTypes, attributes);
    configurations.check();
  }

  private void reflectAndValidateUDF(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    udaf = (UDAF) UDFManagementService.getInstance().reflect(functionName);
    state = udaf.createState();

    final UDFParameters parameters =
        new UDFParameters(
            childExpressions,
            UDFDataTypeTransformer.transformToUDFDataTypeList(childExpressionDataTypes),
            attributes);

    try {
      // Double validates in workers
      udaf.validate(new UDFParameterValidator(parameters));
    } catch (Exception e) {
      onError("validate(UDFParameterValidator)", e);
    }

    try {
      udaf.beforeStart(parameters, configurations);
    } catch (Exception e) {
      onError("beforeStart(UDFParameters, UDAFConfigurations)", e);
    }
  }

  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    udaf.addInput(state, columns, bitMap);
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of UDAF should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }

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
