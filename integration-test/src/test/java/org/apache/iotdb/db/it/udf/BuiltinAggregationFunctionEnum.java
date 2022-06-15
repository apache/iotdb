package org.apache.iotdb.db.it.udf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum BuiltinAggregationFunctionEnum {
  MIN_TIME("min_time"),
  MAX_TIME("max_time"),
  MAX_VALUE("max_value"),
  MIN_VALUE("min_value"),
  EXTREME("extreme"),
  FIRST_VALUE("first_value"),
  LAST_VALUE("last_value"),
  COUNT("count"),
  AVG("avg"),
  SUM("sum"),
  ;

  private final String functionName;

  BuiltinAggregationFunctionEnum(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction.values())
              .map(org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }
}
