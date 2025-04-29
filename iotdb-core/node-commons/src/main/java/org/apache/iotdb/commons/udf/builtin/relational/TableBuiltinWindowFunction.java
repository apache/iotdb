package org.apache.iotdb.commons.udf.builtin.relational;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableBuiltinWindowFunction {
  RANK("rank"),
  DENSE_RANK("dense_rank"),
  ROW_NUMBER("row_number"),
  PERCENT_RANK("percent_rank"),
  CUME_DIST("cume_dist"),
  NTILE("ntile"),
  FIRST_VALUE("first_value"),
  LAST_VALUE("last_value"),
  NTH_VALUE("nth_value"),
  LEAD("lead"),
  LAG("lag"),
  ;

  private final String functionName;

  TableBuiltinWindowFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> BUILT_IN_WINDOW_FUNCTION_NAME =
      new HashSet<>(
          Arrays.stream(TableBuiltinWindowFunction.values())
              .map(TableBuiltinWindowFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getBuiltInWindowFunctionName() {
    return BUILT_IN_WINDOW_FUNCTION_NAME;
  }
}
