package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.NthValueFunction;

import java.util.List;

public class WindowFunctionFactory {
  public static WindowFunction createBuiltinWindowFunction(
      String functionName, List<Integer> argumentChannels, boolean ignoreNulls) {
    if (functionName.equals("nth_value")) {
      return new NthValueFunction(argumentChannels, );
    }
  }
}
