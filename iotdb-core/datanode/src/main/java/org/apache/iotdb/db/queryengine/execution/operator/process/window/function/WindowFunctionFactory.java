package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.CumeDistFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.DenseRankFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.NTileFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.PercentRankFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.RankFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank.RowNumberFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.FirstValueFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.LagFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.LastValueFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.LeadFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.NthValueFunction;

import java.util.List;

public class WindowFunctionFactory {
  public static WindowFunction createBuiltinWindowFunction(
      String functionName, List<Integer> argumentChannels, boolean ignoreNulls) {
    switch (functionName) {
      case "nth_value":
        return new NthValueFunction(argumentChannels, ignoreNulls);
      case "first_value":
        return new FirstValueFunction(argumentChannels.get(0), ignoreNulls);
      case "last_value":
        return new LastValueFunction(argumentChannels.get(0), ignoreNulls);
      case "lead":
        return new LeadFunction(argumentChannels, ignoreNulls);
      case "lag":
        return new LagFunction(argumentChannels, ignoreNulls);
      case "rank":
        return new RankFunction();
      case "dense_rank":
        return new DenseRankFunction();
      case "row_number":
        return new RowNumberFunction();
      case "percent_rank":
        return new PercentRankFunction();
      case "cume_dist":
        return new CumeDistFunction();
      case "ntile":
        return new NTileFunction(argumentChannels.get(0));
      default:
        throw new UnsupportedOperationException(
            "Unsupported built-in window function name: " + functionName);
    }
  }
}
