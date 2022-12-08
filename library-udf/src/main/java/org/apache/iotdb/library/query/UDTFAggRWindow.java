package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class UDTFAggRWindow implements UDTF {
  private String func;
  long st, ed;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    func = parameters.getString("func");
    long window = parameters.getLong("window");
    long slide = parameters.getLong("skip");
    st = parameters.getLong("start");
    ed = parameters.getLong("end");
    configurations
        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(window, slide))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() == 0
        || rowWindow.windowStartTime() > ed
        || rowWindow.windowEndTime() < st) { // skip null rows
      return;
    }
    double res;
    switch (func) {
      case "avg":
        res = findAvg(rowWindow);
        break;
      case "spe":
        res = findSpecial(rowWindow);
        break;
      default:
        res = findMax(rowWindow);
    }
    collector.putDouble(rowWindow.windowStartTime(), res);
  }

  public double findMax(RowWindow rowWindow) throws IOException {
    double ans = Double.MIN_VALUE;
    for (int i = 0; i < rowWindow.windowSize(); i++) {
      ans = Double.max(ans, rowWindow.getRow(i).getDouble(0));
    }
    return ans;
  }

  public double findAvg(RowWindow rowWindow) throws IOException {
    double ans = 0.;
    for (int i = 0; i < rowWindow.windowSize(); i++) {
      ans += rowWindow.getRow(i).getDouble(0);
    }
    return ans / rowWindow.windowSize();
  }

  public double findSpecial(RowWindow rowWindow) throws IOException {
    double ans = 0.;
    for (int i = 0; i < rowWindow.windowSize(); i++) {
      ans += rowWindow.getRow(i).getDouble(0) * rowWindow.getRow(i).getDouble(0);
    }
    return ans;
  }
}
