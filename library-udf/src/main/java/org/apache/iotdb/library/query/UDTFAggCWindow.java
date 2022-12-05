package org.apache.iotdb.library.query;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class UDTFAggCWindow implements UDTF {
  private String func;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    func = parameters.getString("func");
    int window = parameters.getInt("window");
    int slide = parameters.getInt("skip");
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(window, slide))
        .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() == 0) { // skip null rows
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
