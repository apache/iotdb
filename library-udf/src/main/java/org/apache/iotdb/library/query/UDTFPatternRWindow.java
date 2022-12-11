package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Find distance between matching patterns and a continuous subsequence of time-series, the subseq
 * and the pattern share the same size of time intervals.
 */
public class UDTFPatternRWindow implements UDTF {
  private List<Double> pattern = new ArrayList<>();

  public void init(int len) {
    Random rd = new Random();
    for (int i = 0; i < len; i++) pattern.add(rd.nextDouble());
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    // int psz = parameters.getIntOrDefault("len", 5);
    String[] pat = parameters.getString("pattern").split(":");
    pattern =
        Arrays.stream(pat)
            .map(
                new Function<String, Double>() {
                  @Override
                  public Double apply(String s) {
                    return Double.parseDouble(s);
                  }
                })
            .collect(Collectors.toList());
    long wsz = parameters.getLong("window");
    long skip = parameters.getLong("skip");
    configurations
        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(wsz, skip))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() == 0) { // skip null rows
      return;
    }
    int avgPts = (int) rowWindow.windowSize() / pattern.size();
    collector.putDouble(rowWindow.windowStartTime(), distance(rowWindow, avgPts));
  }

  private double distance(RowWindow rowWindow, int avgPts) throws IOException {
    double ans = 0.;
    List<Double> tmp = new ArrayList<>();
    for (int i = 0; i < pattern.size(); i++) {
      double s = 0.;
      int cnt = 0;
      for (int j = 0; j < avgPts && j + i * avgPts < rowWindow.windowSize(); j++) {
        s += rowWindow.getRow(j + i * avgPts).getDouble(0);
        cnt += 1;
      }
      s /= cnt;
      tmp.add(s);
    }
    for (int i = 0; i < pattern.size() && i < tmp.size(); i++) {
      double diff = (pattern.get(i) - tmp.get(i));
      ans += diff * diff;
    }
    return Math.sqrt(ans);
  }
}
