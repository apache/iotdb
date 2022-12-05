package org.apache.iotdb.library.query;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UDTFPatternCWindow implements UDTF {
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
    // init(psz);
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(pat.length, 1))
        .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() == 0) { // skip null rows
      return;
    }
    collector.putDouble(rowWindow.windowStartTime(), distance(rowWindow));
  }

  private double distance(RowWindow rowWindow) throws IOException {
    double ans = 0.;
    for (int i = 0; i < pattern.size(); i++) {
      double diff = (pattern.get(i) - rowWindow.getRow(i).getDouble(0));
      ans += diff * diff;
    }
    return Math.sqrt(ans);
  }
}
