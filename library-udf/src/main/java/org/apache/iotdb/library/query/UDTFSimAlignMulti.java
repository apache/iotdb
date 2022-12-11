package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** Joining >=3 series with the tolerance on matching timestamps, defined by eps. */
public class UDTFSimAlignMulti implements UDTF {
  double eps;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    eps = parameters.getDoubleOrDefault("eps", 1);
    configurations
        .setAccessStrategy(
            new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE, Integer.MAX_VALUE))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    for (int i = 0; i < rowWindow.windowSize(); i++) {
      Row curr = rowWindow.getRow(i);
      long signal1 = -1, signal2 = -1;
      for (int j = Math.max(i - 5, 0); j < Math.min(i + 5, rowWindow.windowSize()); j++) {
        Row comp1 = rowWindow.getRow(j);
        if (Math.abs(curr.getTime() - comp1.getTime()) > eps) continue;
        for (int k = Math.max(j - 5, 0); k < Math.min(j + 5, rowWindow.windowSize()); k++) {
          Row comp2 = rowWindow.getRow(k);
          if (Math.abs(curr.getTime() - comp2.getTime()) <= eps
              && Math.abs(comp1.getTime() - comp2.getTime()) <= eps) {
            signal1 = comp1.getTime();
            signal2 = comp2.getTime();
            break;
          }
        }
        if (signal1 != -1) break;
      }
      collector.putDouble(curr.getTime(), signal2);
    }
  }
}
