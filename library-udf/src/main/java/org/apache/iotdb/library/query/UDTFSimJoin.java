package org.apache.iotdb.library.query;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** Joining 2 series with the tolerance on matching timestamps, defined by eps. */
public class UDTFSimJoin implements UDTF {
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
      long signal = -1;
      for (int j = Math.max(i - 5, 0); j < Math.min(i + 5, rowWindow.windowSize()); j++) {
        Row comp = rowWindow.getRow(j);
        if (Math.abs(curr.getTime() - comp.getTime()) <= eps) {
          signal = comp.getTime();
        }
      }
      collector.putDouble(curr.getTime(), signal);
    }
  }
}
