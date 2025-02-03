package org.apache.iotdb.library.dquality;

import org.apache.iotdb.library.dquality.util.TimeSeriesQuality;
import org.apache.iotdb.library.dquality.util.TimeSeriesSegQuality;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDTFTimeliness implements UDTF {
  Boolean segmentation;
  long[] timestamp;
  double[] value;

  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    boolean isTime = false;
    long window = Integer.MAX_VALUE;
    segmentation = udfp.getBooleanOrDefault("Segment", false);
    if (udfp.hasAttribute("window")) {
      String s = udfp.getString("window");
      window = Util.parseTime(s);
      if (window > 0) {
        isTime = true;
      } else {
        window = Long.parseLong(s);
      }
    }
    if (isTime) {
      udtfc.setAccessStrategy(new SlidingTimeWindowAccessStrategy(window));
    } else {
      udtfc.setAccessStrategy(new SlidingSizeWindowAccessStrategy((int) window));
    }
    udtfc.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (segmentation) {
      try {
        if (rowWindow.windowSize() > TimeSeriesSegQuality.WINDOW_SIZE) {
          TimeSeriesSegQuality tsq = new TimeSeriesSegQuality(rowWindow.getRowIterator(), 2);
          collector.putDouble(rowWindow.getRow(0).getTime(), tsq.getAnswer());
        }
      } catch (IOException | NoNumberException ex) {
        Logger.getLogger(UDTFTimeliness.class.getName()).log(Level.SEVERE, null, ex);
      }
    } else {
      try {
        if (rowWindow.windowSize() > TimeSeriesQuality.WINDOW_SIZE) {
          TimeSeriesQuality tsq = new TimeSeriesQuality(rowWindow.getRowIterator());
          tsq.timeDetect();
          collector.putDouble(rowWindow.getRow(0).getTime(), tsq.getTimeliness());
        }
      } catch (IOException | NoNumberException ex) {
        Logger.getLogger(UDTFTimeliness.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
}
