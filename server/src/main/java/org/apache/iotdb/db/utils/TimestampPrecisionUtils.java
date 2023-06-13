package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.util.concurrent.TimeUnit;

public class TimestampPrecisionUtils {
  private static final String timestampPrecision =
      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();

  @FunctionalInterface
  private interface ConvertFunction<T1, T2, R> {
    public R apply(T1 t1, T2 t2);
  }

  private static final ConvertFunction<Long, TimeUnit, Long> convertFunction;

  static {
    switch (timestampPrecision) {
      case "ms":
        convertFunction = TimeUnit.MILLISECONDS::convert;
        break;
      case "ns":
        convertFunction = TimeUnit.NANOSECONDS::convert;
        break;
      case "us":
        convertFunction = TimeUnit.MICROSECONDS::convert;
        break;
        // this case will never reach
      default:
        convertFunction = null;
    }
  }

  /** convert specific precision timestamp to current precision timestamp */
  public static long convertToCurrPrecision(long sourceTime, TimeUnit sourceUnit) {
    return convertFunction.apply(sourceTime, sourceUnit);
  }
}
