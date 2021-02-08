package org.apache.iotdb.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Metric {
  //metric.counter(5, "insertRecords","interface","insertRecords","sg","sg1");
  void count(int delta, String metric, String... tags);
  void count(long delta, String metric, String... tags);
  void histogram(int value, String metric, String... tags);
  void histogram(long value, String metric, String... tags);
  void gauge(int value, String metric, String... tags);
  void gauge(long value, String metric, String... tags);
  void meter(int value, String metric, String... tags);
  void meter(long value, String metric, String... tags);
  void timer(long delta, TimeUnit timeUnit, String metric, String... tags);
  void timerStart(String metric, String... tags);
  void timerEnd(String metric, String... tags);


  Map<String, String[]> getAllMetrics();
  Object getMetricValue (String metric, String... tags);
  Object getMetricHistogram (String metric, String... tags);
  Object getMetricTimer (String metric, String... tags);

}
