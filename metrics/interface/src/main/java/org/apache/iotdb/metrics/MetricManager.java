package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface MetricManager {

  Counter counter(String metric, String... tags);
  Gauge gauge(String metric, String... tags);
  Histogram histogram(String metric, String... tags);
  Rate rate(String metric, String... tags);
  Timer timer(String metric, String... tags);

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

  Map<String, String[]> getAllMetricKeys();

  // key is name + tags
  Map<String[], Counter> getAllCounters();
  Map<String[], Gauge> getAllGauges();
  Map<String[], Rate> getAllMeters();
  Map<String[], Histogram> getAllHistograms();
  Map<String[], Timer> getAllTimers();
}
