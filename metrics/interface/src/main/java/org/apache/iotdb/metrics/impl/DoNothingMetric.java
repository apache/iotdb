package org.apache.iotdb.metrics.impl;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.metrics.Metric;

public class DoNothingMetric implements Metric {

  @Override
  public void count(int delta, String metric, String... tags) {

  }

  @Override
  public void count(long delta, String metric, String... tags) {

  }

  @Override
  public void histogram(int value, String metric, String... tags) {

  }

  @Override
  public void histogram(long value, String metric, String... tags) {

  }

  @Override
  public void gauge(int value, String metric, String... tags) {

  }

  @Override
  public void gauge(long value, String metric, String... tags) {

  }

  @Override
  public void meter(int value, String metric, String... tags) {

  }

  @Override
  public void meter(long value, String metric, String... tags) {

  }

  @Override
  public void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {

  }

  @Override
  public void timerStart(String metric, String... tags) {

  }

  @Override
  public void timerEnd(String metric, String... tags) {

  }

  @Override
  public Map<String, String[]> getAllMetrics() {
    return Collections.emptyMap();
  }

  @Override
  public Object getMetricValue(String metric, String... tags) {
    return 0;
  }
}
