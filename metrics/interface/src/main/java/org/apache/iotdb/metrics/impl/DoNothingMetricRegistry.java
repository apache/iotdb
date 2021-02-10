package org.apache.iotdb.metrics.impl;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.metrics.MetricRegistry;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Meter;
import org.apache.iotdb.metrics.type.Metric;
import org.apache.iotdb.metrics.type.Timer;

public class DoNothingMetricRegistry implements MetricRegistry {

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
  public Map<String, String[]> getAllMetricKeys() {
    return null;
  }

  @Override
  public Metric getMetricValue(String metric, String... tags) {
    return null;
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    return null;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    return null;
  }

  @Override
  public Map<String[], Meter> getAllMeters() {
    return null;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    return null;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    return null;
  }


}
