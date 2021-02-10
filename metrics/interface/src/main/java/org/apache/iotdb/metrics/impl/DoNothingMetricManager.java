package org.apache.iotdb.metrics.impl;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;

public class DoNothingMetricManager implements MetricManager {

  @Override
  public Counter counter(String metric, String... tags) {
    return null;
  }

  @Override
  public Gauge gauge(String metric, String... tags) {
    return null;
  }

  @Override
  public Histogram histogram(String metric, String... tags) {
    return null;
  }

  @Override
  public Rate rate(String metric, String... tags) {
    return null;
  }

  @Override
  public Timer timer(String metric, String... tags) {
    return null;
  }

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
  public Map<String[], Counter> getAllCounters() {
    return null;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    return null;
  }

  @Override
  public Map<String[], Rate> getAllMeters() {
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
