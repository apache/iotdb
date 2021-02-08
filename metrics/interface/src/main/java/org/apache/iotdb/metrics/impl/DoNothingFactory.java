package org.apache.iotdb.metrics.impl;

import java.util.Collections;
import java.util.Map;
import org.apache.iotdb.metrics.MetricFactory;
import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.Metric;

public class DoNothingFactory implements MetricFactory {
  private DoNothingMetric metric = new DoNothingMetric();
  @Override
  public Metric getMetric(String namespace) {
    return metric;
  }

  @Override
  public void enableKnownMetric(KnownMetric metric) {

  }

  @Override
  public Map<String, Metric> getAllMetrics() {
    return Collections.emptyMap();
  }

  @Override
  public boolean isEnable() {
    return true;
  }
}
