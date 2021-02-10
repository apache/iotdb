package org.apache.iotdb.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iotdb.metrics.impl.DoNothingFactory;

public class MetricService {

 private static List<MetricReporter> reporters = new ArrayList<>();

 private static MetricFactory factory;

 static {
  init();
 }

 private static void init() {

  ServiceLoader<MetricReporter> reporter = ServiceLoader.load(MetricReporter.class);
  for (MetricReporter r : reporter) {
   reporters.add(r);
   r.start();
  }

  ServiceLoader<MetricFactory> metricFactories = ServiceLoader.load(MetricFactory.class);
  int size = 0;
  MetricFactory nothingFactory = null;

  for (MetricFactory mf : metricFactories) {
   if (mf instanceof DoNothingFactory) {
    nothingFactory = mf;
    continue;
   }
    size ++;
    if (size > 1) {
     throw new RuntimeException("More than one Metric Implementation is detected.");
    }
   factory = mf;
  }

  // if no more implementation, we use nothingFactory.
  if (size == 0) {
   factory = nothingFactory;
  }
 }

 public static void stop() {
  for (MetricReporter r : reporters) {
   r.stop();
  }
 }

 public static MetricManager getMetric(String namespace) {
  return factory.getMetric(namespace);
 }
 public static void enableKnownMetric(KnownMetric metric) {
  factory.enableKnownMetric(metric);
 }
 public static Map<String, MetricManager> getAllMetrics() {
  return factory.getAllMetrics();
 }
 public static boolean isEnable() {
  return factory.isEnable();
 }
}
