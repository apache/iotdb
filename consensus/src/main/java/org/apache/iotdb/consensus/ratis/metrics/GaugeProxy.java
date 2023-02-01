package org.apache.iotdb.consensus.ratis.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/** AutoGauge supplier holder class */
public class GaugeProxy implements Gauge {

  private final Gauge gauge;

  public GaugeProxy(MetricRegistry.MetricSupplier<Gauge> metricSupplier) {
    this.gauge = metricSupplier.newMetric();
  }

  @Override
  public Object getValue() {
    return gauge.getValue();
  }

  Long getValueAsLong() {
    Object value = getValue();
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return -1L;
  }
}
