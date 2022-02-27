package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.enums.MetricLevel;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;

public class MetricName {
  private Meter.Id id;
  private MetricLevel metricLevel;

  public MetricName(String name, Meter.Type type, String... tags) {
    this.id = new Meter.Id(name, Tags.of(tags), null, null, type);
  }

  public MetricName(String name, Meter.Type type, MetricLevel metricLevel, String... tags) {
    this(name, type, tags);
    this.metricLevel = metricLevel;
  }

  public Meter.Id getId() {
    return id;
  }

  public MetricLevel getMetricLevel() {
    return metricLevel;
  }

  @Override
  public String toString() {
    return id.toString();
  }

  @Override
  public boolean equals(Object obj) {
    // do not compare metricLevel
    if (!(obj instanceof MetricName)) {
      return false;
    }
    return id.equals(((MetricName) obj).getId());
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
