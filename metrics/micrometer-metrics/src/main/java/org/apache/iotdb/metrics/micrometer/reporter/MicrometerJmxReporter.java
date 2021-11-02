package org.apache.iotdb.metrics.micrometer.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerJmxReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerJmxReporter.class);
  private MetricManager metricManager;

  @Override
  public boolean start() {
    try {
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(reporter -> reporter instanceof JmxMeterRegistry)
              .collect(Collectors.toSet());
      for (MeterRegistry meterRegistry : meterRegistrySet) {
        ((JmxMeterRegistry) meterRegistry).start();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to start Micrometer JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public boolean stop() {
    try {
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(reporter -> reporter instanceof JmxMeterRegistry)
              .collect(Collectors.toSet());
      for (MeterRegistry meterRegistry : meterRegistrySet) {
        ((JmxMeterRegistry) meterRegistry).stop();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to stop Micrometer JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.jmx;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
