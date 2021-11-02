package org.apache.iotdb.metrics.dropwizard.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropwizardJmxReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardJmxReporter.class);

  private MetricManager dropwizardMetricManager = null;
  private JmxReporter jmxReporter = null;

  @Override
  public boolean start() {
    if (jmxReporter != null) {
      LOGGER.warn("Dropwizard JmxReporter already start!");
      return false;
    }
    try {
      jmxReporter =
          JmxReporter.forRegistry(
                  ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
              .build();
    } catch (Exception e) {
      LOGGER.error("Failed to start Dropwizard JmxReporter, because {}", e.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public boolean stop() {
    if (jmxReporter == null) {
      LOGGER.warn("Dropwizard JmxReporter already stop!");
      return false;
    }
    jmxReporter.stop();
    jmxReporter = null;
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.jmx;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }
}
