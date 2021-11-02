package org.apache.iotdb.metrics.micrometer.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.Set;
import java.util.stream.Collectors;

public class MicrometerPrometheusReporter implements Reporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerPrometheusReporter.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  private MetricManager metricManager;
  private Thread runThread;

  @Override
  public boolean start() {
    Set<MeterRegistry> meterRegistrySet =
        Metrics.globalRegistry.getRegistries().stream()
            .filter(reporter -> reporter instanceof PrometheusMeterRegistry)
            .collect(Collectors.toSet());
    if (meterRegistrySet.size() != 1) {
      LOGGER.warn("Too many prometheusReporters");
    }
    PrometheusMeterRegistry prometheusMeterRegistry =
        (PrometheusMeterRegistry) meterRegistrySet.toArray()[0];
    DisposableServer server =
        HttpServer.create()
            .port(
                Integer.parseInt(
                    metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort()))
            .route(
                routes ->
                    routes.get(
                        "/prometheus",
                        (request, response) ->
                            response.sendString(Mono.just(prometheusMeterRegistry.scrape()))))
            .bindNow();

    runThread = new Thread(server::onDispose);
    runThread.start();
    return true;
  }

  @Override
  public boolean stop() {
    try {
      // stop prometheus reporter
      if (runThread != null) {
        runThread.join();
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Failed to stop micrometer prometheus reporter", e);
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return null;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.metricManager = metricManager;
  }
}
