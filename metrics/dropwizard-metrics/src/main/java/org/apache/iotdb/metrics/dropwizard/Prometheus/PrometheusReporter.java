package org.apache.iotdb.metrics.dropwizard.Prometheus;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PrometheusReporter extends ScheduledReporter {

    /**
     * A builder for {@link PrometheusReporter} instances. Defaults to not using a prefix, and
     * not filtering metrics.
     */

    public static class Builder {

        private final MetricRegistry registry;
        private String prefix;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.prefix = null;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
        }

        /**
         * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
         * Default value is true.
         * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
         *
         * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
         * @return {@code this}
         */
        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        /**
         * Specifies the executor to use while scheduling reporting of metrics.
         * Default value is null.
         * Null value leads to executor will be auto created on start.
         *
         * @param executor the executor to use while scheduling reporting of metrics.
         * @return {@code this}
         */
        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link PrometheusReporter} with the given properties, sending metrics using the
         * given {@link PrometheusSender}.
         *
         * Present for binary compatibility
         *
         * @param prometheus a {@link Pushgateway}
         * @return a {@link PrometheusReporter}
         */
        public PrometheusReporter build(Pushgateway prometheus) {
            return build((PrometheusSender) prometheus);
        }

        /**
         * Builds a {@link PrometheusReporter} with the given properties, sending metrics using the
         * given {@link PrometheusSender}.
         *
         * @param prometheus a {@link PrometheusSender}
         * @return a {@link PrometheusReporter}
         */
        public PrometheusReporter build(PrometheusSender prometheus) {
            return new PrometheusReporter(registry,
                                        prometheus,
                                        prefix,
                                        filter,
                                        executor,
                                        shutdownExecutorOnStop);
        }

    }

    private static final TimeUnit DURATION_UNIT = TimeUnit.MILLISECONDS;
    private static final TimeUnit RATE_UNIT = TimeUnit.SECONDS;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

    private final PrometheusSender prometheus;
    private final String prefix;


    /**
     * Creates a new {@link PrometheusReporter} instance.
     *
     * @param registry               the {@link MetricRegistry} containing the metrics this
     *                               reporter will report
     * @param prometheus               the {@link PrometheusSender} which is responsible for sending metrics to a Carbon server
     *                               via a transport protocol
     * @param prefix                 the prefix of all metric names (may be null)
     * @param filter                 the filter for which metrics to report
     * @param executor               the executor to use while scheduling reporting of metrics (may be null).
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
     */
    protected PrometheusReporter(MetricRegistry registry, PrometheusSender prometheus, String prefix, MetricFilter filter, ScheduledExecutorService executor, boolean shutdownExecutorOnStop) {
        super(registry, "prometheus-reporter", filter, RATE_UNIT, DURATION_UNIT, executor, shutdownExecutorOnStop, Collections.<MetricAttribute>emptySet());
        this.prometheus = prometheus;
        this.prefix = prefix;
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            try {
                prometheus.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from Prometheus", prometheus, e);
            }
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        try {
            if (!prometheus.isConnected()) {
                prometheus.connect();
            }

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                prometheus.sendGauge(prefixed(entry.getKey()), entry.getValue());
            }
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                prometheus.sendCounter(prefixed(entry.getKey()), entry.getValue());
            }
            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                prometheus.sendHistogram(prefixed(entry.getKey()), entry.getValue());
            }
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                prometheus.sendMeter(prefixed(entry.getKey()), entry.getValue());
            }
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                prometheus.sendTimer(prefixed(entry.getKey()), entry.getValue());
            }

            prometheus.flush();
        } catch (IOException e) {
            LOGGER.warn("Unable to report to Prometheus", prometheus, e);
        }

    }

    private String prefixed(String name) {
        return prefix == null ? name : (prefix + name);
    }

    /**
     * Returns a new {@link Builder} for {@link PrometheusReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link PrometheusReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

}
