/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metrics2;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionTimer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultLongTaskTimer;
import io.micrometer.core.instrument.internal.DefaultMeter;
import io.micrometer.core.instrument.push.PushMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * Registry which stores all values as IoTDB Time Series.
 * All time-related values should be milliseconds.
 *
 * With default settings, each second all values are written.
 */
public class IoTDBRegistry extends PushMeterRegistry {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBRegistry.class);

    private final PlanExecutor executor;
    private final Planner planner;

    public IoTDBRegistry(IoTDBRegistryConfig config, Clock clock) {
        super(config, clock);

        planner = new Planner();
        try {
            executor = new PlanExecutor();
        } catch (QueryProcessException e) {
            throw new IllegalStateException("Unable to instantiate IoTDB Metric Backend", e);
        }

        // Prepare a metric for here...
        start(new NamedThreadFactory("iotdb-metrics-publisher"));
    }

    @Override
    protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> valueFunction) {
        return new DefaultGauge<>(id, obj, valueFunction);
    }

    @Override
    protected Counter newCounter(Meter.Id id) {
        return new CumulativeCounter(id);
    }

    @Override
    protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
        return new DefaultLongTaskTimer(id, clock);
    }

    @Override
    protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
        return new CumulativeTimer(id, clock, distributionStatisticConfig, pauseDetector, TimeUnit.MILLISECONDS);
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, double scale) {
        return new CumulativeDistributionSummary(id, clock, distributionStatisticConfig, scale, true);
    }

    @Override
    protected Meter newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> measurements) {
        return new DefaultMeter(id, type, measurements);
    }

    @Override
    protected <T> FunctionTimer newFunctionTimer(Meter.Id id, T obj, ToLongFunction<T> countFunction, ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
        return new CumulativeFunctionTimer<T>(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit, TimeUnit.MILLISECONDS);
    }

    @Override
    protected <T> FunctionCounter newFunctionCounter(Meter.Id id, T obj, ToDoubleFunction<T> countFunction) {
        return new CumulativeFunctionCounter<T>(id, obj, countFunction);
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    protected DistributionStatisticConfig defaultHistogramConfig() {
        return DistributionStatisticConfig.DEFAULT;
    }


    @Override
    protected void publish() {
        if (!IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
            Metrics.timer("iotdb.metrics.write.timer").record(this::writeMetrics);
        }
    }

    private void writeMetrics() {
        for (Meter meter : getMeters()) {

            // Add this to an IoTDB Timeseries now
            final String conventionName = meter.getId().getConventionName(NamingConvention.dot);
            final List<Tag> conventionTags = meter.getId().getConventionTags(NamingConvention.dot);

            // Now we add this as a timeseries
            final String query = meter.match(
                g -> createQuery(conventionName, conventionTags, g.value()),
                c -> createQuery(conventionName, conventionTags, c.count()),
                t -> createQueryForTimer(conventionName, conventionTags, t),
                a -> {throw new NotImplementedException("");},
                ltt -> createQueryForLTT(conventionName, conventionTags, ltt),
                tg -> createQuery(conventionName, conventionTags, tg.value(TimeUnit.MILLISECONDS)),
                fc -> createQuery(conventionName, conventionTags, fc.count()),
                a -> {throw new NotImplementedException("");},
                a -> {throw new NotImplementedException("");});
            
            // Currently there is no Support for Double.NaN thus we skip it
            if (query.contains("NaN")) {
                continue;
            }

            try {
                if (!IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
                    final PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(query);

                    // To avoid parsing and auto generation of the measurements we could use the following commands:
                    // MManager.getInstance().createTimeseries(new CreateTimeSeriesPlan(new Path(new String[]{ conventionName, "value"}), TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.GZIP, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ""));
                    // InsertPlan plan = new InsertPlan(conventionName, Instant.now().toEpochMilli(), new String[]{"value"}, new TSDataType[]{TSDataType.DOUBLE}, new Object[]{Double.NaN});
                    // executor.processNonQuery(plan);

                    final boolean success = executor.processNonQuery(physicalPlan);
                    if (!success) {
                        logger.warn("Unable to process metrics query '{}'!", query);
                    }
                }
            } catch (QueryProcessException | StorageEngineException | StorageGroupNotSetException e) {
                logger.error("Unable to store metrics", e);
            }
        }
    }

    private String createQuery(String conventionName, List<Tag> conventionTags, double value) {
        final String tagKeys = conventionTags.stream()
            .map(Tag::getKey)
            .collect(Collectors.joining(","));
        final String tagValues = conventionTags.stream()
            .map(Tag::getValue)
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.joining(","));

        final String escapedPath = escapeName(conventionName);

        final String query;
        if (tagKeys.isEmpty()) {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, value) VALUES (NOW(), %f)", escapedPath, value);
        } else {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, %s, value) VALUES (NOW(), %s, %f)", escapedPath, tagKeys, tagValues, value);
        }
        return query;
    }

    private String createQueryForTimer(String conventionName, List<Tag> conventionTags, Timer timer) {
        final String tagKeys = conventionTags.stream()
            .map(Tag::getKey)
            .collect(Collectors.joining(","));
        final String tagValues = conventionTags.stream()
            .map(Tag::getValue)
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.joining(","));

        final String escapedPath = escapeName(conventionName);

        final String query;
        if (tagKeys.isEmpty()) {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, _count, _mean, _max, _total) VALUES (NOW(), %d, %f, %f, %f)", conventionName, timer.count(), timer.mean(TimeUnit.MILLISECONDS), timer.max(TimeUnit.MILLISECONDS), timer.totalTime(TimeUnit.MILLISECONDS));
        } else {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, %s, _count, _mean, _max, _total) VALUES (NOW(), %s, %d, %f, %f, %f)", escapedPath, tagKeys, tagValues, timer.count(), timer.mean(TimeUnit.MILLISECONDS), timer.max(TimeUnit.MILLISECONDS), timer.totalTime(TimeUnit.MILLISECONDS));
        }
        return query;
    }

    private String createQueryForLTT(String conventionName, List<Tag> conventionTags, LongTaskTimer timer) {
        final String tagKeys = conventionTags.stream()
                .map(Tag::getKey)
                .collect(Collectors.joining(","));
        final String tagValues = conventionTags.stream()
                .map(Tag::getValue)
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(","));

        final String escapedPath = escapeName(conventionName);

        final String query;
        if (tagKeys.isEmpty()) {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, _active, _duration) VALUES (NOW(), %d, %f)", conventionName, timer.activeTasks(), timer.duration(TimeUnit.MILLISECONDS));
        } else {
            query = String.format(Locale.ENGLISH, "INSERT INTO root._metrics.%s (timestamp, %s, _active, _duration) VALUES (NOW(), %s, %d, %f)", escapedPath, tagKeys, tagValues, timer.activeTasks(), timer.duration(TimeUnit.MILLISECONDS));
        }
        return query;
    }

    private String escapeName(String conventionName) {
        return conventionName
                .replace("load", "_load")
                .replace("count", "_count")
                .replace("time", "_time")
                .replace("storage", "_storage")
                .replace("insert", "_insert");
    }

}
