/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.micrometer.reporter;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricsUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IoTDBMeterRegistry extends StepMeterRegistry {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBMeterRegistry.class);
  private static final MetricConfig.IoTDBReporterConfig ioTDBReporterConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig().getIoTDBReporterConfig();
  private final SessionPool sessionPool;

  public IoTDBMeterRegistry(StepRegistryConfig config, Clock clock) {
    super(config, clock);
    this.sessionPool =
        new SessionPool(
            ioTDBReporterConfig.getHost(),
            ioTDBReporterConfig.getPort(),
            ioTDBReporterConfig.getUsername(),
            ioTDBReporterConfig.getPassword(),
            ioTDBReporterConfig.getMaxConnectionNumber());
  }

  @Override
  public void stop() {
    super.stop();
    if (sessionPool != null) {
      sessionPool.close();
    }
  }

  @Override
  protected void publish() {
    Long time = System.currentTimeMillis();
    getMeters()
        .forEach(
            meter -> {
              Meter.Id id = meter.getId();
              String name = id.getName();
              List<Tag> tags = id.getTags();
              Map<String, String> labels = tagsConvertToMap(tags);
              meter.use(
                  gauge -> {
                    updateValue(name, labels, gauge.value(), time);
                  },
                  counter -> {
                    updateValue(name, labels, counter.count(), time);
                  },
                  timer -> {
                    writeSnapshotAndCount(name, labels, timer.takeSnapshot(), time);
                  },
                  summary -> {
                    writeSnapshotAndCount(name, labels, summary.takeSnapshot(), time);
                  },
                  longTaskTimer -> {
                    updateValue(name, labels, (double) longTaskTimer.activeTasks(), time);
                  },
                  timeGauge -> {
                    updateValue(name, labels, timeGauge.value(getBaseTimeUnit()), time);
                  },
                  functionCounter -> {
                    updateValue(name, labels, functionCounter.count(), time);
                  },
                  functionTimer -> {
                    updateValue(name, labels, functionTimer.count(), time);
                  },
                  m -> {
                    logger.debug("unknown meter:" + meter);
                  });
            });
  }

  private void writeSnapshotAndCount(
      String name, Map<String, String> labels, HistogramSnapshot snapshot, Long time) {
    updateValue(name + "_max", labels, snapshot.max(), time);
    updateValue(name + "_mean", labels, snapshot.mean(), time);
    updateValue(name + "_total", labels, snapshot.total(), time);
    updateValue(name + "_count", labels, (double) snapshot.count(), time);
  }

  private Map<String, String> tagsConvertToMap(List<Tag> tags) {
    Map<String, String> labels = new HashMap<>();
    for (Tag tag : tags) {
      labels.put(tag.getKey(), tag.getValue());
    }
    return labels;
  }

  private void updateValue(String name, Map<String, String> labels, Double value, Long time) {
    if (value != null) {
      String deviceId = MetricsUtils.generatePath(name, labels);
      List<String> sensors = Collections.singletonList("value");
      List<TSDataType> dataTypes = Collections.singletonList(TSDataType.DOUBLE);
      List<Object> values = Collections.singletonList(value);

      try {
        sessionPool.insertRecord(deviceId, time, sensors, dataTypes, values);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        logger.warn("Failed to insert record");
      }
    }
  }

  @Override
  protected TimeUnit getBaseTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }
}
