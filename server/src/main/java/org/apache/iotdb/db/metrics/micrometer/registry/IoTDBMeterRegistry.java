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

package org.apache.iotdb.db.metrics.micrometer.registry;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IoTDBMeterRegistry extends StepMeterRegistry {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBMeterRegistry.class);
  private BasicServiceProvider basicServiceProvider;
  private final int rpcPort;
  private final String address;

  public IoTDBMeterRegistry(StepRegistryConfig config, Clock clock) {
    super(config, clock);
    IoTDBConfig ioTDBConfig = new IoTDBConfig();
    rpcPort = ioTDBConfig.getRpcPort();
    address = ioTDBConfig.getRpcAddress();
    try {
      basicServiceProvider = new BasicServiceProvider();
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void publish() {
    getMeters()
        .forEach(
            meter -> {
              Meter.Id id = meter.getId();
              String name = id.getName();
              List<Tag> tags = id.getTags();
              Double value =
                  (Double)
                      meter.match(
                          Gauge::value,
                          Counter::count,
                          timer -> {
                            HistogramSnapshot snapshot = timer.takeSnapshot();
                            return ((Long) snapshot.count()).doubleValue();
                          },
                          summary -> {
                            HistogramSnapshot snapshot = summary.takeSnapshot();
                            return ((Long) snapshot.count()).doubleValue();
                          },
                          LongTaskTimer::activeTasks,
                          timeGauge -> timeGauge.value(getBaseTimeUnit()),
                          FunctionCounter::count,
                          FunctionTimer::count,
                          m -> {
                            logger.debug("unknown meter:" + meter);
                            return null;
                          });
              updateValue(name, tags, value);
            });
  }

  private void updateValue(String name, List<Tag> tags, Double value) {
    if (value != null) {
      try {
        InsertRowPlan insertRowPlan =
            new InsertRowPlan(
                new PartialPath(generatePath(name, tags)),
                System.currentTimeMillis(),
                new String[] {"value"},
                DataTypeUtils.getValueBuffer(
                    new ArrayList<>(Arrays.asList(TSDataType.DOUBLE)),
                    new ArrayList<>(Arrays.asList(value))),
                false);
        basicServiceProvider.executeNonQuery(insertRowPlan);
      } catch (IllegalPathException
          | IoTDBConnectionException
          | QueryProcessException
          | StorageGroupNotSetException
          | StorageEngineException e) {
        logger.error("illegal insertRowPlan,reason:" + e.getMessage());
      }
    }
  }

  private String generatePath(String name, List<Tag> tags) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("root._metric.\"")
        .append(address)
        .append(":")
        .append(rpcPort)
        .append("\"")
        .append(".")
        .append("\"")
        .append(name)
        .append("\"");
    for (Tag tag : tags) {
      stringBuilder.append(".\"").append(tag.getValue()).append("\"");
    }
    return stringBuilder.toString();
  }

  @Override
  protected TimeUnit getBaseTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }
}
