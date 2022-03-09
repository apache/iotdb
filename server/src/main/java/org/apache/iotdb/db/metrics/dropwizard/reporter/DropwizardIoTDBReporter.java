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

package org.apache.iotdb.db.metrics.dropwizard.reporter;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.MetricFilter;

import java.util.concurrent.TimeUnit;

public class DropwizardIoTDBReporter implements Reporter {

  private MetricManager dropwizardMetricManager = null;
  private IoTDBReporter ioTDBReporter;

  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  @Override
  public boolean start() {
    if (!metricConfig.getEnableMetric()) {
      return false;
    }
    ioTDBReporter =
        IoTDBReporter.forRegistry(
                ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
            .prefixedWith("dropwizard:")
            .filter(MetricFilter.ALL)
            .build();
    ioTDBReporter.start(metricConfig.getPushPeriodInSecond(), TimeUnit.SECONDS);
    return true;
  }

  @Override
  public boolean stop() {
    if (ioTDBReporter == null) {
      return false;
    }
    ioTDBReporter.stop();
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }
}
