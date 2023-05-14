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
              .inDomain("org.apache.iotdb.metrics")
              .build();
      jmxReporter.start();
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
