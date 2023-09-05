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

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropwizardJmxReporter implements JmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardJmxReporter.class);

  private AbstractMetricManager dropwizardMetricManager = null;
  private com.codahale.metrics.jmx.JmxReporter jmxReporter = null;

  @Override
  public boolean start() {
    if (jmxReporter != null) {
      LOGGER.warn("Dropwizard JmxReporter already start!");
      return false;
    }
    try {
      jmxReporter =
          com.codahale.metrics.jmx.JmxReporter.forRegistry(
                  ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
              .inDomain("org.apache.iotdb.metrics")
              .build();
      jmxReporter.start();
    } catch (Exception e) {
      jmxReporter = null;
      LOGGER.warn("Dropwizard JmxReporter failed to start, because ", e);
      return false;
    }
    LOGGER.info("Dropwizard JmxReporter start!");
    return true;
  }

  @Override
  public boolean stop() {
    try {
      if (jmxReporter != null) {
        jmxReporter.stop();
        jmxReporter = null;
      }
    } catch (RuntimeException e) {
      // catch possible RuntimeException throwed by stop method of jmxReporter
      LOGGER.warn("Dropwizard JmxReporter failed to stop, because ", e);
      return false;
    }
    LOGGER.info("Dropwizard JmxReporter stop!");
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.JMX;
  }

  @Override
  public void setMetricManager(AbstractMetricManager metricManager) {
    this.dropwizardMetricManager = metricManager;
  }
}
