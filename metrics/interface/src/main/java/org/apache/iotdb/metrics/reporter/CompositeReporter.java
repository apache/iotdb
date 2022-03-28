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

package org.apache.iotdb.metrics.reporter;

import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CompositeReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeReporter.class);
  private final List<Reporter> reporters = new ArrayList<>();

  /** Start all reporter */
  public boolean startAll() {
    boolean result = true;
    for (Reporter reporter : reporters) {
      if (!reporter.start()) {
        LOGGER.warn("Failed to init {} reporter.", reporter.getReporterType());
        result = false;
      }
    }
    return result;
  }

  /** Start reporter by name values in jmx, prometheus, iotdb */
  public boolean start(ReporterType reporterType) {
    for (Reporter reporter : reporters) {
      if (reporter.getReporterType() == reporterType) {
        return reporter.start();
      }
    }
    LOGGER.error("Failed to find {} reporter.", reporterType);
    return false;
  }

  /** Stop all reporter */
  public boolean stopAll() {
    boolean result = true;
    for (Reporter reporter : reporters) {
      if (!reporter.stop()) {
        LOGGER.error("Failed to stop {} reporter.", reporter.getReporterType());
        result = false;
      }
    }
    return result;
  }

  /** Stop reporter by name, values in jmx, prometheus, iotdb */
  public boolean stop(ReporterType reporterType) {
    for (Reporter reporter : reporters) {
      if (reporter.getReporterType() == reporterType) {
        return reporter.stop();
      }
    }
    LOGGER.error("Failed to stop reporter: {}", reporterType.name());
    return true;
  }

  /** Clear reporter */
  public void clearReporter() {
    reporters.clear();
  }

  /** Add reporter */
  public void addReporter(Reporter reporter) {
    for (Reporter originReporter : reporters) {
      if (originReporter.getReporterType() == reporter.getReporterType()) {
        LOGGER.warn("Already exist reporter, type is " + reporter.getReporterType());
        return;
      }
    }
    reporters.add(reporter);
  }
}
