/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.service.metrics.file;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class SystemRelatedFileMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemRelatedFileMetrics.class);
  private static final MetricConfig CONFIG = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final Runtime runtime = Runtime.getRuntime();
  private String[] getOpenFileNumberCommand;

  @SuppressWarnings("squid:S1075")
  private String fileHandlerCntPathInLinux = "/proc/%s/fd";

  public SystemRelatedFileMetrics() {
    fileHandlerCntPathInLinux = String.format(fileHandlerCntPathInLinux, CONFIG.getPid());
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    if ((CONFIG.getSystemType() == SystemType.LINUX || CONFIG.getSystemType() == SystemType.MAC)
        && !CONFIG.getPid().isEmpty()) {
      this.getOpenFileNumberCommand =
          new String[] {"/bin/sh", "-c", String.format("lsof -p %s | wc -l", CONFIG.getPid())};
      metricService.createAutoGauge(
          Metric.FILE_COUNT.toString(),
          MetricLevel.IMPORTANT,
          this,
          SystemRelatedFileMetrics::getOpenFileHandlersNumber,
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }

  private long getOpenFileHandlersNumber() {
    long fdCount = 0;
    try {
      if (CONFIG.getSystemType() == SystemType.LINUX) {
        // count the fd in the system directory instead of
        // calling runtime.exec() which could be much slower
        File fdDir = new File(fileHandlerCntPathInLinux);
        if (fdDir.exists()) {
          File[] fds = fdDir.listFiles();
          fdCount = fds == null ? 0 : fds.length;
        }
      } else if ((CONFIG.getSystemType() == SystemType.MAC) && !CONFIG.getPid().isEmpty()) {
        Process process = runtime.exec(getOpenFileNumberCommand);
        StringBuilder result = new StringBuilder();
        try (BufferedReader input =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = input.readLine()) != null) {
            result.append(line);
          }
        }
        fdCount = Long.parseLong(result.toString().trim());
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to get open file number, because ", e);
    }
    return fdCount;
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    if ((CONFIG.getSystemType() == SystemType.LINUX || CONFIG.getSystemType() == SystemType.MAC)
        && !CONFIG.getPid().isEmpty()) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.FILE_COUNT.toString(),
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }
}
