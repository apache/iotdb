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
package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.db.wal.node.WALNode;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.stream.Stream;

public class MetricsService extends MetricService implements MetricsServiceMBean, IService {
  private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private MetricsService() {}

  @Override
  public void start() throws StartupException {
    try {
      if (isEnable()) {
        logger.info("Start to start metric Service.");
        JMXService.registerMBean(getInstance(), mbeanName);
        startService();
        logger.info("Finish start metric Service");
      }
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (isEnable()) {
      logger.info("Stop metric Service.");
      stopService();
      JMXService.deregisterMBean(mbeanName);
      logger.info("Finish stop metric Service");
    }
  }

  @Override
  public void restartService() throws StartupException {
    stopService();
    startService();
  }

  @Override
  public void collectFileSystemInfo() {
    logger.info("start collecting fileSize and fileCount of wal/seq/unseq");
    String[] walDirs = IoTDBDescriptor.getInstance().getConfig().getWalDirs();
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        walDir,
        value -> Stream.of(value).mapToLong(dir -> FileUtils.getDirSize(dir)).sum(),
        Tag.NAME.toString(),
        "wal");

    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dataDirs,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      dir += File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME;
                      return FileUtils.getDirSize(dir);
                    })
                .sum(),
        Tag.NAME.toString(),
        "seq");
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dataDirs,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      dir += File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME;
                      return FileUtils.getDirSize(dir);
                    })
                .sum(),
        Tag.NAME.toString(),
        "unseq");
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        walDir,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      File walFolder = new File(dir);
                      File[] walNodeFolders = walFolder.listFiles(WALNode::walNodeFolderNameFilter);
                      for (File walNodeFolder : walNodeFolders) {
                        if (walNodeFolder.exists() && walNodeFolder.isDirectory()) {
                          return org.apache.commons.io.FileUtils.listFiles(
                                  walNodeFolder, null, true)
                              .size();
                        }
                      }
                      return 0L;
                    })
                .sum(),
        Tag.NAME.toString(),
        "wal");
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        dataDirs,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      dir += File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME;
                      return org.apache.commons.io.FileUtils.listFiles(
                              new File(dir), new String[] {"tsfile"}, true)
                          .size();
                    })
                .sum(),
        Tag.NAME.toString(),
        "seq");
    metricManager.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        dataDirs,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      dir += File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME;
                      return org.apache.commons.io.FileUtils.listFiles(
                              new File(dir), new String[] {"tsfile"}, true)
                          .size();
                    })
                .sum(),
        Tag.NAME.toString(),
        "unseq");
  }

  @Override
  public void reloadProperties(ReloadLevel reloadLevel) {
    logger.info("Reload properties of metric service");
    synchronized (this) {
      try {
        switch (reloadLevel) {
          case START_METRIC:
            isEnableMetric = true;
            start();
            break;
          case STOP_METRIC:
            stop();
            isEnableMetric = false;
            break;
          case RESTART_METRIC:
            stop();
            isEnableMetric = true;
            start();
            break;
          case RESTART_REPORTER:
            compositeReporter.restartAll();
            logger.info("Finish restart metric reporters.");
            break;
          case NOTHING:
            logger.debug("There are nothing change in metric module.");
            break;
          default:
            break;
        }
      } catch (StartupException startupException) {
        logger.error("Failed to start metric when reload properties");
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRICS_SERVICE;
  }

  public static MetricsService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricsService INSTANCE = new MetricsService();

    private MetricsServiceHolder() {}
  }
}
