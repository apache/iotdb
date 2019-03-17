/**
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
package org.apache.iotdb.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController.ControllerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDescriptor.class);
  private IoTDBConfig conf = new IoTDBConfig();

  private IoTDBDescriptor() {
    loadProps();
  }

  public static IoTDBDescriptor getInstance() {
    return IoTDBDescriptorHolder.INSTANCE;
  }

  public IoTDBConfig getConfig() {
    return conf;
  }

  /**
   * load an property file and set TsfileDBConfig variables.
   */
  private void loadProps() {
    InputStream inputStream;
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + IoTDBConfig.CONFIG_NAME;
      } else {
        LOGGER.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            IoTDBConfig.CONFIG_NAME);
        // update all data seriesPath
        conf.updatePath();
        return;
      }
    } else {
      url += (File.separatorChar + IoTDBConfig.CONFIG_NAME);
    }

    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      LOGGER.warn("Fail to find config file {}", url, e);
      // update all data seriesPath
      conf.updatePath();
      return;
    }

    LOGGER.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
      conf.setEnableStatMonitor(Boolean
          .parseBoolean(properties.getProperty("enable_stat_monitor",
                  Boolean.toString(conf.isEnableStatMonitor()))));
      conf.setBackLoopPeriodSec(Integer
          .parseInt(properties.getProperty("back_loop_period_sec",
                  Integer.toString(conf.getBackLoopPeriodSec()))));
      int statMonitorDetectFreqSec = Integer.parseInt(
          properties.getProperty("stat_monitor_detect_freq_sec",
                  Integer.toString(conf.getStatMonitorDetectFreqSec())));
      int statMonitorRetainIntervalSec = Integer.parseInt(
          properties.getProperty("stat_monitor_retain_interval_sec",
                  Integer.toString(conf.getStatMonitorRetainIntervalSec())));
      // the conf value must > default value, or may cause system unstable
      if (conf.getStatMonitorDetectFreqSec() < statMonitorDetectFreqSec) {
        conf.setStatMonitorDetectFreqSec(statMonitorDetectFreqSec);
      } else {
        LOGGER.info("The stat_monitor_detect_freq_sec value is smaller than default,"
            + " use default value");
      }

      if (conf.getStatMonitorRetainIntervalSec() < statMonitorRetainIntervalSec) {
        conf.setStatMonitorRetainIntervalSec(statMonitorRetainIntervalSec);
      } else {
        LOGGER.info("The stat_monitor_retain_interval_sec value is smaller than default,"
            + " use default value");
      }

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      conf.setRpcPort(Integer.parseInt(properties.getProperty("rpc_port",
              Integer.toString(conf.getRpcPort()))));

      conf.setEnableWal(Boolean.parseBoolean(properties.getProperty("enable_wal",
              Boolean.toString(conf.isEnableWal()))));

      conf.setFlushWalThreshold(Integer
          .parseInt(properties.getProperty("flush_wal_threshold",
                  Integer.toString(conf.getFlushWalThreshold()))));
      conf.setFlushWalPeriodInMs(Long
          .parseLong(properties.getProperty("flush_wal_period_in_ms",
                  Long.toString(conf.getFlushWalPeriodInMs()))));
      conf.setForceWalPeriodInMs(Long
          .parseLong(properties.getProperty("force_wal_period_in_ms",
                  Long.toString(conf.getForceWalPeriodInMs()))));

      conf.setDataDir(properties.getProperty("data_dir", conf.getDataDir()));
      conf.setBufferWriteDirs(properties.getProperty("tsfile_dir", conf.DEFAULT_TSFILE_DIR)
          .split(","));
      conf.setSysDir(properties.getProperty("sys_dir", conf.getSysDir()));
      conf.setWalDir(properties.getProperty("wal_dir", conf.getWalDir()));

      conf.setMultDirStrategyClassName(properties.getProperty("mult_dir_strategy",
          conf.getMultDirStrategyClassName()));

      conf.setMaxOpenFolder(Integer.parseInt(properties.getProperty("max_opened_folder",
              Integer.toString(conf.getMaxOpenFolder()))));
      conf.setMergeConcurrentThreads(Integer
          .parseInt(properties.getProperty("merge_concurrent_threads",
                  Integer.toString(conf.getMergeConcurrentThreads()))));
      if (conf.getMergeConcurrentThreads() <= 0
          || conf.getMergeConcurrentThreads() > Runtime.getRuntime().availableProcessors()) {
        conf.setMergeConcurrentThreads(Runtime.getRuntime().availableProcessors());
      }

      conf.setFetchSize(Integer.parseInt(properties.getProperty("fetch_size",
          Integer.toString(conf.getFetchSize()))));

      conf.setPeriodTimeForFlush(Long.parseLong(
          properties.getProperty("period_time_for_flush_in_second",
                  Long.toString(conf.getPeriodTimeForFlush())).trim()));
      conf.setPeriodTimeForMerge(Long.parseLong(
          properties.getProperty("period_time_for_merge_in_second",
              Long.toString(conf.getPeriodTimeForMerge())).trim()));
      conf.setEnableTimingCloseAndMerge(Boolean.parseBoolean(properties
          .getProperty("enable_timing_close_and_Merge",
                  Boolean.toString(conf.isEnableTimingCloseAndMerge())).trim()));

      conf.setMemThresholdWarning((long) (Runtime.getRuntime().maxMemory() * Double.parseDouble(
          properties.getProperty("mem_threshold_warning",
                  Long.toString(conf.getMemThresholdWarning())).trim())));
      conf.setMemThresholdDangerous((long) (Runtime.getRuntime().maxMemory() * Double.parseDouble(
          properties.getProperty("mem_threshold_dangerous",
                  Long.toString(conf.getMemThresholdDangerous())).trim())));

      conf.setMemMonitorInterval(Long
          .parseLong(properties.getProperty("mem_monitor_interval",
                  Long.toString(conf.getMemMonitorInterval())).trim()));

      conf.setMemControllerType(Integer
          .parseInt(properties.getProperty("mem_controller_type",
                  Integer.toString(conf.getMemControllerType())).trim()));
      conf.setMemControllerType(conf.getMemControllerType() >= ControllerType.values().length ? 0
          : conf.getMemControllerType());

      conf.setBufferwriteMetaSizeThreshold(Long.parseLong(properties
          .getProperty("bufferwrite_meta_size_threshold",
                  Long.toString(conf.getBufferwriteMetaSizeThreshold())).trim()));
      conf.setBufferwriteFileSizeThreshold(Long.parseLong(properties
          .getProperty("bufferwrite_file_size_threshold",
                  Long.toString(conf.getBufferwriteFileSizeThreshold())).trim()));

      conf.setOverflowMetaSizeThreshold(Long.parseLong(
          properties.getProperty("overflow_meta_size_threshold",
                  Long.toString(conf.getOverflowMetaSizeThreshold())).trim()));
      conf.setOverflowFileSizeThreshold(Long.parseLong(
          properties.getProperty("overflow_file_size_threshold",
              Long.toString(conf.getOverflowFileSizeThreshold())).trim()));

      conf.setSyncEnable(Boolean
          .parseBoolean(properties.getProperty("is_sync_enable",
                  Boolean.toString(conf.isSyncEnable()))));
      conf.setSyncServerPort(Integer
          .parseInt(properties.getProperty("sync_server_port",
                  Integer.toString(conf.getSyncServerPort())).trim()));
      conf.setUpdate_historical_data_possibility(Boolean.parseBoolean(
          properties.getProperty("update_historical_data_possibility",
                  Boolean.toString(conf.isSyncEnable()))));
      conf.setIpWhiteList(properties.getProperty("IP_white_list", conf.getIpWhiteList()));

      if (conf.getMemThresholdWarning() <= 0) {
        conf.setMemThresholdWarning(IoTDBConstant.MEM_THRESHOLD_WARNING_DEFAULT);
      }
      if (conf.getMemThresholdDangerous() < conf.getMemThresholdWarning()) {
        conf.setMemThresholdDangerous(Math.max(conf.getMemThresholdWarning(),
            IoTDBConstant.MEM_THRESHOLD_DANGEROUS_DEFAULT));
      }

      conf.setConcurrentFlushThread(Integer
          .parseInt(properties.getProperty("concurrent_flush_thread",
                  Integer.toString(conf.getConcurrentFlushThread()))));
      if (conf.getConcurrentFlushThread() <= 0) {
        conf.setConcurrentFlushThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setEnableMemMonitor(Boolean
          .parseBoolean(properties.getProperty("enable_mem_monitor",
                  Boolean.toString(conf.isEnableMemMonitor())).trim()));
      conf.setEnableSmallFlush(Boolean
          .parseBoolean(properties.getProperty("enable_small_flush",
                  Boolean.toString(conf.isEnableSmallFlush())).trim()));
      conf.setSmallFlushInterval(Long
          .parseLong(properties.getProperty("small_flush_interval",
                  Long.toString(conf.getSmallFlushInterval())).trim()));
      conf.setExternalSortThreshold(Integer.parseInt(
          properties.getProperty("external_sort_threshold",
                  Integer.toString(conf.getExternalSortThreshold())).trim()));
      conf.setmManagerCacheSize(Integer
          .parseInt(properties.getProperty("schema_manager_cache_size",
                  Integer.toString(conf.getmManagerCacheSize())).trim()));

      int maxLogEntrySize = Integer
          .parseInt(properties.getProperty("max_log_entry_size",
                  Integer.toString(conf.getMaxLogEntrySize())).trim());
      conf.setMaxLogEntrySize(maxLogEntrySize > 0 ? maxLogEntrySize :
          conf.getMaxLogEntrySize());

      conf.setLanguageVersion(properties.getProperty("language_version",
          conf.getLanguageVersion()).trim());

      String tmpTimeZone = properties.getProperty("time_zone", conf.getZoneID().toString());
      try {
        conf.setZoneID(ZoneId.of(tmpTimeZone.trim()));
        LOGGER.info("Time zone has been set to {}", conf.getZoneID());
      } catch (Exception e) {
        LOGGER.error("Time zone format error {}, use default configuration {}", tmpTimeZone,
            conf.getZoneID(), e);
      }

    } catch (IOException e) {
      LOGGER.warn("Cannot load config file because, use default configuration", e);
    } catch (Exception e) {
      LOGGER.warn("Incorrect format in config file, use default configuration", e);
    } finally {
      // update all data seriesPath
      conf.updatePath();
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close config file input stream because ", e);
        }
      }
    }
  }

  private static class IoTDBDescriptorHolder {
    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();
  }
}
