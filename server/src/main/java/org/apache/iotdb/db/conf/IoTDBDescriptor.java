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
import org.apache.iotdb.db.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);
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

  private String getPropsUrl() {
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + IoTDBConfig.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            IoTDBConfig.CONFIG_NAME);
        // update all data seriesPath
        conf.updatePath();
        return null;
      }
    } else {
      url += (File.separatorChar + IoTDBConfig.CONFIG_NAME);
    }
    return url;
  }

  /**
   * load an property file and set TsfileDBConfig variables.
   */
  private void loadProps() {
    InputStream inputStream;

    String url = getPropsUrl();
    if (url == null) {
      return;
    }

    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", url, e);
      // update all data seriesPath
      conf.updatePath();
      return;
    }

    logger.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
      conf.setEnableStatMonitor(Boolean
          .parseBoolean(properties.getProperty("enable_stat_monitor",
              Boolean.toString(conf.isEnableStatMonitor()))));
      conf.setBackLoopPeriodSec(Integer
          .parseInt(properties.getProperty("back_loop_period_in_second",
              Integer.toString(conf.getBackLoopPeriodSec()))));
      int statMonitorDetectFreqSec = Integer.parseInt(
          properties.getProperty("stat_monitor_detect_freq_in_second",
              Integer.toString(conf.getStatMonitorDetectFreqSec())));
      int statMonitorRetainIntervalSec = Integer.parseInt(
          properties.getProperty("stat_monitor_retain_interval_in_second",
              Integer.toString(conf.getStatMonitorRetainIntervalSec())));
      // the conf value must > default value, or may cause system unstable
      if (conf.getStatMonitorDetectFreqSec() < statMonitorDetectFreqSec) {
        conf.setStatMonitorDetectFreqSec(statMonitorDetectFreqSec);
      } else {
        logger.info("The stat_monitor_detect_freq_sec value is smaller than default,"
            + " use default value");
      }

      if (conf.getStatMonitorRetainIntervalSec() < statMonitorRetainIntervalSec) {
        conf.setStatMonitorRetainIntervalSec(statMonitorRetainIntervalSec);
      } else {
        logger.info("The stat_monitor_retain_interval_sec value is smaller than default,"
            + " use default value");
      }

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      conf.setRpcThriftCompressionEnable(Boolean.parseBoolean(properties.getProperty("rpc_thrift_compression_enable",
              Boolean.toString(conf.isRpcThriftCompressionEnable()))));

      conf.setRpcPort(Integer.parseInt(properties.getProperty("rpc_port",
          Integer.toString(conf.getRpcPort()))));

      conf.setTimestampPrecision(properties.getProperty("timestamp_precision",
          conf.getTimestampPrecision()));

      conf.setEnableParameterAdapter(
          Boolean.parseBoolean(properties.getProperty("enable_parameter_adapter",
              Boolean.toString(conf.isEnableParameterAdapter()))));

      conf.setMetaDataCacheEnable(
          Boolean.parseBoolean(properties.getProperty("meta_data_cache_enable",
              Boolean.toString(conf.isMetaDataCacheEnable()))));

      initMemoryAllocate(properties);

      conf.setEnableWal(Boolean.parseBoolean(properties.getProperty("enable_wal",
          Boolean.toString(conf.isEnableWal()))));

      conf.setBaseDir(properties.getProperty("base_dir", conf.getBaseDir()));

      conf.setSystemDir(FilePathUtils.regularizePath(conf.getBaseDir()) + "system");

      conf.setSchemaDir(FilePathUtils.regularizePath(conf.getSystemDir()) + "schema");

      conf.setDataDirs(properties.getProperty("data_dirs", conf.getDataDirs()[0])
          .split(","));

      conf.setWalFolder(properties.getProperty("wal_dir", conf.getWalFolder()));

      conf.setFlushWalThreshold(Integer
          .parseInt(properties.getProperty("flush_wal_threshold",
              Integer.toString(conf.getFlushWalThreshold()))));

      conf.setForceWalPeriodInMs(Long
          .parseLong(properties.getProperty("force_wal_period_in_ms",
              Long.toString(conf.getForceWalPeriodInMs()))));

      int walBufferSize = Integer.parseInt(properties.getProperty("wal_buffer_size",
          Integer.toString(conf.getWalBufferSize())));
      if (walBufferSize > 0) {
        conf.setWalBufferSize(walBufferSize);
      }

      conf.setMultiDirStrategyClassName(properties.getProperty("multi_dir_strategy",
          conf.getMultiDirStrategyClassName()));

      conf.setMergeConcurrentThreads(Integer
          .parseInt(properties.getProperty("merge_concurrent_threads",
              Integer.toString(conf.getMergeConcurrentThreads()))));
      if (conf.getMergeConcurrentThreads() <= 0
          || conf.getMergeConcurrentThreads() > Runtime.getRuntime().availableProcessors()) {
        conf.setMergeConcurrentThreads(Runtime.getRuntime().availableProcessors());
      }

      conf.setFetchSize(Integer.parseInt(properties.getProperty("fetch_size",
          Integer.toString(conf.getFetchSize()))));

      long tsfileSizeThreshold = Long.parseLong(properties
          .getProperty("tsfile_size_threshold",
              Long.toString(conf.getTsFileSizeThreshold())).trim());
      if (tsfileSizeThreshold > 0) {
        conf.setTsFileSizeThreshold(tsfileSizeThreshold);
      }

      long memTableSizeThreshold = Long.parseLong(properties
          .getProperty("memtable_size_threshold",
              Long.toString(conf.getMemtableSizeThreshold())).trim());
      if (memTableSizeThreshold > 0) {
        conf.setMemtableSizeThreshold(memTableSizeThreshold);
      }

      conf.setSyncEnable(Boolean
          .parseBoolean(properties.getProperty("is_sync_enable",
              Boolean.toString(conf.isSyncEnable()))));

      conf.setSyncServerPort(Integer
          .parseInt(properties.getProperty("sync_server_port",
              Integer.toString(conf.getSyncServerPort())).trim()));

      conf.setUpdateHistoricalDataPossibility(Boolean.parseBoolean(
          properties.getProperty("update_historical_data_possibility",
              Boolean.toString(conf.isSyncEnable()))));
      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));

      conf.setConcurrentFlushThread(Integer
          .parseInt(properties.getProperty("concurrent_flush_thread",
              Integer.toString(conf.getConcurrentFlushThread()))));

      if (conf.getConcurrentFlushThread() <= 0) {
        conf.setConcurrentFlushThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setmManagerCacheSize(Integer
          .parseInt(properties.getProperty("schema_manager_cache_size",
              Integer.toString(conf.getmManagerCacheSize())).trim()));

      conf.setLanguageVersion(properties.getProperty("language_version",
          conf.getLanguageVersion()).trim());

      if (properties.containsKey("chunk_buffer_pool_enable")) {
        conf.setChunkBufferPoolEnable(Boolean
            .parseBoolean(properties.getProperty("chunk_buffer_pool_enable")));
      }
      String tmpTimeZone = properties.getProperty("time_zone", conf.getZoneID().toString());
      conf.setZoneID(ZoneId.of(tmpTimeZone.trim()));
      logger.info("Time zone has been set to {}", conf.getZoneID());

      conf.setEnablePerformanceStat(Boolean
          .parseBoolean(properties.getProperty("enable_performance_stat",
              Boolean.toString(conf.isEnablePerformanceStat())).trim()));

      conf.setPerformanceStatDisplayInterval(Long
          .parseLong(properties.getProperty("performance_stat_display_interval",
              Long.toString(conf.getPerformanceStatDisplayInterval())).trim()));
      conf.setPerformanceStatMemoryInKB(Integer
          .parseInt(properties.getProperty("performance_stat_memory_in_kb",
              Integer.toString(conf.getPerformanceStatMemoryInKB())).trim()));

      int maxConcurrentClientNum = Integer.parseInt(properties.
          getProperty("max_concurrent_client_num",
              Integer.toString(conf.getMaxConcurrentClientNum()).trim()));
      if(maxConcurrentClientNum <= 0)
        maxConcurrentClientNum = 65535;
      conf.setMaxConcurrentClientNum(maxConcurrentClientNum);

    } catch (IOException e) {
      logger.warn("Cannot load config file because, use default configuration", e);
    } catch (Exception e) {
      logger.warn("Incorrect format in config file, use default configuration", e);
    } finally {
      // update all data seriesPath
      conf.updatePath();
      try {
        inputStream.close();
      } catch (IOException e) {
        logger.error("Fail to close config file input stream because ", e);
      }
    }
  }

  private void initMemoryAllocate(Properties properties) {
    String memoryAllocateProportion = properties.getProperty("write_read_free_memory_proportion");
    if (memoryAllocateProportion != null) {
      String[] proportions = memoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = Runtime.getRuntime().maxMemory();
      conf.setAllocateMemoryForWrite(
          maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
      conf.setAllocateMemoryForRead(
          maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
    }

    if (!conf.isMetaDataCacheEnable()) {
      return;
    }

    String queryMemoryAllocateProportion = properties
        .getProperty("filemeta_chunkmeta_free_memory_proportion");
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = conf.getAllocateMemoryForRead();
      try {
        conf.setAllocateMemoryForFileMetaDataCache(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setAllocateMemoryForChumkMetaDataCache(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
      } catch (Exception e) {
        throw new RuntimeException(
            "Each subsection of configuration item filemeta_chunkmeta_free_memory_proportion should be an integer, which is "
                + queryMemoryAllocateProportion);
      }

    }

  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();
  }
}
