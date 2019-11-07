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
package org.apache.iotdb.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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

      conf.setMetricsPort(Integer.parseInt(properties.getProperty("metrics_port",
          Integer.toString(conf.getMetricsPort()))));

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      conf.setRpcThriftCompressionEnable(
          Boolean.parseBoolean(properties.getProperty("rpc_thrift_compression_enable",
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

      conf.setQueryDir(FilePathUtils.regularizePath(conf.getBaseDir()) + "query");

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

      conf.setEnableExternalSort(Boolean.parseBoolean(properties
          .getProperty("enable_external_sort", Boolean.toString(conf.isEnableExternalSort()))));
      conf.setExternalSortThreshold(Integer.parseInt(properties
          .getProperty("external_sort_threshold",
              Integer.toString(conf.getExternalSortThreshold()))));
      conf.setMergeMemoryBudget(Long.parseLong(properties.getProperty("merge_memory_budget",
          Long.toString(conf.getMergeMemoryBudget()))));
      conf.setMergeThreadNum(Integer.parseInt(properties.getProperty("merge_thread_num",
          Integer.toString(conf.getMergeThreadNum()))));
      conf.setMergeChunkSubThreadNum(Integer.parseInt(properties.getProperty
          ("merge_chunk_subthread_num",
              Integer.toString(conf.getMergeChunkSubThreadNum()))));
      conf.setContinueMergeAfterReboot(Boolean.parseBoolean(properties.getProperty(
          "continue_merge_after_reboot", Boolean.toString(conf.isContinueMergeAfterReboot()))));
      conf.setMergeFileSelectionTimeBudget(Long.parseLong(properties.getProperty
          ("merge_fileSelection_time_budget",
              Long.toString(conf.getMergeFileSelectionTimeBudget()))));
      conf.setMergeIntervalSec(Long.parseLong(properties.getProperty("merge_interval_sec",
          Long.toString(conf.getMergeIntervalSec()))));
      conf.setForceFullMerge(Boolean.parseBoolean(properties.getProperty("force_full_merge",
          Boolean.toString(conf.isForceFullMerge()))));
      conf.setChunkMergePointThreshold(Integer.parseInt(properties.getProperty(
          "chunk_merge_point_threshold", Integer.toString(conf.getChunkMergePointThreshold()))));

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
          getProperty("rpc_max_concurrent_client_num",
              Integer.toString(conf.getRpcMaxConcurrentClientNum()).trim()));
      if (maxConcurrentClientNum <= 0) {
        maxConcurrentClientNum = 65535;
      }

      conf.setEnableWatermark(Boolean.parseBoolean(properties.getProperty("watermark_module_opened",
          Boolean.toString(conf.isEnableWatermark()).trim())));
      conf.setWatermarkSecretKey(
          properties.getProperty("watermark_secret_key", conf.getWatermarkSecretKey()));
      conf.setWatermarkBitString(
          properties.getProperty("watermark_bit_string", conf.getWatermarkBitString()));
      conf.setWatermarkMethod(
          properties.getProperty("watermark_method", conf.getWatermarkMethod()));

      conf.setAutoCreateSchemaEnabled(
          Boolean.parseBoolean(properties.getProperty("enable_auto_create_schema",
              Boolean.toString(conf.isAutoCreateSchemaEnabled()).trim())));
      conf.setDefaultStorageGroupLevel(
          Integer.parseInt(properties.getProperty("default_storage_group_level",
              Integer.toString(conf.getDefaultStorageGroupLevel()))));
      conf.setDefaultBooleanEncoding(
          properties.getProperty("default_boolean_encoding",
              conf.getDefaultBooleanEncoding().toString()));
      conf.setDefaultInt32Encoding(
          properties.getProperty("default_int32_encoding",
              conf.getDefaultInt32Encoding().toString()));
      conf.setDefaultInt64Encoding(
          properties.getProperty("default_int64_encoding",
              conf.getDefaultInt64Encoding().toString()));
      conf.setDefaultFloatEncoding(
          properties.getProperty("default_float_encoding",
              conf.getDefaultFloatEncoding().toString()));
      conf.setDefaultDoubleEncoding(
          properties.getProperty("default_double_encoding",
              conf.getDefaultDoubleEncoding().toString()));
      conf.setDefaultTextEncoding(
          properties.getProperty("default_text_encoding",
              conf.getDefaultTextEncoding().toString()));

      conf.setRpcMaxConcurrentClientNum(maxConcurrentClientNum);

      conf.setTsFileStorageFs(properties.getProperty("tsfile_storage_fs"));
      conf.setHdfsIp(properties.getProperty("hdfs_ip").split(","));
      conf.setHdfsPort(properties.getProperty("hdfs_port"));
      conf.setDfsNameServices(properties.getProperty("dfs_nameservices"));
      conf.setDfsHaNamenodes(properties.getProperty("dfs_ha_namenodes").split(","));
      conf.setDfsHaAutomaticFailoverEnabled(
          Boolean.parseBoolean(properties.getProperty("dfs_ha_automatic_failover_enabled")));
      conf.setDfsClientFailoverProxyProvider(
          properties.getProperty("dfs_client_failover_proxy_provider"));

      conf.setDefaultTTL(Long.parseLong(properties.getProperty("default_ttl",
          String.valueOf(conf.getDefaultTTL()))));

      // At the same time, set TSFileConfig
      TSFileDescriptor.getInstance().getConfig().setTSFileStorageFs(properties.getProperty("tsfile_storage_fs"));
      TSFileDescriptor.getInstance().getConfig().setHdfsIp(properties.getProperty("hdfs_ip").split(","));
      TSFileDescriptor.getInstance().getConfig().setHdfsPort(properties.getProperty("hdfs_port"));
      TSFileDescriptor.getInstance().getConfig().setDfsNameServices(properties.getProperty("dfs_nameservices"));
      TSFileDescriptor.getInstance().getConfig().setDfsHaNamenodes(properties.getProperty("dfs_ha_namenodes").split(","));
      TSFileDescriptor.getInstance().getConfig().setDfsHaAutomaticFailoverEnabled(
          Boolean.parseBoolean(properties.getProperty("dfs_ha_automatic_failover_enabled")));
      TSFileDescriptor.getInstance().getConfig().setDfsClientFailoverProxyProvider(
          properties.getProperty("dfs_client_failover_proxy_provider"));

      // set tsfile-format config
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(Integer
          .parseInt(properties.getProperty("group_size_in_byte",
              Integer.toString(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()))));
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(Integer
          .parseInt(properties.getProperty("page_size_in_byte",
              Integer.toString(TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()))));
      if (TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() > TSFileDescriptor
          .getInstance().getConfig().getGroupSizeInByte()) {
        logger
            .warn("page_size is greater than group size, will set it as the same with group size");
        TSFileDescriptor.getInstance().getConfig()
            .setPageSizeInByte(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte());
      }
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(Integer
          .parseInt(properties.getProperty("max_number_of_points_in_page",
              Integer.toString(
                  TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage()))));
      TSFileDescriptor.getInstance().getConfig().setTimeSeriesDataType(properties
          .getProperty("time_series_data_type",
              TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType()));
      TSFileDescriptor.getInstance().getConfig().setMaxStringLength(Integer
          .parseInt(properties.getProperty("max_string_length",
              Integer.toString(TSFileDescriptor.getInstance().getConfig().getMaxStringLength()))));
      TSFileDescriptor.getInstance().getConfig().setBloomFilterErrorRate(Double
          .parseDouble(properties.getProperty("bloom_filter_error_rate",
              Double.toString(
                  TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate()))));
      TSFileDescriptor.getInstance().getConfig().setFloatPrecision(Integer
          .parseInt(properties
              .getProperty("float_precision", Integer
                  .toString(TSFileDescriptor.getInstance().getConfig().getFloatPrecision()))));
      TSFileDescriptor.getInstance().getConfig().setTimeEncoder(properties
          .getProperty("time_encoder",
              TSFileDescriptor.getInstance().getConfig().getTimeEncoder()));
      TSFileDescriptor.getInstance().getConfig().setValueEncoder(properties
          .getProperty("value_encoder",
              TSFileDescriptor.getInstance().getConfig().getValueEncoder()));
      TSFileDescriptor.getInstance().getConfig().setCompressor(properties
          .getProperty("compressor", TSFileDescriptor.getInstance().getConfig().getCompressor()));

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
