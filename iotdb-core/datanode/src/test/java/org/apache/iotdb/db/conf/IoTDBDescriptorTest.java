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

import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

public class IoTDBDescriptorTest {
  private final String confPath = System.getProperty(IoTDBConstant.IOTDB_CONF, null);

  @BeforeClass
  public static void init() {
    URL.setURLStreamHandlerFactory(
        new ConfigurableStreamHandlerFactory(
            "classpath",
            new URLStreamHandler() {
              @Override
              protected URLConnection openConnection(URL u) {
                // Well ... we actually don't even need to do anything ... just need to provide a
                // handler...
                throw new RuntimeException("This shouldn't have been called");
              }
            }));
  }

  @After
  public void clear() {
    if (confPath != null) {
      System.setProperty(IoTDBConstant.IOTDB_CONF, confPath);
    } else {
      System.clearProperty(IoTDBConstant.IOTDB_CONF);
    }
  }

  @Test
  public void testConfigURLWithFileProtocol() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();
    String pathString = "file:/usr/local/bin";

    System.setProperty(IoTDBConstant.IOTDB_CONF, pathString);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithClasspathProtocol() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();

    String pathString = "classpath:/root/path";
    System.setProperty(IoTDBConstant.IOTDB_CONF, pathString);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithPlainFilePath() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();
    URL path = IoTDBConfig.class.getResource("/" + IoTDBConfig.CONFIG_NAME);
    // filePath is a plain path string
    String filePath = path.getFile();
    System.setProperty(IoTDBConstant.IOTDB_CONF, filePath);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertEquals(confURL.toString(), path.toString());
  }

  @Test
  public void testInvalidWriteDeleteLoadAndMetadataValuesFallBack() throws Exception {
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    IoTDBConfig config = descriptor.getConfig();
    long originalSyncMlogPeriodInMs = config.getSyncMlogPeriodInMs();
    boolean originalEnablePartialInsert = config.isEnablePartialInsert();
    long originalCacheLastValuesMemoryBudgetInByte = config.getCacheLastValuesMemoryBudgetInByte();

    config.setSyncMlogPeriodInMs(101);
    config.setEnablePartialInsert(true);
    config.setCacheLastValuesMemoryBudgetInByte(103);

    TrimProperties properties = new TrimProperties();
    properties.setProperty("sync_mlog_period_in_ms", "invalid");
    properties.setProperty("enable_partial_insert", "invalid");
    properties.setProperty("cache_last_values_memory_budget_in_byte", "-1");

    try {
      descriptor.loadProperties(properties);

      Assert.assertEquals(101, config.getSyncMlogPeriodInMs());
      Assert.assertTrue(config.isEnablePartialInsert());
      Assert.assertEquals(103, config.getCacheLastValuesMemoryBudgetInByte());
    } finally {
      config.setSyncMlogPeriodInMs(originalSyncMlogPeriodInMs);
      config.setEnablePartialInsert(originalEnablePartialInsert);
      config.setCacheLastValuesMemoryBudgetInByte(originalCacheLastValuesMemoryBudgetInByte);
    }
  }

  @Test
  public void testInvalidWalWriteValuesFallBack() throws Exception {
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    IoTDBConfig config = descriptor.getConfig();
    long originalWalAsyncModeFsyncDelayInMs = config.getWalAsyncModeFsyncDelayInMs();
    long originalWalSyncModeFsyncDelayInMs = config.getWalSyncModeFsyncDelayInMs();
    long originalWalFileSizeThresholdInByte = config.getWalFileSizeThresholdInByte();
    double originalWalMinEffectiveInfoRatio = config.getWalMinEffectiveInfoRatio();
    long originalWalMemTableSnapshotThreshold = config.getWalMemTableSnapshotThreshold();
    int originalMaxWalMemTableSnapshotNum = config.getMaxWalMemTableSnapshotNum();
    long originalDeleteWalFilesPeriodInMs = config.getDeleteWalFilesPeriodInMs();

    config.setWalAsyncModeFsyncDelayInMs(101);
    config.setWalSyncModeFsyncDelayInMs(102);
    config.setWalFileSizeThresholdInByte(103);
    config.setWalMinEffectiveInfoRatio(0.2);
    config.setWalMemTableSnapshotThreshold(104);
    config.setMaxWalMemTableSnapshotNum(105);
    config.setDeleteWalFilesPeriodInMs(106);

    TrimProperties properties = new TrimProperties();
    properties.setProperty("wal_async_mode_fsync_delay_in_ms", "invalid");
    properties.setProperty("wal_sync_mode_fsync_delay_in_ms", "-1");
    properties.setProperty("wal_file_size_threshold_in_byte", "invalid");
    properties.setProperty("wal_min_effective_info_ratio", "2");
    properties.setProperty("wal_memtable_snapshot_threshold_in_byte", "-1");
    properties.setProperty("max_wal_memtable_snapshot_num", "0");
    properties.setProperty("delete_wal_files_period_in_ms", "-1");

    try {
      descriptor.loadProperties(properties);

      Assert.assertEquals(
          Long.parseLong(
              ConfigurationFileUtils.getConfigurationDefaultValue(
                  "wal_async_mode_fsync_delay_in_ms")),
          config.getWalAsyncModeFsyncDelayInMs());
      Assert.assertEquals(
          Long.parseLong(
              ConfigurationFileUtils.getConfigurationDefaultValue(
                  "wal_sync_mode_fsync_delay_in_ms")),
          config.getWalSyncModeFsyncDelayInMs());
      Assert.assertEquals(
          Long.parseLong(
              ConfigurationFileUtils.getConfigurationDefaultValue(
                  "wal_file_size_threshold_in_byte")),
          config.getWalFileSizeThresholdInByte());
      Assert.assertEquals(
          Double.parseDouble(
              ConfigurationFileUtils.getConfigurationDefaultValue("wal_min_effective_info_ratio")),
          config.getWalMinEffectiveInfoRatio(),
          0.0);
      Assert.assertEquals(
          Long.parseLong(
              ConfigurationFileUtils.getConfigurationDefaultValue(
                  "wal_memtable_snapshot_threshold_in_byte")),
          config.getWalMemTableSnapshotThreshold());
      Assert.assertEquals(
          Integer.parseInt(
              ConfigurationFileUtils.getConfigurationDefaultValue("max_wal_memtable_snapshot_num")),
          config.getMaxWalMemTableSnapshotNum());
      Assert.assertEquals(
          Long.parseLong(
              ConfigurationFileUtils.getConfigurationDefaultValue("delete_wal_files_period_in_ms")),
          config.getDeleteWalFilesPeriodInMs());
    } finally {
      config.setWalAsyncModeFsyncDelayInMs(originalWalAsyncModeFsyncDelayInMs);
      config.setWalSyncModeFsyncDelayInMs(originalWalSyncModeFsyncDelayInMs);
      config.setWalFileSizeThresholdInByte(originalWalFileSizeThresholdInByte);
      config.setWalMinEffectiveInfoRatio(originalWalMinEffectiveInfoRatio);
      config.setWalMemTableSnapshotThreshold(originalWalMemTableSnapshotThreshold);
      config.setMaxWalMemTableSnapshotNum(originalMaxWalMemTableSnapshotNum);
      config.setDeleteWalFilesPeriodInMs(originalDeleteWalFilesPeriodInMs);
    }
  }

  static class ConfigurableStreamHandlerFactory implements URLStreamHandlerFactory {
    private final Map<String, URLStreamHandler> protocolHandlers;

    public ConfigurableStreamHandlerFactory(String protocol, URLStreamHandler urlHandler) {
      protocolHandlers = new HashMap<>();
      addHandler(protocol, urlHandler);
    }

    public void addHandler(String protocol, URLStreamHandler urlHandler) {
      protocolHandlers.put(protocol, urlHandler);
    }

    public URLStreamHandler createURLStreamHandler(String protocol) {
      return protocolHandlers.get(protocol);
    }
  }
}
