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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RSchemaConfLoader {

  private int maxBackgroundJobs = 10;
  private int blockSizeDeviation = 5;
  private int blockRestartInterval = 10;
  private int maxWriteBufferNumber = 6;

  private double bloomFilterPolicy = 64;

  private long blockSize = 4 * SizeUnit.KB;
  private long writeBufferSize = 64 * SizeUnit.KB;
  private long maxTotalWalSize = 64 * SizeUnit.KB;
  private long blockCache =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion() * 2 / 3;
  private long blockCacheCompressed =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion() / 3;

  private static final String ROCKSDB_CONFIG_FILE_NAME = "schema-rocksdb.properties";
  private static final Logger logger = LoggerFactory.getLogger(RSchemaConfLoader.class);

  public RSchemaConfLoader() {
    loadProperties();
  }

  private void loadProperties() {
    String iotdbHomePath = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    String rocksdbConfigPath =
        iotdbHomePath + File.separatorChar + "conf" + File.separatorChar + ROCKSDB_CONFIG_FILE_NAME;
    try (InputStream in = new BufferedInputStream(new FileInputStream(rocksdbConfigPath))) {
      Properties properties = new Properties();
      properties.load(in);
      setBlockCache(
          Long.parseLong(properties.getProperty("block_cache_size", Long.toString(blockCache))));
      setBlockCacheCompressed(
          Long.parseLong(
              properties.getProperty(
                  "block_cache_compressed_size", Long.toString(blockCacheCompressed))));
      setBlockSize(Long.parseLong(properties.getProperty("block_size", Long.toString(blockSize))));
      setWriteBufferSize(
          Long.parseLong(
              properties.getProperty("write_buffer_size", Long.toString(writeBufferSize))));
      setMaxTotalWalSize(
          Long.parseLong(
              properties.getProperty("max_total_wal_size", Long.toString(maxTotalWalSize))));
      setMaxBackgroundJobs(
          Integer.parseInt(
              properties.getProperty(
                  "max_background_job_num", Integer.toString(maxBackgroundJobs))));
      setBlockSizeDeviation(
          Integer.parseInt(
              properties.getProperty(
                  "block_size_deviation", Integer.toString(blockSizeDeviation))));
      setBlockRestartInterval(
          Integer.parseInt(
              properties.getProperty(
                  "block_restart_interval", Integer.toString(blockRestartInterval))));
      setMaxWriteBufferNumber(
          Integer.parseInt(
              properties.getProperty(
                  "max_write_buffer_num", Integer.toString(maxWriteBufferNumber))));
      setBloomFilterPolicy(
          Double.parseDouble(
              properties.getProperty("bloom_filter_policy", Double.toString(bloomFilterPolicy))));
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find rocksdb config file {}", rocksdbConfigPath, e);
    } catch (IOException e) {
      logger.warn("Cannot load rocksdb config file, use default configuration", e);
    }
  }

  public long getBlockCache() {
    return blockCache;
  }

  private void setBlockCache(long blockCache) {
    this.blockCache = blockCache;
  }

  public long getBlockCacheCompressed() {
    return blockCacheCompressed;
  }

  private void setBlockCacheCompressed(long blockCacheCompressed) {
    this.blockCacheCompressed = blockCacheCompressed;
  }

  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  private void setWriteBufferSize(long writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
  }

  public long getMaxTotalWalSize() {
    return maxTotalWalSize;
  }

  private void setMaxTotalWalSize(long maxTotalWalSize) {
    this.maxTotalWalSize = maxTotalWalSize;
  }

  public int getMaxBackgroundJobs() {
    return maxBackgroundJobs;
  }

  private void setMaxBackgroundJobs(int maxBackgroundJobs) {
    this.maxBackgroundJobs = maxBackgroundJobs;
  }

  public double getBloomFilterPolicy() {
    return bloomFilterPolicy;
  }

  private void setBloomFilterPolicy(double bloomFilterPolicy) {
    this.bloomFilterPolicy = bloomFilterPolicy;
  }

  public int getBlockSizeDeviation() {
    return blockSizeDeviation;
  }

  private void setBlockSizeDeviation(int blockSizeDeviation) {
    this.blockSizeDeviation = blockSizeDeviation;
  }

  public int getBlockRestartInterval() {
    return blockRestartInterval;
  }

  private void setBlockRestartInterval(int blockRestartInterval) {
    this.blockRestartInterval = blockRestartInterval;
  }

  public int getMaxWriteBufferNumber() {
    return maxWriteBufferNumber;
  }

  private void setMaxWriteBufferNumber(int maxWriteBufferNumber) {
    this.maxWriteBufferNumber = maxWriteBufferNumber;
  }

  public long getBlockSize() {
    return blockSize;
  }

  private void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
  }
}
