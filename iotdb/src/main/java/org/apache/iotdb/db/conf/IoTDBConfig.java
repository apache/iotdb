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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfig.class);
  public static final String CONFIG_NAME = "iotdb-engine.properties";
  public static final String DEFAULT_DATA_DIR = "data";
  public static final String DEFAULT_SYS_DIR = "system";
  public static final String DEFAULT_TSFILE_DIR = "settled";
  public static final String MULT_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.conf.directories.strategy.";
  public static final String DEFAULT_MULT_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";

  private String rpcAddress = "0.0.0.0";
  /**
   * Port which the JDBC server listens to.
   */
  private int rpcPort = 6667;

  /**
   * Is the write ahead log enable.
   */
  private boolean enableWal = false;

  /**
   * When a certain amount of write ahead logs is reached, they will be flushed to the disk. It is
   * possible to lose at most flush_wal_threshold operations.
   */
  private int flushWalThreshold = 10000;

  /**
   * The cycle when write ahead logs are periodically refreshed to disk(in milliseconds). It is
   * possible to lose at most flush_wal_period_in_ms ms operations.
   */
  private long flushWalPeriodInMs = 10000;

  /**
   * The cycle when write ahead log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call outputStream.force(true) after every each write
   */
  private long forceWalPeriodInMs = 10;

  /**
   * Data directory.
   */
  private String dataDir = null;

  /**
   * System directory.
   */
  private String sysDir = null;

  /**
   * Wal directory.
   */
  private String walDir = null;

  /**
   * Data directory of Overflow data.
   */
  private String overflowDataDir = "overflow";

  /**
   * Data directory of fileNode data.
   */
  private String fileNodeDir = "info";

  /**
   * Data directory of bufferWrite data.
   * It can be setted as bufferWriteDirs = {"settled1", "settled2", "settled3"};
   */
  private String[] bufferWriteDirs = {DEFAULT_TSFILE_DIR};

  /**
   * Strategy of multiple directories.
   */
  private String multDirStrategyClassName = null;

  /**
   * Data directory of metadata data.
   */
  private String metadataDir = "schema";

  /**
   * Data directory of derby data.
   */
  private String derbyHome = "derby";

  /**
   * Data directory of Write ahead log folder.
   */
  private String walFolder = "wal";

  /**
   * Data directory for index files (KV-match indexes).
   */
  private String indexFileDir = "index";

  /**
   * Temporary directory for temporary files of read (External Sort). TODO: unused field
   */
  private String readTmpFileDir = "readTmp";

  /**
   * The maximum concurrent thread number for merging overflow. When the value <=0 or > CPU core
   * number, use the CPU core number.
   */
  private int mergeConcurrentThreads = Runtime.getRuntime().availableProcessors();

  /**
   * Maximum number of folders open at the same time.
   */
  private int maxOpenFolder = 100;

  /**
   * The amount of data that is read every time when IoTDB merges data.
   */
  private int fetchSize = 10000;

  /**
   * the maximum number of writing instances existing in same time.
   */
  @Deprecated
  private int writeInstanceThreshold = 5;

  /**
   * The period time of flushing data from memory to file. . The unit is second.
   */
  private long periodTimeForFlush = 3600;

  /**
   * The period time for merge overflow data with tsfile data. The unit is second.
   */
  private long periodTimeForMerge = 7200;

  /**
   * When set true, start timed flush and merge service. Else, stop timed flush and merge service.
   * The default value is true. TODO: 'timed' better explains this than 'timing'.
   */
  private boolean enableTimingCloseAndMerge = true;

  /**
   * How many threads can concurrently flush. When <= 0, use CPU core number.
   */
  private int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  private ZoneId zoneID = ZoneId.systemDefault();
  /**
   * BufferWriteProcessor and OverflowProcessor will immediately flush if this threshold is
   * reached.
   */
  private long memThresholdWarning = (long) (0.5 * Runtime.getRuntime().maxMemory());
  /**
   * No more insert is allowed if this threshold is reached.
   */
  private long memThresholdDangerous = (long) (0.6 * Runtime.getRuntime().maxMemory());
  /**
   * MemMonitorThread will check every such interval(in ms). If memThresholdWarning is reached,
   * MemMonitorThread will inform FileNodeManager to flush.
   */
  private long memMonitorInterval = 1000;
  /**
   * Decide how to control memory usage of inserting data. 0 is RecordMemController, which sums the
   * size of each record (tuple). 1 is JVMMemController, which uses the JVM heap memory as the
   * memory usage indicator.
   */
  private int memControllerType = 1;
  /**
   * When a bufferwrite's metadata size (in byte) exceed this, the bufferwrite is forced closed.
   */
  private long bufferwriteMetaSizeThreshold = 200 * 1024 * 1024L;
  /**
   * When a bufferwrite's file size (in byte) exceed this, the bufferwrite is forced closed.
   */
  private long bufferwriteFileSizeThreshold = 2 * 1024 * 1024 * 1024L;
  /**
   * When a overflow's metadata size (in byte) exceed this, the overflow is forced closed.
   */
  private long overflowMetaSizeThreshold = 20 * 1024 * 1024L;
  /**
   * When a overflow's file size (in byte) exceed this, the overflow is forced closed.
   */
  private long overflowFileSizeThreshold = 200 * 1024 * 1024L;
  /**
   * If set false, MemMonitorThread and MemStatisticThread will not be created.
   */
  private boolean enableMemMonitor = false;
  /**
   * When set to true, small flushes will be triggered periodically even if the memory threshold is
   * not exceeded.
   */
  private boolean enableSmallFlush = false;
  /**
   * The interval of small flush in ms.
   */
  private long smallFlushInterval = 60L * 1000;
  /**
   * The statMonitor writes statistics info into IoTDB every backLoopPeriodSec secs. The default
   * value is 5s.
   */
  private int backLoopPeriodSec = 5;
  /**
   * Set true to enable statistics monitor service, false to disable statistics service.
   */
  private boolean enableStatMonitor = false;
  /**
   * Set the time interval when StatMonitor performs delete detection. The default value is 600s.
   */
  private int statMonitorDetectFreqSec = 60 * 10;
  /**
   * Set the maximum time to keep monitor statistics information in IoTDB. The default value is
   * 600s.
   */
  private int statMonitorRetainIntervalSec = 60 * 10;
  /**
   * Threshold for external sort. When using multi-line merging sort, if the count of lines exceed
   * {@code externalSortThreshold}, it will trigger external sort.
   */
  private int externalSortThreshold = 50;
  /**
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  private int mManagerCacheSize = 400000;
  /**
   * The maximum size of a single log in byte. If a log exceeds this size, it cannot be written to
   * the WAL file and an exception is thrown.
   */
  private int maxLogEntrySize = 4 * 1024 * 1024;
  /**
   * Is this IoTDB instance a receiver of sync or not.
   */
  private boolean isSyncEnable = true;
  /**
   * If this IoTDB instance is a receiver of sync, set the server port.
   */
  private int syncServerPort = 5555;
  /*
   * Set the language version when loading file including error information, default value is "EN"
   */
  private String languageVersion = "EN";
  /**
   * Choose a postBack strategy of merging historical data: 1. It's more likely to update historical
   * data, choose "true". 2. It's more likely not to update historical data or you don't know
   * exactly, choose "false".
   */
  private boolean update_historical_data_possibility = false;
  private String ipWhiteList = "0.0.0.0/0";
  /**
   * Examining period of cache file reader : 100 seconds.
   */
  private long cacheFileReaderClearPeriod = 100000;

  public IoTDBConfig() {
    // empty constructor
  }

  public ZoneId getZoneID() {
    return zoneID;
  }

  public String getZoneIDString() {
    return getZoneID().toString();
  }

  public void updatePath() {
    confirmMultiDirStrategy();

    preUpdatePath();

    // update the paths of subdirectories in the dataDir
    if (getDataDir().length() > 0 && !getDataDir().endsWith(File.separator)) {
      setDataDir(getDataDir() + File.separatorChar);
    }
    setOverflowDataDir(getDataDir() + getOverflowDataDir());

    if (getBufferWriteDirs() == null || getBufferWriteDirs().length == 0) {
      setBufferWriteDirs(new String[]{DEFAULT_TSFILE_DIR});
    }
    for (int i = 0; i < getBufferWriteDirs().length; i++) {
      if (new File(getBufferWriteDirs()[i]).isAbsolute()) {
        continue;
      }

      getBufferWriteDirs()[i] = getDataDir() + getBufferWriteDirs()[i];
    }

    // update the paths of subdirectories in the sysDir
    if (getSysDir().length() > 0 && !getSysDir().endsWith(File.separator)) {
      setSysDir(getSysDir() + File.separatorChar);
    }
    setFileNodeDir(getSysDir() + getFileNodeDir());
    setMetadataDir(getSysDir() + getMetadataDir());

    // update the paths of subdirectories in the walDir
    if (getWalDir().length() > 0 && !getWalDir().endsWith(File.separator)) {
      setWalDir(getWalDir() + File.separatorChar);
    }
    setWalFolder(getWalDir() + getWalFolder());

    setDerbyHome(getSysDir() + getDerbyHome());
    setIndexFileDir(getDataDir() + getIndexFileDir());
  }

  /*
   * First, if dataDir is null, dataDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"data".
   * Then, if dataDir is absolute, leave dataDir as it is. If dataDir is relative, dataDir
   * will be converted to the complete version using non-empty %IOTDB_HOME%. e.g.
   * for windows platform, | IOTDB_HOME | dataDir before | dataDir
   * after | |-----------------|--------------------|---------------------------| |
   * D:\\iotdb\iotdb | null |
   * D:\\iotdb\iotdb\data\data | | D:\\iotdb\iotdb | dataDir | D:\\iotdb\iotdb\dataDir |
   * | D:\\iotdb\iotdb |
   * C:\\dataDir | C:\\dataDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   * First, if sysDir is null, sysDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"system".
   * Then, if sysDir is absolute, leave sysDir as it is. If sysDir is relative,
   * sysDir will be converted to the complete version using non-empty %IOTDB_HOME%.
   * e.g. for windows platform, | IOTDB_HOME | sysDir before | sysDir
   * after | |-----------------|--------------------|-----------------------------|
   * | D:\\iotdb\iotdb | null |D:\\iotdb\iotdb\data\system | | D:\\iotdb\iotdb | sysDir
   * | D:\\iotdb\iotdb\sysDir | | D:\\iotdb\iotdb |
   * C:\\sysDir | C:\\sysDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   * First, if walDir is null, walDir will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"data". Then,
   * if walDir is absolute, leave walDir as it is. If walDir is relative,
   * walDir will be converted to the complete
   * version using non-empty %IOTDB_HOME%. e.g. for windows platform,
   * | IOTDB_HOME | walDir before | walDir after |
   * |-----------------|--------------------|-----------------------------|
   * | D:\\iotdb\iotdb | null |
   * D:\\iotdb\iotdb\data\wal | | D:\\iotdb\iotdb | walDir | D:\\iotdb\iotdb\walDir |
   * | D:\\iotdb\iotdb | C:\\walDir |
   * C:\\walDir | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   */

  private void preUpdatePath() {
    if (getDataDir() == null) {
      setDataDir(DEFAULT_DATA_DIR + File.separatorChar + DEFAULT_DATA_DIR);
    }
    if (getSysDir() == null) {
      setSysDir(DEFAULT_DATA_DIR + File.separatorChar + DEFAULT_SYS_DIR);
    }
    if (getWalDir() == null) {
      setWalDir(DEFAULT_DATA_DIR);
    }

    List<String> dirs = new ArrayList<>();
    dirs.add(getDataDir());
    dirs.add(getSysDir());
    dirs.add(getWalDir());
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    for (int i = 0; i < 3; i++) {
      String dir = dirs.get(i);
      if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
        if (!homeDir.endsWith(File.separator)) {
          dir = homeDir + File.separatorChar + dir;
        } else {
          dir = homeDir + dir;
        }
        dirs.set(i, dir);
      }
    }
    setDataDir(dirs.get(0));
    setSysDir(dirs.get(1));
    setWalDir(dirs.get(2));
  }

  private void confirmMultiDirStrategy() {
    if (getMultDirStrategyClassName() == null) {
      setMultDirStrategyClassName(DEFAULT_MULT_DIR_STRATEGY);
    }
    if (!getMultDirStrategyClassName().contains(".")) {
      setMultDirStrategyClassName(MULT_DIR_STRATEGY_PREFIX + getMultDirStrategyClassName());
    }

    try {
      Class.forName(getMultDirStrategyClassName());
    } catch (ClassNotFoundException e) {
      LOGGER.warn("Cannot find given directory strategy {}, using the default value",
          getMultDirStrategyClassName(), e);
      setMultDirStrategyClassName(MULT_DIR_STRATEGY_PREFIX + DEFAULT_MULT_DIR_STRATEGY);
    }
  }

  public String[] getBufferWriteDirs() {
    return bufferWriteDirs;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  public void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public boolean isEnableWal() {
    return enableWal;
  }

  public void setEnableWal(boolean enableWal) {
    this.enableWal = enableWal;
  }

  public int getFlushWalThreshold() {
    return flushWalThreshold;
  }

  public void setFlushWalThreshold(int flushWalThreshold) {
    this.flushWalThreshold = flushWalThreshold;
  }

  public long getFlushWalPeriodInMs() {
    return flushWalPeriodInMs;
  }

  public void setFlushWalPeriodInMs(long flushWalPeriodInMs) {
    this.flushWalPeriodInMs = flushWalPeriodInMs;
  }

  public long getForceWalPeriodInMs() {
    return forceWalPeriodInMs;
  }

  public void setForceWalPeriodInMs(long forceWalPeriodInMs) {
    this.forceWalPeriodInMs = forceWalPeriodInMs;
  }

  public String getDataDir() {
    return dataDir;
  }

  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  public String getSysDir() {
    return sysDir;
  }

  public void setSysDir(String sysDir) {
    this.sysDir = sysDir;
  }

  public String getWalDir() {
    return walDir;
  }

  public void setWalDir(String walDir) {
    this.walDir = walDir;
  }

  public String getOverflowDataDir() {
    return overflowDataDir;
  }

  public void setOverflowDataDir(String overflowDataDir) {
    this.overflowDataDir = overflowDataDir;
  }

  public String getFileNodeDir() {
    return fileNodeDir;
  }

  public void setFileNodeDir(String fileNodeDir) {
    this.fileNodeDir = fileNodeDir;
  }

  public void setBufferWriteDirs(String[] bufferWriteDirs) {
    this.bufferWriteDirs = bufferWriteDirs;
  }

  public String getMultDirStrategyClassName() {
    return multDirStrategyClassName;
  }

  public void setMultDirStrategyClassName(String multDirStrategyClassName) {
    this.multDirStrategyClassName = multDirStrategyClassName;
  }

  public String getMetadataDir() {
    return metadataDir;
  }

  public void setMetadataDir(String metadataDir) {
    this.metadataDir = metadataDir;
  }

  public String getDerbyHome() {
    return derbyHome;
  }

  public void setDerbyHome(String derbyHome) {
    this.derbyHome = derbyHome;
  }

  public String getWalFolder() {
    return walFolder;
  }

  public void setWalFolder(String walFolder) {
    this.walFolder = walFolder;
  }

  public String getIndexFileDir() {
    return indexFileDir;
  }

  public void setIndexFileDir(String indexFileDir) {
    this.indexFileDir = indexFileDir;
  }

  public String getReadTmpFileDir() {
    return readTmpFileDir;
  }

  public void setReadTmpFileDir(String readTmpFileDir) {
    this.readTmpFileDir = readTmpFileDir;
  }

  public int getMergeConcurrentThreads() {
    return mergeConcurrentThreads;
  }

  public void setMergeConcurrentThreads(int mergeConcurrentThreads) {
    this.mergeConcurrentThreads = mergeConcurrentThreads;
  }

  public int getMaxOpenFolder() {
    return maxOpenFolder;
  }

  public void setMaxOpenFolder(int maxOpenFolder) {
    this.maxOpenFolder = maxOpenFolder;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public int getWriteInstanceThreshold() {
    return writeInstanceThreshold;
  }

  public void setWriteInstanceThreshold(int writeInstanceThreshold) {
    this.writeInstanceThreshold = writeInstanceThreshold;
  }

  public long getPeriodTimeForFlush() {
    return periodTimeForFlush;
  }

  public void setPeriodTimeForFlush(long periodTimeForFlush) {
    this.periodTimeForFlush = periodTimeForFlush;
  }

  public long getPeriodTimeForMerge() {
    return periodTimeForMerge;
  }

  public void setPeriodTimeForMerge(long periodTimeForMerge) {
    this.periodTimeForMerge = periodTimeForMerge;
  }

  public boolean isEnableTimingCloseAndMerge() {
    return enableTimingCloseAndMerge;
  }

  public void setEnableTimingCloseAndMerge(boolean enableTimingCloseAndMerge) {
    this.enableTimingCloseAndMerge = enableTimingCloseAndMerge;
  }

  public int getConcurrentFlushThread() {
    return concurrentFlushThread;
  }

  public void setConcurrentFlushThread(int concurrentFlushThread) {
    this.concurrentFlushThread = concurrentFlushThread;
  }

  public void setZoneID(ZoneId zoneID) {
    this.zoneID = zoneID;
  }

  public long getMemThresholdWarning() {
    return memThresholdWarning;
  }

  public void setMemThresholdWarning(long memThresholdWarning) {
    this.memThresholdWarning = memThresholdWarning;
  }

  public long getMemThresholdDangerous() {
    return memThresholdDangerous;
  }

  public void setMemThresholdDangerous(long memThresholdDangerous) {
    this.memThresholdDangerous = memThresholdDangerous;
  }

  public long getMemMonitorInterval() {
    return memMonitorInterval;
  }

  public void setMemMonitorInterval(long memMonitorInterval) {
    this.memMonitorInterval = memMonitorInterval;
  }

  public int getMemControllerType() {
    return memControllerType;
  }

  public void setMemControllerType(int memControllerType) {
    this.memControllerType = memControllerType;
  }

  public long getBufferwriteMetaSizeThreshold() {
    return bufferwriteMetaSizeThreshold;
  }

  public void setBufferwriteMetaSizeThreshold(long bufferwriteMetaSizeThreshold) {
    this.bufferwriteMetaSizeThreshold = bufferwriteMetaSizeThreshold;
  }

  public long getBufferwriteFileSizeThreshold() {
    return bufferwriteFileSizeThreshold;
  }

  public void setBufferwriteFileSizeThreshold(long bufferwriteFileSizeThreshold) {
    this.bufferwriteFileSizeThreshold = bufferwriteFileSizeThreshold;
  }

  public long getOverflowMetaSizeThreshold() {
    return overflowMetaSizeThreshold;
  }

  public void setOverflowMetaSizeThreshold(long overflowMetaSizeThreshold) {
    this.overflowMetaSizeThreshold = overflowMetaSizeThreshold;
  }

  public long getOverflowFileSizeThreshold() {
    return overflowFileSizeThreshold;
  }

  public void setOverflowFileSizeThreshold(long overflowFileSizeThreshold) {
    this.overflowFileSizeThreshold = overflowFileSizeThreshold;
  }

  public boolean isEnableMemMonitor() {
    return enableMemMonitor;
  }

  public void setEnableMemMonitor(boolean enableMemMonitor) {
    this.enableMemMonitor = enableMemMonitor;
  }

  public boolean isEnableSmallFlush() {
    return enableSmallFlush;
  }

  public void setEnableSmallFlush(boolean enableSmallFlush) {
    this.enableSmallFlush = enableSmallFlush;
  }

  public long getSmallFlushInterval() {
    return smallFlushInterval;
  }

  public void setSmallFlushInterval(long smallFlushInterval) {
    this.smallFlushInterval = smallFlushInterval;
  }

  public int getBackLoopPeriodSec() {
    return backLoopPeriodSec;
  }

  public void setBackLoopPeriodSec(int backLoopPeriodSec) {
    this.backLoopPeriodSec = backLoopPeriodSec;
  }

  public boolean isEnableStatMonitor() {
    return enableStatMonitor;
  }

  public void setEnableStatMonitor(boolean enableStatMonitor) {
    this.enableStatMonitor = enableStatMonitor;
  }

  public int getStatMonitorDetectFreqSec() {
    return statMonitorDetectFreqSec;
  }

  public void setStatMonitorDetectFreqSec(int statMonitorDetectFreqSec) {
    this.statMonitorDetectFreqSec = statMonitorDetectFreqSec;
  }

  public int getStatMonitorRetainIntervalSec() {
    return statMonitorRetainIntervalSec;
  }

  public void setStatMonitorRetainIntervalSec(int statMonitorRetainIntervalSec) {
    this.statMonitorRetainIntervalSec = statMonitorRetainIntervalSec;
  }

  public int getExternalSortThreshold() {
    return externalSortThreshold;
  }

  public void setExternalSortThreshold(int externalSortThreshold) {
    this.externalSortThreshold = externalSortThreshold;
  }

  public int getmManagerCacheSize() {
    return mManagerCacheSize;
  }

  public void setmManagerCacheSize(int mManagerCacheSize) {
    this.mManagerCacheSize = mManagerCacheSize;
  }

  public int getMaxLogEntrySize() {
    return maxLogEntrySize;
  }

  public void setMaxLogEntrySize(int maxLogEntrySize) {
    this.maxLogEntrySize = maxLogEntrySize;
  }

  public boolean isSyncEnable() {
    return isSyncEnable;
  }

  public void setSyncEnable(boolean syncEnable) {
    isSyncEnable = syncEnable;
  }

  public int getSyncServerPort() {
    return syncServerPort;
  }

  public void setSyncServerPort(int syncServerPort) {
    this.syncServerPort = syncServerPort;
  }

  public String getLanguageVersion() {
    return languageVersion;
  }

  public void setLanguageVersion(String languageVersion) {
    this.languageVersion = languageVersion;
  }

  public boolean isUpdate_historical_data_possibility() {
    return update_historical_data_possibility;
  }

  public void setUpdate_historical_data_possibility(boolean update_historical_data_possibility) {
    this.update_historical_data_possibility = update_historical_data_possibility;
  }

  public String getIpWhiteList() {
    return ipWhiteList;
  }

  public void setIpWhiteList(String ipWhiteList) {
    this.ipWhiteList = ipWhiteList;
  }

  public long getCacheFileReaderClearPeriod() {
    return cacheFileReaderClearPeriod;
  }

  public void setCacheFileReaderClearPeriod(long cacheFileReaderClearPeriod) {
    this.cacheFileReaderClearPeriod = cacheFileReaderClearPeriod;
  }
}
