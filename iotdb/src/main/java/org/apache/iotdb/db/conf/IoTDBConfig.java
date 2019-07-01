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
import org.apache.iotdb.db.service.TSServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfig.class);
  public static final String CONFIG_NAME = "iotdb-engine.properties";
  private static final String DEFAULT_DATA_DIR = "data";
  private static final String DEFAULT_SYS_DIR = "system";
  static final String DEFAULT_SEQ_DATA_DIR = "sequence";
  static final String DEFAULT_UNSEQ_DATA_DIR = "unsequence";
  private static final String MULTI_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.conf.directories.strategy.";
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";

  private String rpcAddress = "0.0.0.0";
  /**
   * Port which the JDBC server listens to.
   */
  private int rpcPort = 6667;

  /**
   * Is the insert ahead log enable.
   */
  private boolean enableWal = true;

  /**
   * When a certain amount of insert ahead logs is reached, they will be flushed to the disk. It is
   * possible to lose at most flush_wal_threshold operations.
   */
  private int flushWalThreshold = 10000;

  /**
   * The cycle when insert ahead log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call outputStream.force(true) after every each insert
   */
  private long forceWalPeriodInMs = 10;

  /**
   * Size of log buffer in each log node(in byte).
   */
  private int walBufferSize = 16*1024*1024;

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
  private String walFolder = "wal";

  /**
   * Data directory of non-sequential data.
   */
  private String[] unseqDataDirs = {DEFAULT_UNSEQ_DATA_DIR};

  /**
   * Data directory of fileNode data.
   */
  private String fileNodeDir = "info";

  /**
   * Data directory of sequential data.
   * It can be settled as seqDataDirs = {"settled1", "settled2", "settled3"};
   */
  private String[] seqDataDirs = {DEFAULT_SEQ_DATA_DIR};

  /**
   * Strategy of multiple directories.
   */
  private String multiDirStrategyClassName = null;

  /**
   * Data directory of metadata data.
   */
  private String metadataDir = "meta";

  private int memtableNumber = 20;

  /**
   * Data directory for index files (KV-match indexes).
   */
  private String indexFileDir = "index";

  /**
   * The maximum concurrent thread number for merging overflow. When the value <=0 or > CPU core
   * number, use the CPU core number.
   */
  private int mergeConcurrentThreads = Runtime.getRuntime().availableProcessors();

  /**
   * The amount of data that is read every time when IoTDB merges data.
   */
  private int fetchSize = 10000;

  /**
   * How many threads can concurrently flush. When <= 0, use CPU core number.
   */
  private int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  private ZoneId zoneID = ZoneId.systemDefault();

  /**
   * When a TsFile's file size (in byte) exceed this, the TsFile is forced closed.
   */
  private long tsFileSizeThreshold = 2 * 1024 * 1024 * 1024L;

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
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  private int mManagerCacheSize = 400000;

  /**
   * Is this IoTDB instance a receiver of sync or not.
   */
  private boolean isSyncEnable = true;
  /**
   * If this IoTDB instance is a receiver of sync, set the server port.
   */
  private int syncServerPort = 5555;
  /**
   * Set the language version when loading file including error information, default value is "EN"
   */
  private String languageVersion = "EN";
  /**
   * Choose a postBack strategy of merging historical data: 1. It's more likely to update historical
   * data, choose "true". 2. It's more likely not to update historical data or you don't know
   * exactly, choose "false".
   */
  private boolean updateHistoricalDataPossibility = false;
  private String ipWhiteList = "0.0.0.0/0";
  /**
   * Examining period of cache file reader : 100 seconds.
   */
  private long cacheFileReaderClearPeriod = 100000;

  /**
   * Replace implementation class of JDBC service
   */
  private String rpcImplClassName = TSServiceImpl.class.getName();

  /**
   * whether use chunkBufferPool.
   */
  private boolean chunkBufferPoolEnable = false;

  public IoTDBConfig() {
    // empty constructor
  }

  public ZoneId getZoneID() {
    return zoneID;
  }

  void updatePath() {
    confirmMultiDirStrategy();

    preUpdatePath();

    // update the paths of subdirectories in the dataDir
    if (getDataDir().length() > 0 && !getDataDir().endsWith(File.separator)) {
      setDataDir(getDataDir() + File.separatorChar);
    }
    if (getUnseqDataDirs() == null || getUnseqDataDirs().length == 0) {
      setUnseqDataDirs(new String[]{DEFAULT_UNSEQ_DATA_DIR});
    }
    for (int i = 0; i < getUnseqDataDirs().length; i++) {
      if (new File(getUnseqDataDirs()[i]).isAbsolute()) {
        continue;
      }

      getUnseqDataDirs()[i] = getDataDir() + getUnseqDataDirs()[i];
    }

    if (getSeqDataDirs() == null || getSeqDataDirs().length == 0) {
      setSeqDataDirs(new String[]{DEFAULT_SEQ_DATA_DIR});
    }
    for (int i = 0; i < getSeqDataDirs().length; i++) {
      if (new File(getSeqDataDirs()[i]).isAbsolute()) {
        continue;
      }

      getSeqDataDirs()[i] = getDataDir() + getSeqDataDirs()[i];
    }

    // update the paths of subdirectories in the sysDir
    if (getSysDir().length() > 0 && !getSysDir().endsWith(File.separator)) {
      setSysDir(getSysDir() + File.separatorChar);
    }
    setFileNodeDir(getSysDir() + getFileNodeDir());
    setMetadataDir(getSysDir() + getMetadataDir());

    // update the paths of subdirectories in the walFolder
    if (getWalFolder().length() > 0 && !getWalFolder().endsWith(File.separator)) {
      setWalFolder(getWalFolder() + File.separatorChar);
    }
    setWalFolder(getWalFolder() + getWalFolder());

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
   * First, if walFolder is null, walFolder will be assigned the default
   * value(i.e.,"data"+File.separatorChar+"data". Then,
   * if walFolder is absolute, leave walFolder as it is. If walFolder is relative,
   * walFolder will be converted to the complete
   * version using non-empty %IOTDB_HOME%. e.g. for windows platform,
   * | IOTDB_HOME | walFolder before | walFolder after |
   * |-----------------|--------------------|-----------------------------|
   * | D:\\iotdb\iotdb | null |
   * D:\\iotdb\iotdb\data\wal | | D:\\iotdb\iotdb | walFolder | D:\\iotdb\iotdb\walFolder |
   * | D:\\iotdb\iotdb | C:\\walFolder |
   * C:\\walFolder | | D:\\iotdb\iotdb | "" | D:\\iotdb\iotdb\ |
   *
   */

  private void preUpdatePath() {
    if (getDataDir() == null) {
      setDataDir(DEFAULT_DATA_DIR + File.separatorChar + DEFAULT_DATA_DIR);
    }
    if (getSysDir() == null) {
      setSysDir(DEFAULT_DATA_DIR + File.separatorChar + DEFAULT_SYS_DIR);
    }
    if (getWalFolder() == null) {
      setWalFolder(DEFAULT_DATA_DIR);
    }

    List<String> dirs = new ArrayList<>();
    dirs.add(getDataDir());
    dirs.add(getSysDir());
    dirs.add(getWalFolder());
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
    setWalFolder(dirs.get(2));
  }

  private void confirmMultiDirStrategy() {
    if (getMultiDirStrategyClassName() == null) {
      setMultiDirStrategyClassName(DEFAULT_MULTI_DIR_STRATEGY);
    }
    if (!getMultiDirStrategyClassName().contains(".")) {
      setMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + getMultiDirStrategyClassName());
    }

    try {
      Class.forName(getMultiDirStrategyClassName());
    } catch (ClassNotFoundException e) {
      LOGGER.warn("Cannot find given directory strategy {}, using the default value",
          getMultiDirStrategyClassName(), e);
      setMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY);
    }
  }

  public String[] getSeqDataDirs() {
    return seqDataDirs;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  void setRpcPort(int rpcPort) {
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

  public long getForceWalPeriodInMs() {
    return forceWalPeriodInMs;
  }

  public void setForceWalPeriodInMs(long forceWalPeriodInMs) {
    this.forceWalPeriodInMs = forceWalPeriodInMs;
  }

  public String getDataDir() {
    return dataDir;
  }

  void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  String getSysDir() {
    return sysDir;
  }

  void setSysDir(String sysDir) {
    this.sysDir = sysDir;
  }

  public String getWalFolder() {
    return walFolder;
  }

  void setWalFolder(String walFolder) {
    this.walFolder = walFolder;
  }

  public String[] getUnseqDataDirs() {
    return unseqDataDirs;
  }

  void setUnseqDataDirs(String[] unseqDataDirs) {
    this.unseqDataDirs = unseqDataDirs;
  }

  public String getFileNodeDir() {
    return fileNodeDir;
  }

  private void setFileNodeDir(String fileNodeDir) {
    this.fileNodeDir = fileNodeDir;
  }

  void setSeqDataDirs(String[] seqDataDirs) {
    this.seqDataDirs = seqDataDirs;
  }

  public String getMultiDirStrategyClassName() {
    return multiDirStrategyClassName;
  }

  void setMultiDirStrategyClassName(String multiDirStrategyClassName) {
    this.multiDirStrategyClassName = multiDirStrategyClassName;
  }

  public String getMetadataDir() {
    return metadataDir;
  }

  private void setMetadataDir(String metadataDir) {
    this.metadataDir = metadataDir;
  }

  public String getIndexFileDir() {
    return indexFileDir;
  }

  private void setIndexFileDir(String indexFileDir) {
    this.indexFileDir = indexFileDir;
  }

  public int getMergeConcurrentThreads() {
    return mergeConcurrentThreads;
  }

  void setMergeConcurrentThreads(int mergeConcurrentThreads) {
    this.mergeConcurrentThreads = mergeConcurrentThreads;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public int getMemtableNumber() {
    return memtableNumber;
  }

  void setMemtableNumber(int memtableNumber) {
    this.memtableNumber = memtableNumber;
  }

  public int getConcurrentFlushThread() {
    return concurrentFlushThread;
  }

  void setConcurrentFlushThread(int concurrentFlushThread) {
    this.concurrentFlushThread = concurrentFlushThread;
  }

  void setZoneID(ZoneId zoneID) {
    this.zoneID = zoneID;
  }

  public long getTsFileSizeThreshold() {
    return tsFileSizeThreshold;
  }

  void setTsFileSizeThreshold(long tsFileSizeThreshold) {
    this.tsFileSizeThreshold = tsFileSizeThreshold;
  }

  public int getBackLoopPeriodSec() {
    return backLoopPeriodSec;
  }

  void setBackLoopPeriodSec(int backLoopPeriodSec) {
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

  void setStatMonitorDetectFreqSec(int statMonitorDetectFreqSec) {
    this.statMonitorDetectFreqSec = statMonitorDetectFreqSec;
  }

  public int getStatMonitorRetainIntervalSec() {
    return statMonitorRetainIntervalSec;
  }

  void setStatMonitorRetainIntervalSec(int statMonitorRetainIntervalSec) {
    this.statMonitorRetainIntervalSec = statMonitorRetainIntervalSec;
  }

  public int getmManagerCacheSize() {
    return mManagerCacheSize;
  }

  void setmManagerCacheSize(int mManagerCacheSize) {
    this.mManagerCacheSize = mManagerCacheSize;
  }

  public boolean isSyncEnable() {
    return isSyncEnable;
  }

  void setSyncEnable(boolean syncEnable) {
    isSyncEnable = syncEnable;
  }

  public int getSyncServerPort() {
    return syncServerPort;
  }

  void setSyncServerPort(int syncServerPort) {
    this.syncServerPort = syncServerPort;
  }

  public String getLanguageVersion() {
    return languageVersion;
  }

  void setLanguageVersion(String languageVersion) {
    this.languageVersion = languageVersion;
  }

  public boolean isUpdateHistoricalDataPossibility() {
    return updateHistoricalDataPossibility;
  }

  void setUpdateHistoricalDataPossibility(boolean updateHistoricalDataPossibility) {
    this.updateHistoricalDataPossibility = updateHistoricalDataPossibility;
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

  public String getRpcImplClassName() {
    return rpcImplClassName;
  }

  public void setRpcImplClassName(String rpcImplClassName) {
    this.rpcImplClassName = rpcImplClassName;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public boolean isChunkBufferPoolEnable() {
    return chunkBufferPoolEnable;
  }

  void setChunkBufferPoolEnable(boolean chunkBufferPoolEnable) {
    this.chunkBufferPoolEnable = chunkBufferPoolEnable;
  }
}
