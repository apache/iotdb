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

package org.apache.iotdb.db.monitor.collector;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.MonitorConstants.FileSizeConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to collect some file size statistics.
 */
public class FileSize implements IStatistic {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(FileSize.class);
  private static final long ABNORMAL_VALUE = -1L;
  private static final long INIT_VALUE_IF_FILE_NOT_EXIST = 0L;

  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    long curTime = System.currentTimeMillis();
    TSRecord tsRecord = StatMonitor
        .convertToTSRecord(getStatParamsHashMap(), MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME,
            curTime);
    HashMap<String, TSRecord> ret = new HashMap<>();
    ret.put(MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME, tsRecord);
    return ret;
  }

  @Override
  public void registerStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (FileSizeConstants kind : FileSizeConstants.values()) {
      String seriesPath = MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME
          + MonitorConstants.MONITOR_PATH_SEPARATOR
          + kind.name();
      hashMap.put(seriesPath, MonitorConstants.DATA_TYPE_INT64);
    }
    StatMonitor.getInstance().registerStatStorageGroup(hashMap);
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (FileSizeConstants kind : MonitorConstants.FileSizeConstants.values()) {
      list.add(
          MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
              + kind.name());
    }
    return list;
  }

  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    Map<FileSizeConstants, Long> fileSizeMap = getFileSizesInByte();
    Map<String, AtomicLong> statParamsMap = new HashMap<>();
    for (FileSizeConstants kind : MonitorConstants.FileSizeConstants.values()) {
      statParamsMap.put(kind.name(), new AtomicLong(fileSizeMap.get(kind)));
    }
    return statParamsMap;
  }

  private static class FileSizeHolder {

    private static final FileSize INSTANCE = new FileSize();
  }

  private FileSize() {
    if (config.isEnableStatMonitor()) {
      StatMonitor statMonitor = StatMonitor.getInstance();
      registerStatMetadata();
      statMonitor.registerStatistics(MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME, this);
    }
  }

  public static FileSize getInstance() {
    return FileSizeHolder.INSTANCE;
  }

  /**
   * Return a map[FileSizeConstants, Long]. The key is the dir type and the value is the dir size in
   * byte.
   *
   * @return a map[FileSizeConstants, Long] with the dir type and the dir size in byte
   */
  public Map<FileSizeConstants, Long> getFileSizesInByte() {
    EnumMap<FileSizeConstants, Long> fileSizes = new EnumMap<>(FileSizeConstants.class);
    for (FileSizeConstants kinds : MonitorConstants.FileSizeConstants.values()) {

      if (kinds.equals(FileSizeConstants.SYS)) {
        fileSizes.put(kinds, collectSeqFileSize(fileSizes, kinds));
      } else {
        File file = SystemFileFactory.INSTANCE.getFile(kinds.getPath());
        if (file.exists()) {
          try {
            fileSizes.put(kinds, FileUtils.sizeOfDirectory(file));
          } catch (Exception e) {
            logger.error("Meet error while trying to get {} size with dir {} .", kinds,
                kinds.getPath(), e);
            fileSizes.put(kinds, ABNORMAL_VALUE);
          }
        } else {
          fileSizes.put(kinds, INIT_VALUE_IF_FILE_NOT_EXIST);
        }
      }
    }
    return fileSizes;
  }

  private long collectSeqFileSize(EnumMap<FileSizeConstants, Long> fileSizes,
      FileSizeConstants kinds) {
    long fileSize = INIT_VALUE_IF_FILE_NOT_EXIST;
    for (String sequenceDir : config.getDataDirs()) {
      if (sequenceDir.contains("unsequence")) {
        continue;
      }
      File settledFile = SystemFileFactory.INSTANCE.getFile(sequenceDir);
      if (settledFile.exists()) {
        try {
          fileSize += FileUtils.sizeOfDirectory(settledFile);
        } catch (Exception e) {
          logger.error("Meet error while trying to get {} size with dir {} .", kinds,
              sequenceDir, e);
          fileSizes.put(kinds, ABNORMAL_VALUE);
        }
      }
    }
    return fileSize;
  }
}
