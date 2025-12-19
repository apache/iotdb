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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.service.metrics.file.CompactionFileMetrics;
import org.apache.iotdb.db.service.metrics.file.ModsFileMetrics;
import org.apache.iotdb.db.service.metrics.file.ObjectFileMetrics;
import org.apache.iotdb.db.service.metrics.file.SystemRelatedFileMetrics;
import org.apache.iotdb.db.service.metrics.file.TsFileMetrics;
import org.apache.iotdb.db.service.metrics.file.WalFileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

import java.util.List;
import java.util.Map;

@SuppressWarnings("java:S6548") // do not warn about singleton class
public class FileMetrics implements IMetricSet {
  private static final TsFileMetrics TS_FILE_METRICS = new TsFileMetrics();
  private static final ModsFileMetrics MODS_FILE_METRICS = new ModsFileMetrics();
  private static final CompactionFileMetrics COMPACTION_FILE_METRICS = new CompactionFileMetrics();
  private static final WalFileMetrics WAL_FILE_METRICS = new WalFileMetrics();
  private static final SystemRelatedFileMetrics SYSTEM_RELATED_FILE_METRICS =
      new SystemRelatedFileMetrics();
  private static final ObjectFileMetrics OBJECT_FILE_METRICS = new ObjectFileMetrics();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    TS_FILE_METRICS.bindTo(metricService);
    MODS_FILE_METRICS.bindTo(metricService);
    COMPACTION_FILE_METRICS.bindTo(metricService);
    WAL_FILE_METRICS.bindTo(metricService);
    SYSTEM_RELATED_FILE_METRICS.bindTo(metricService);
    OBJECT_FILE_METRICS.bindTo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    TS_FILE_METRICS.unbindFrom(metricService);
    MODS_FILE_METRICS.unbindFrom(metricService);
    COMPACTION_FILE_METRICS.unbindFrom(metricService);
    WAL_FILE_METRICS.unbindFrom(metricService);
    SYSTEM_RELATED_FILE_METRICS.unbindFrom(metricService);
    OBJECT_FILE_METRICS.unbindFrom(metricService);
  }

  // region TsFile Related Metrics Update
  public void addTsFile(String database, String regionId, long size, boolean seq, String name) {
    TS_FILE_METRICS.addTsFile(database, regionId, size, seq, name);
  }

  public void deleteTsFile(boolean seq, List<TsFileResource> tsFileResourceList) {
    TS_FILE_METRICS.deleteFile(seq, tsFileResourceList);
  }

  public void deleteRegion(String database, String regionId) {
    TS_FILE_METRICS.deleteRegion(database, regionId);
  }

  // endregion

  // region Mods TsFile Related Metrics Update

  public void increaseModFileNum(int num) {
    MODS_FILE_METRICS.increaseModFileNum(num);
  }

  public void decreaseModFileNum(int num) {
    MODS_FILE_METRICS.decreaseModFileNum(num);
  }

  public void increaseModFileSize(long size) {
    MODS_FILE_METRICS.increaseModFileSize(size);
  }

  public void decreaseModFileSize(long size) {
    MODS_FILE_METRICS.decreaseModFileSize(size);
  }

  @TestOnly
  public int getModFileNum() {
    return MODS_FILE_METRICS.getModFileNum();
  }

  @TestOnly
  public long getModFileSize() {
    return MODS_FILE_METRICS.getModFileSize();
  }

  // endregion

  public void increaseObjectFileNum(int num) {
    OBJECT_FILE_METRICS.increaseObjectFileNum(num);
  }

  public void decreaseObjectFileNum(int num) {
    OBJECT_FILE_METRICS.decreaseObjectFileNum(num);
  }

  public void increaseObjectFileSize(long size) {
    OBJECT_FILE_METRICS.increaseObjectFileSize(size);
  }

  public void decreaseObjectFileSize(long size) {
    OBJECT_FILE_METRICS.decreaseObjectFileSize(size);
  }

  public Map<Integer, Long> getRegionSizeMap() {
    return TS_FILE_METRICS.getRegionSizeMap();
  }

  private static class FileMetricsInstanceHolder {
    private static final FileMetrics INSTANCE = new FileMetrics();

    private FileMetricsInstanceHolder() {
      // do nothing constructor
    }
  }

  public static FileMetrics getInstance() {
    return FileMetricsInstanceHolder.INSTANCE;
  }

  @TestOnly
  public long getFileCount(boolean seq) {
    return TS_FILE_METRICS.getFileCount(seq);
  }
}
