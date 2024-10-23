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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileResourceManager {

  private final Map<Integer, PipeTsFileDataRegionResourceManager> dataRegion2ChildManagerMap =
      new ConcurrentHashMap<>();

  public PipeTsFileResourceManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeTsFileResourceManager#ttlCheck()",
            this::tryTtlCheck,
            Math.max(PipeTsFileResource.TSFILE_MIN_TIME_TO_LIVE_IN_MS / 1000, 1));
  }

  private void tryTtlCheck() {
    dataRegion2ChildManagerMap.values().forEach(PipeTsFileDataRegionResourceManager::tryTtlCheck);
  }

  /**
   * Given a file, create a hardlink or copy it to pipe dir, maintain a reference count for the
   * hardlink or copied file, and return the hardlink or copied file.
   *
   * <p>If the given file is already a hardlink or copied file, increase its reference count and
   * return it.
   *
   * <p>If the given file is a tsfile, create a hardlink in pipe dir, increase the reference count
   * of the hardlink and return it.
   *
   * <p>Otherwise, copy the file (.mod or .resource) to pipe dir, increase the reference count of
   * the copied file and return it.
   *
   * @param file tsfile, resource file or mod file. can be original file or hardlink/copy of
   *     original file
   * @param isTsFile {@code true} to create hardlink, {@code false} to copy file
   * @param tsFileResource the TsFileResource of original TsFile. Ignored if {@param isTsFile} is
   *     {@code false}.
   * @return the hardlink or copied file
   * @throws IOException when create hardlink or copy file failed
   */
  public File increaseFileReference(
      final File file,
      final boolean isTsFile,
      final TsFileResource tsFileResource,
      final int regionId)
      throws IOException {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .increaseFileReference(file, isTsFile, tsFileResource);
  }

  /**
   * Given a hardlink or copied file, decrease its reference count, if the reference count is 0,
   * delete the file. if the given file is not a hardlink or copied file, do nothing.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   */
  public void decreaseFileReference(final File hardlinkOrCopiedFile, final int regionId) {
    dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .decreaseFileReference(hardlinkOrCopiedFile);
  }

  /**
   * Get the reference count of the file.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @return the reference count of the file
   */
  public int getFileReferenceCount(final File hardlinkOrCopiedFile, final int regionId) {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .getFileReferenceCount(hardlinkOrCopiedFile);
  }

  /**
   * Cache maps of the TsFile for further use.
   *
   * @return {@code true} if the maps are successfully put into cache or already cached. {@code
   *     false} if they can not be cached.
   */
  public boolean cacheObjectsIfAbsent(final File hardlinkOrCopiedTsFile, final int regionId)
      throws IOException {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .cacheObjectsIfAbsent(hardlinkOrCopiedTsFile);
  }

  public Map<IDeviceID, List<String>> getDeviceMeasurementsMapFromCache(
      final File hardlinkOrCopiedTsFile, final int regionId) throws IOException {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .getDeviceMeasurementsMapFromCache(hardlinkOrCopiedTsFile);
  }

  public Map<IDeviceID, Boolean> getDeviceIsAlignedMapFromCache(
      final File hardlinkOrCopiedTsFile, final boolean cacheOtherMetadata, final int regionId)
      throws IOException {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .getDeviceIsAlignedMapFromCache(hardlinkOrCopiedTsFile, cacheOtherMetadata);
  }

  public Map<String, TSDataType> getMeasurementDataTypeMapFromCache(
      final File hardlinkOrCopiedTsFile, final int regionId) throws IOException {
    return dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .getMeasurementDataTypeMapFromCache(hardlinkOrCopiedTsFile);
  }

  public void pinTsFileResource(
      final TsFileResource resource, final boolean withMods, final int regionId)
      throws IOException {
    dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .pinTsFileResource(resource, withMods);
  }

  public void unpinTsFileResource(final TsFileResource resource, final int regionId)
      throws IOException {
    dataRegion2ChildManagerMap
        .computeIfAbsent(regionId, k -> new PipeTsFileDataRegionResourceManager())
        .unpinTsFileResource(resource);
  }

  public int getLinkedTsFileCount() {
    return dataRegion2ChildManagerMap.values().stream()
        .map(PipeTsFileDataRegionResourceManager::getLinkedTsfileCount)
        .reduce(0, Integer::sum);
  }

  /**
   * Get the total size of linked TsFiles whose original TsFile is deleted (by compaction or else)
   */
  public long getTotalLinkedButDeletedTsFileSize() {
    return dataRegion2ChildManagerMap.values().stream()
        .map(PipeTsFileDataRegionResourceManager::getTotalLinkedButDeletedTsfileSize)
        .reduce(0L, Long::sum);
  }
}
