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

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.TreeSet;

public class TsFileResourceManager {
  private static final Logger logger = LoggerFactory.getLogger(TsFileResourceManager.class);

  private static final DataNodeMemoryConfig MEMORY_CONFIG =
      IoTDBDescriptor.getInstance().getMemoryConfig();

  /** The memory block of time index */
  private final IMemoryBlock memoryBlock;

  // One registration and one TreeMap.Entry (key, value, parent, left, right and color) per file.
  static final long REGISTRATION_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(Registration.class)
          + RamUsageEstimator.alignObjectSize(
              RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                  + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                  + Byte.BYTES);

  private final TreeSet<Registration> sealedTsFileResources = new TreeSet<>();
  private long degradedTimeIndexNum;
  private long nextRegistrationId;
  private boolean degrading;

  /** The sort key and the charged bytes remain stable until the manager updates the entry. */
  public static final class Registration implements Comparable<Registration> {
    private final TsFileResource resource;
    private final long id;
    private long indexBytes;
    private File file;
    private long startTime;
    private boolean fileIndex;
    private long bytes;

    private Registration(TsFileResource resource, long id) {
      this.resource = resource;
      this.id = id;
      updatePriority(resource.getTimeIndex());
    }

    private void updatePriority(ITimeIndex index) {
      file = resource.getTsFile();
      startTime = index.getMinStartTime();
      fileIndex = index.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE;
    }

    private int comparePriority(Registration other) {
      int cmp = Boolean.compare(fileIndex, other.fileIndex);
      if (cmp == 0) {
        cmp = Long.compare(startTime, other.startTime);
      }
      return cmp != 0 ? cmp : file.getAbsolutePath().compareTo(other.file.getAbsolutePath());
    }

    @Override
    public int compareTo(Registration other) {
      int cmp = comparePriority(other);
      // Two previously distinct entries can acquire the same priority after degradation.
      return cmp == 0 ? Long.compare(id, other.id) : cmp;
    }

    public void refresh(boolean indexChanged) {
      TsFileResourceManager.getInstance().refresh(this, indexChanged);
    }
  }

  private TsFileResourceManager() {
    memoryBlock =
        MEMORY_CONFIG
            .getTimeIndexMemoryManager()
            .exactAllocate("TimeIndex", MemoryBlockType.DYNAMIC);
  }

  @TestOnly
  public void setTimeIndexMemoryThreshold(long timeIndexMemoryThreshold) {
    this.memoryBlock.setTotalMemorySizeInBytes(timeIndexMemoryThreshold);
  }

  public synchronized long getPriorityQueueSize() {
    return sealedTsFileResources.size();
  }

  public synchronized void registerSealedTsFileResource(TsFileResource resource) {
    if (resource.getMemoryRegistration() != null) {
      return;
    }
    Registration entry = new Registration(resource, ++nextRegistrationId);
    Registration previous = sealedTsFileResources.floor(entry);
    if (previous == null || entry.comparePriority(previous) != 0) {
      sealedTsFileResources.add(entry);
      if (entry.fileIndex) {
        degradedTimeIndexNum++;
      }
      // Publish before sampling so a concurrent metadata change cannot miss its charge.
      resource.setMemoryRegistration(entry);
      try {
        refresh(entry, true);
      } catch (RuntimeException | Error e) {
        removeTsFileResource(resource);
        throw e;
      }
    }
  }

  public synchronized void removeTsFileResource(TsFileResource resource) {
    Registration entry = resource.getMemoryRegistration();
    if (entry == null) {
      // Recovery can supply another resource describing the same file.
      if (sealedTsFileResources.isEmpty()) {
        return;
      }
      Registration key = new Registration(resource, Long.MIN_VALUE);
      entry = sealedTsFileResources.ceiling(key);
      if (entry == null || entry.comparePriority(key) != 0) {
        return;
      }
    }
    if (sealedTsFileResources.remove(entry)) {
      entry.resource.setMemoryRegistration(null);
      memoryBlock.release(entry.bytes);
      if (entry.fileIndex) {
        degradedTimeIndexNum--;
      }
    }
  }

  private synchronized void refresh(Registration entry, boolean indexChanged) {
    TsFileResource resource = entry.resource;
    if (resource.getMemoryRegistration() != entry) {
      return;
    }
    ITimeIndex index = resource.getTimeIndex();
    boolean fileIndex = index.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE;
    if (entry.fileIndex != fileIndex
        || entry.startTime != index.getMinStartTime()
        || entry.file != resource.getTsFile()) {
      sealedTsFileResources.remove(entry);
      if (entry.fileIndex != fileIndex) {
        degradedTimeIndexNum += fileIndex ? 1 : -1;
      }
      entry.updatePriority(index);
      sealedTsFileResources.add(entry);
    }
    if (indexChanged) {
      entry.indexBytes = index.calculateRamSize();
    }
    long bytes = REGISTRATION_RAM_BYTES + resource.calculateMetadataRamSize() + entry.indexBytes;
    long delta = bytes - entry.bytes;
    entry.bytes = bytes;
    if (delta > 0) {
      memoryBlock.forceAllocateWithoutLimitation(delta);
      if (!degrading
          && memoryBlock.getUsedMemoryInBytes() > memoryBlock.getTotalMemorySizeInBytes()) {
        degrading = true;
        try {
          chooseTsFileResourceToDegrade();
        } finally {
          degrading = false;
        }
      }
    } else if (delta < 0) {
      memoryBlock.release(-delta);
    }
  }

  public void forceDegradeTsFileResource(TsFileResource resource) {
    if (resource.replaceDeviceTimeIndexWithFileTimeIndex() != null) {
      logger.debug(StorageEngineMessages.FORCE_DEGRADE_TSFILE_RESOURCE, resource.getTsFilePath());
    }
  }

  private void chooseTsFileResourceToDegrade() {
    while (memoryBlock.getUsedMemoryInBytes() > memoryBlock.getTotalMemorySizeInBytes()) {
      Registration entry = sealedTsFileResources.first();
      if (entry.fileIndex) {
        logger.debug(StorageEngineMessages.CANNOT_DEGRADE_TIME_INDEX_ALL_FILE_LEVEL);
        return;
      }
      logger.debug(StorageEngineMessages.DEGRADE_TSFILE_RESOURCE, entry.file);
      if (entry.resource.replaceDeviceTimeIndexWithFileTimeIndex() == null) {
        // Another thread has already swapped the index and is waiting to publish its size.
        refresh(entry, true);
      }
    }
  }

  public synchronized long getDegradedTimeIndexNum() {
    return degradedTimeIndexNum;
  }

  public long getTimeIndexMemoryThreshold() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public long getTotalTimeIndexMemCost() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  /** function for clearing TsFileManager */
  @TestOnly
  public synchronized void clear() {
    sealedTsFileResources.forEach(entry -> entry.resource.setMemoryRegistration(null));
    this.sealedTsFileResources.clear();
    this.memoryBlock.setUsedMemoryInBytes(0);
    this.degradedTimeIndexNum = 0;
  }

  public static TsFileResourceManager getInstance() {
    return TsFileResourceManager.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final TsFileResourceManager INSTANCE = new TsFileResourceManager();
  }
}
