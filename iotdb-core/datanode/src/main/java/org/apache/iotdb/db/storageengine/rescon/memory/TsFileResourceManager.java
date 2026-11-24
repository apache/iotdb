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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.utils.MemUtils;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class TsFileResourceManager {
  private static final Logger logger = LoggerFactory.getLogger(TsFileResourceManager.class);

  private static final DataNodeMemoryConfig MEMORY_CONFIG =
      IoTDBDescriptor.getInstance().getMemoryConfig();

  /** The memory block of time index */
  private final IMemoryBlock memoryBlock;

  // Registration, its priority-tree node, and the path map's node and amortized bucket slots.
  static final long REGISTRATION_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(Registration.class)
          + RamUsageEstimator.alignObjectSize(
              RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                  + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                  + Byte.BYTES)
          + RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY
          + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

  private final Map<String, Registration> registrationsByPath = new HashMap<>();
  private final TreeSet<Registration> sealedTsFileResources =
      new TreeSet<>(Registration::comparePriority);
  private long degradedTimeIndexNum;
  private boolean degrading;

  /**
   * After publication the manager monitor protects the priority key, measured index and charged
   * bytes.
   */
  public static final class Registration {
    private final TsFileResource resource;
    private File file;
    private String path;
    private long startTime;
    private ITimeIndex index;
    private long indexBytes;
    private long bytes;

    private Registration(TsFileResource resource, File file, String path) {
      this.resource = resource;
      this.file = file;
      this.path = path;
      index = resource.getTimeIndex();
      startTime = index == null ? Long.MAX_VALUE : index.getMinStartTime();
    }

    private boolean hasFileIndex() {
      return index != null && index.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE;
    }

    private boolean canDegrade() {
      return index != null && !hasFileIndex();
    }

    private int comparePriority(Registration other) {
      int cmp = Boolean.compare(other.canDegrade(), canDegrade());
      if (cmp == 0) {
        cmp = Long.compare(startTime, other.startTime);
      }
      return cmp == 0 ? path.compareTo(other.path) : cmp;
    }

    public long getIndexRamSize(ITimeIndex candidate) {
      synchronized (TsFileResourceManager.getInstance()) {
        if (candidate == index) {
          return indexBytes;
        }
      }
      // A compaction may own a rebuilt index or still hold the index from before degradation.
      return candidate.calculateRamSize();
    }

    public void refresh(boolean indexChanged) {
      TsFileResourceManager.getInstance().refresh(this, indexChanged);
    }

    private void detach() {
      resource.setMemoryRegistration(null);
      index = null;
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

  // Device IDs are appended to a live index, and close() can trim its time arrays in place.
  private record DeviceIndexShape(
      ArrayDeviceTimeIndex index, int deviceCount, int startCapacity, int endCapacity) {
    private DeviceIndexShape(ArrayDeviceTimeIndex index) {
      this(
          index,
          index.getDeviceToIndex().size(),
          index.getStartTimes().length,
          index.getEndTimes().length);
    }

    private boolean changed() {
      return deviceCount != index.getDeviceToIndex().size()
          || startCapacity != index.getStartTimes().length
          || endCapacity != index.getEndTimes().length;
    }
  }

  public void registerSealedTsFileResource(TsFileResource resource) {
    if (resource.getMemoryRegistration() != null) {
      return;
    }
    Registration entry;
    synchronized (this) {
      File file = resource.getTsFile();
      String path = file.getAbsolutePath();
      if (registrationsByPath.containsKey(path)) {
        return;
      }
      entry = new Registration(resource, file, path);
    }

    // Measure device IDs while the entry is private, outside the monitor shared with queries.
    DeviceIndexShape shape =
        entry.index instanceof ArrayDeviceTimeIndex deviceIndex
            ? new DeviceIndexShape(deviceIndex)
            : null;
    entry.indexBytes = entry.index == null ? 0 : entry.index.calculateRamSize();
    publishRegistration(entry, shape);
  }

  private synchronized void publishRegistration(Registration entry, DeviceIndexShape shape) {
    TsFileResource resource = entry.resource;
    if (resource.getMemoryRegistration() != null) {
      return;
    }
    File file = resource.getTsFile();
    if (entry.file != file) {
      entry.file = file;
      entry.path = file.getAbsolutePath();
    }
    if (registrationsByPath.putIfAbsent(entry.path, entry) != null) {
      return;
    }
    sealedTsFileResources.add(entry);
    if (entry.hasFileIndex()) {
      degradedTimeIndexNum++;
    }
    // Publish before validating the sample so subsequent mutations deliver their own refresh.
    resource.setMemoryRegistration(entry);
    boolean charged = false;
    try {
      // refresh also remeasures if the resource now holds a different index object.
      refresh(entry, shape != null && shape.changed());
      charged = true;
    } finally {
      if (!charged) {
        removeRegistration(entry);
      }
    }
  }

  public synchronized void removeTsFileResource(TsFileResource resource) {
    Registration entry = resource.getMemoryRegistration();
    if (entry == null) {
      // Compaction recovery may only know the path, with an empty or different time index.
      File file = resource.getTsFile();
      entry = file == null ? null : registrationsByPath.get(file.getAbsolutePath());
    }
    if (entry != null) {
      removeRegistration(entry);
    }
  }

  private void removeRegistration(Registration entry) {
    if (registrationsByPath.remove(entry.path, entry)) {
      sealedTsFileResources.remove(entry);
      if (entry.hasFileIndex()) {
        degradedTimeIndexNum--;
      }
      entry.detach();
      memoryBlock.release(entry.bytes);
    }
  }

  private boolean updatePriority(Registration entry, ITimeIndex index, long startTime, File file) {
    String path = entry.path;
    if (file != entry.file) {
      path = file.getAbsolutePath();
      Registration owner = registrationsByPath.get(path);
      if (owner != null && owner != entry) {
        // As on registration, keep the existing owner when two resources name the same file.
        removeRegistration(entry);
        return false;
      }
      registrationsByPath.remove(entry.path);
      registrationsByPath.put(path, entry);
    }
    sealedTsFileResources.remove(entry);
    if (entry.hasFileIndex()) {
      degradedTimeIndexNum--;
    }
    entry.file = file;
    entry.path = path;
    entry.startTime = startTime;
    entry.index = index;
    if (entry.hasFileIndex()) {
      degradedTimeIndexNum++;
    }
    sealedTsFileResources.add(entry);
    return true;
  }

  private synchronized void refresh(Registration entry, boolean indexChanged) {
    TsFileResource resource = entry.resource;
    if (resource.getMemoryRegistration() != entry) {
      return;
    }
    ITimeIndex index = resource.getTimeIndex();
    long startTime = index == null ? Long.MAX_VALUE : index.getMinStartTime();
    File file = resource.getTsFile();
    boolean replaced = index != entry.index;
    // A metadata callback can observe an index swap before the swapping thread takes this monitor.
    long indexBytes = entry.indexBytes;
    if (replaced || indexChanged) {
      indexBytes = index == null ? 0 : index.calculateRamSize();
    }
    if ((replaced || startTime != entry.startTime || file != entry.file)
        && !updatePriority(entry, index, startTime, file)) {
      return;
    }
    entry.indexBytes = indexBytes;
    // A relative File retains its own path string in addition to the map's absolute key.
    long pathBytes = file.isAbsolute() ? 0 : MemUtils.getFilePathRamBytes(entry.path);
    long bytes =
        REGISTRATION_RAM_BYTES + pathBytes + resource.calculateMetadataRamSize() + indexBytes;
    long delta = bytes - entry.bytes;
    entry.bytes = bytes;
    adjustMemoryUsage(delta);
  }

  private void adjustMemoryUsage(long delta) {
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
      if (!entry.canDegrade()) {
        logger.debug(StorageEngineMessages.CANNOT_DEGRADE_TIME_INDEX_ALL_FILE_LEVEL);
        return;
      }
      if (entry.resource.getMemoryRegistration() != entry) {
        // Its degradation callback cannot release this entry's charge.
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
    registrationsByPath.values().forEach(Registration::detach);
    registrationsByPath.clear();
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
