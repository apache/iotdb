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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DeviceAttributeRemoteUpdater {
  private static final Logger logger = LoggerFactory.getLogger(DeviceAttributeRemoteUpdater.class);
  private static final int UPDATE_DETAIL_CONTAINER_SEND_MIN_LIMIT_BYTES = 1024;

  private final Set<TDataNodeLocation> targetDataNodeLocations = new HashSet<>();
  private final ConcurrentMap<TDataNodeLocation, UpdateContainer> attributeUpdateMap =
      new ConcurrentHashMap<>();
  private final AtomicLong version = new AtomicLong(0);

  // Volatiles
  private final MemSchemaRegionStatistics regionStatistics;

  // Only exist for update detail container
  private final Map<TDataNodeLocation, UpdateContainerStatistics> updateContainerStatistics =
      new HashMap<>();

  public DeviceAttributeRemoteUpdater(final MemSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
  }

  /////////////////////////////// Service ///////////////////////////////

  public void update(
      final String tableName, final String[] deviceId, final Map<String, String> attributeMap) {
    targetDataNodeLocations.forEach(
        location -> {
          if (!attributeUpdateMap.containsKey(location)) {
            final UpdateContainer newContainer;
            if (!regionStatistics.isAllowToCreateNewSeries()) {
              newContainer = new UpdateClearContainer();
              requestMemory(UpdateClearContainer.INSTANCE_SIZE);
            } else {
              newContainer = new UpdateDetailContainer();
              requestMemory(UpdateDetailContainer.INSTANCE_SIZE);
              updateContainerStatistics.put(location, new UpdateContainerStatistics());
            }
            attributeUpdateMap.put(location, newContainer);
          }
          final long size =
              attributeUpdateMap.get(location).updateAttribute(tableName, deviceId, attributeMap);
          updateContainerStatistics.computeIfPresent(
              location,
              (k, v) -> {
                v.addSize(size);
                return v;
              });
          updateMemory(size);
        });
  }

  public Pair<Long, Map<TDataNodeLocation, byte[]>> getAttributeUpdateInfo(
      final AtomicInteger limit) {
    // Note that the "updateContainerStatistics" is unsafe to use here for whole read of detail
    // container because the update map is read by GRASS thread, and the container's size may change
    // during the read process
    final Map<TDataNodeLocation, byte[]> updateBytes = new HashMap<>();
    for (final Map.Entry<TDataNodeLocation, UpdateContainer> entry :
        attributeUpdateMap.entrySet()) {
      // If the remaining capacity is too low we just send clear container first
      // Because they require less capacity
      if (limit.get() < UPDATE_DETAIL_CONTAINER_SEND_MIN_LIMIT_BYTES
          && entry.getValue() instanceof UpdateDetailContainer) {
        continue;
      }
      // type(1) + size(4)
      if (limit.get() <= 5) {
        break;
      }
      limit.addAndGet(-5);
      updateBytes.put(entry.getKey(), entry.getValue().getUpdateContent(limit));
    }
    return new Pair<>(version.get(), updateBytes);
  }

  public void addLocation(final TDataNodeLocation dataNodeLocation) {
    targetDataNodeLocations.add(dataNodeLocation);
  }

  public void afterUpdate() {
    version.incrementAndGet();
    degrade();
    GeneralRegionAttributeSecurityService.getInstance().notifyBroadCast();
  }

  private void degrade() {
    if (regionStatistics.isAllowToCreateNewSeries()) {
      return;
    }
    final TreeSet<TDataNodeLocation> degradeSet =
        new TreeSet<>(
            Comparator.comparingLong(v -> updateContainerStatistics.get(v).getDegradePriority())
                .reversed());
    updateContainerStatistics.forEach(
        (k, v) -> {
          if (v.needDegrade()) {
            degradeSet.add(k);
          }
        });
    for (final TDataNodeLocation location : degradeSet) {
      if (regionStatistics.isAllowToCreateNewSeries()) {
        return;
      }
      final UpdateClearContainer newContainer =
          ((UpdateDetailContainer) attributeUpdateMap.get(location)).degrade();
      updateMemory(newContainer.ramBytesUsed() - updateContainerStatistics.get(location).getSize());
      attributeUpdateMap.put(location, newContainer);
      updateContainerStatistics.remove(location);
    }
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  public synchronized boolean createSnapshot(final File targetDir) {
    final File snapshotTmp =
        SystemFileFactory.INSTANCE.getFile(
            targetDir, SchemaConstant.DEVICE_ATTRIBUTE_REMOTE_UPDATER_SNAPSHOT_TMP);
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            targetDir, SchemaConstant.DEVICE_ATTRIBUTE_REMOTE_UPDATER_SNAPSHOT);

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotTmp);
        final BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      try {
        serialize(outputStream);
      } finally {
        outputStream.flush();
        fileOutputStream.getFD().sync();
      }
      if (snapshot.exists() && !FileUtils.deleteFileIfExist(snapshot)) {
        logger.error(
            "Failed to delete old snapshot {} while creating device attribute remote updater snapshot.",
            snapshot.getName());
        return false;
      }
      if (!snapshotTmp.renameTo(snapshot)) {
        logger.error(
            "Failed to rename {} to {} while creating device attribute remote updater snapshot.",
            snapshotTmp.getName(),
            snapshot.getName());
        FileUtils.deleteFileIfExist(snapshot);
        return false;
      }

      return true;
    } catch (final IOException e) {
      logger.error(
          "Failed to create device attribute remote updater snapshot due to {}", e.getMessage(), e);
      FileUtils.deleteFileIfExist(snapshot);
      return false;
    } finally {
      FileUtils.deleteFileIfExist(snapshotTmp);
    }
  }

  private void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(targetDataNodeLocations.size(), outputStream);
    for (final TDataNodeLocation targetDataNodeLocation : targetDataNodeLocations) {
      ReadWriteIOUtils.write(targetDataNodeLocation.getDataNodeId(), outputStream);
      ThriftCommonsSerDeUtils.serializeTEndPoint(
          targetDataNodeLocation.getInternalEndPoint(), outputStream);
    }

    ReadWriteIOUtils.write(attributeUpdateMap.size(), outputStream);
    for (final Map.Entry<TDataNodeLocation, UpdateContainer> entry :
        attributeUpdateMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getDataNodeId(), outputStream);
      ThriftCommonsSerDeUtils.serializeTEndPoint(
          entry.getKey().getInternalEndPoint(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    ReadWriteIOUtils.write(version.get(), outputStream);
  }

  public void loadFromSnapshot(final File snapshotDir) throws IOException {
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            snapshotDir, SchemaConstant.DEVICE_ATTRIBUTE_REMOTE_UPDATER_SNAPSHOT);
    if (!snapshot.exists()) {
      logger.info(
          "Device attribute remote updater snapshot {} not found, consider it as upgraded from the older version, will not update remote",
          snapshot);
      return;
    }
    try (final BufferedInputStream inputStream =
        new BufferedInputStream(Files.newInputStream(snapshot.toPath()))) {
      deserialize(inputStream);
    } catch (final Exception e) {
      logger.warn(
          "Load device attribute remote updater snapshot from {} failed, continue...", snapshotDir);
    }
  }

  private void deserialize(final InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      targetDataNodeLocations.add(
          new TDataNodeLocation(
              ReadWriteIOUtils.readInt(inputStream),
              null,
              ThriftCommonsSerDeUtils.deserializeTEndPoint(inputStream),
              null,
              null,
              null));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final TDataNodeLocation location =
          new TDataNodeLocation(
              ReadWriteIOUtils.readInt(inputStream),
              null,
              ThriftCommonsSerDeUtils.deserializeTEndPoint(inputStream),
              null,
              null,
              null);
      final UpdateContainer container =
          ReadWriteIOUtils.readBool(inputStream)
              ? new UpdateDetailContainer()
              : new UpdateClearContainer();
      container.deserialize(inputStream);
      attributeUpdateMap.put(location, container);
    }

    version.set(ReadWriteIOUtils.readLong(inputStream));
  }

  /////////////////////////////// Memory ///////////////////////////////

  private void updateMemory(final long size) {
    if (size > 0) {
      requestMemory(size);
    } else {
      releaseMemory(size);
    }
  }

  private void requestMemory(final long size) {
    if (regionStatistics != null) {
      regionStatistics.requestMemory(size);
    }
  }

  private void releaseMemory(final long size) {
    if (regionStatistics != null) {
      regionStatistics.releaseMemory(size);
    }
  }
}
