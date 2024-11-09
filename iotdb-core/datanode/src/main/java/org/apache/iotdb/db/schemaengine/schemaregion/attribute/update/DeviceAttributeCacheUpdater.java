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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceCacheAttributeGuard;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeCommitUpdateNode;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

public class DeviceAttributeCacheUpdater {
  private static final Logger logger = LoggerFactory.getLogger(DeviceAttributeCacheUpdater.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final int UPDATE_DETAIL_CONTAINER_SEND_MIN_LIMIT_BYTES = 1024;

  // All the data node locations shall only have internal endpoint with all the other endpoints set
  // to null
  // Note that the locations in SR is also placed here in case the regions are migrated
  private final Set<TDataNodeLocation> targetDataNodeLocations = new HashSet<>();
  private final ConcurrentMap<TDataNodeLocation, UpdateContainer> attributeUpdateMap =
      new ConcurrentHashMap<>();
  private final AtomicLong version = new AtomicLong(0);

  // Volatiles
  private final MemSchemaRegionStatistics regionStatistics;
  private final String databaseName;

  // Only exist for update detail container
  private final Map<TDataNodeLocation, UpdateDetailContainerStatistics> updateContainerStatistics =
      new HashMap<>();

  public DeviceAttributeCacheUpdater(
      final MemSchemaRegionStatistics regionStatistics, final String databaseName) {
    this.regionStatistics = regionStatistics;
    this.databaseName = databaseName;
  }

  /////////////////////////////// Service ///////////////////////////////

  public void update(
      final String tableName, final String[] deviceId, final Map<String, Binary> attributeMap) {
    targetDataNodeLocations.forEach(
        location -> {
          // Skip update on local
          if (location.getDataNodeId() == config.getDataNodeId()) {
            // Remove potential entries put when the dataNodeId == -1
            removeLocation(location);
            return;
          }
          if (!attributeUpdateMap.containsKey(location)) {
            final UpdateContainer newContainer;
            if (!regionStatistics.isAllowToCreateNewSeries()) {
              newContainer = new UpdateClearContainer();
              requestMemory(UpdateClearContainer.INSTANCE_SIZE);
            } else {
              newContainer = new UpdateDetailContainer();
              requestMemory(UpdateDetailContainer.INSTANCE_SIZE);
              updateContainerStatistics.put(location, new UpdateDetailContainerStatistics());
            }
            attributeUpdateMap.put(location, newContainer);
          }
          final long size =
              attributeUpdateMap.get(location).updateAttribute(tableName, deviceId, attributeMap);
          updateContainerStatistics.computeIfPresent(
              location,
              (k, v) -> {
                v.addEntrySize(size);
                return v;
              });
          updateMemory(size);
        });
  }

  public void invalidate(final String tableName) {
    invalidate(container -> container.invalidate(tableName));
  }

  // root.database.table.[deviceNodes]
  public void invalidate(final String[] pathNodes) {
    invalidate(container -> container.invalidate(pathNodes));
  }

  public void invalidate(final String tableName, final String attributeName) {
    invalidate(container -> container.invalidate(tableName, attributeName));
  }

  private void invalidate(final ToLongFunction<UpdateContainer> updateFunction) {
    attributeUpdateMap.forEach(
        (location, container) -> {
          final long size = updateFunction.applyAsLong(container);
          releaseMemory(size);
          updateContainerStatistics.computeIfPresent(
              location,
              (k, v) -> {
                v.decreaseEntrySize(size);
                return v;
              });
        });
  }

  public Pair<Long, Map<TDataNodeLocation, byte[]>> getAttributeUpdateInfo(
      final @Nonnull AtomicInteger limit, final @Nonnull AtomicBoolean hasRemaining) {
    // Note that the "updateContainerStatistics" is unsafe to use here for whole read of detail
    // container because the update map is read by GRASS thread, and the container's size may change
    // during the read process
    final Map<TDataNodeLocation, byte[]> updateBytes = new HashMap<>();
    for (final Map.Entry<TDataNodeLocation, UpdateContainer> entry :
        attributeUpdateMap.entrySet()) {
      final TDataNodeLocation location = entry.getKey();
      final UpdateContainer container = entry.getValue();

      if (location.getDataNodeId() == config.getDataNodeId()) {
        // Remove when commit
        continue;
      }
      // If the remaining capacity is too low we just send clear container first
      // Because they require less capacity
      if (limit.get() < UPDATE_DETAIL_CONTAINER_SEND_MIN_LIMIT_BYTES
          && container instanceof UpdateDetailContainer) {
        hasRemaining.set(true);
        continue;
      }
      // type(1) + size(4)
      if (limit.get() <= 5) {
        hasRemaining.set(true);
        break;
      }
      limit.addAndGet(-5);
      updateBytes.put(location, container.getUpdateContent(limit, hasRemaining));
    }
    return new Pair<>(version.get(), updateBytes);
  }

  public void commit(final TableDeviceAttributeCommitUpdateNode node) {
    final Set<TDataNodeLocation> shrunkNodes = node.getShrunkNodes();
    targetDataNodeLocations.removeAll(shrunkNodes);
    shrunkNodes.forEach(this::removeLocation);

    if (version.get() == node.getVersion()) {
      removeLocation(node.getLeaderLocation());
    }

    node.getCommitMap()
        .forEach(
            ((location, bytes) ->
                attributeUpdateMap.computeIfPresent(
                    location,
                    (dataNode, container) -> {
                      if (container instanceof UpdateDetailContainer
                          || version.get() == node.getVersion()) {
                        final Pair<Long, Boolean> result =
                            container.updateSelfByCommitContainer(getContainer(bytes));
                        releaseMemory(result.getLeft());
                        // isEmpty
                        if (Boolean.TRUE.equals(result.getRight())) {
                          releaseMemory(
                              container instanceof UpdateDetailContainer
                                  ? UpdateDetailContainer.INSTANCE_SIZE
                                  : UpdateClearContainer.INSTANCE_SIZE);
                          updateContainerStatistics.remove(dataNode);
                          return null;
                        } else if (updateContainerStatistics.containsKey(dataNode)) {
                          updateContainerStatistics
                              .get(dataNode)
                              .decreaseEntrySize(result.getLeft());
                        }
                      }
                      return container;
                    })));
  }

  private void removeLocation(final TDataNodeLocation location) {
    if (attributeUpdateMap.containsKey(location)) {
      releaseMemory(
          updateContainerStatistics.containsKey(location)
              ? updateContainerStatistics.get(location).getContainerSize()
              : ((UpdateClearContainer) attributeUpdateMap.get(location)).ramBytesUsed());
      attributeUpdateMap.remove(location);
      updateContainerStatistics.remove(location);
    }
  }

  public static UpdateContainer getContainer(final byte[] bytes) {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    UpdateContainer result = null;
    try {
      result =
          ReadWriteIOUtils.readBool(inputStream)
              ? new UpdateDetailContainer()
              : new UpdateClearContainer();
      result.deserialize(inputStream);
    } catch (final IOException ignore) {
      // ByteArrayInputStream won't throw IOException
    }
    return result;
  }

  public boolean addLocation(final TDataNodeLocation dataNodeLocation) {
    return targetDataNodeLocations.add(dataNodeLocation);
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
      updateMemory(
          newContainer.ramBytesUsed() - updateContainerStatistics.get(location).getContainerSize());
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

    try {
      final FileOutputStream fileOutputStream = new FileOutputStream(snapshotTmp);
      final BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
      try {
        serialize(outputStream);
      } finally {
        outputStream.flush();
        fileOutputStream.getFD().sync();
        outputStream.close();
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
    ReadWriteIOUtils.write(version.get(), outputStream);

    ReadWriteIOUtils.write(targetDataNodeLocations.size(), outputStream);
    for (final TDataNodeLocation targetDataNodeLocation : targetDataNodeLocations) {
      serializeNodeLocation4AttributeUpdate(targetDataNodeLocation, outputStream);
    }

    ReadWriteIOUtils.write(attributeUpdateMap.size(), outputStream);
    for (final Map.Entry<TDataNodeLocation, UpdateContainer> entry :
        attributeUpdateMap.entrySet()) {
      serializeNodeLocation4AttributeUpdate(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
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
    version.set(ReadWriteIOUtils.readLong(inputStream));

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      targetDataNodeLocations.add(deserializeNodeLocationForAttributeUpdate(inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final TDataNodeLocation location = deserializeNodeLocationForAttributeUpdate(inputStream);
      final boolean isDetails = ReadWriteIOUtils.readBool(inputStream);
      final UpdateContainer container =
          isDetails ? new UpdateDetailContainer() : new UpdateClearContainer();
      container.deserialize(inputStream);

      // Update local cache for region migration
      if (config.getDataNodeId() == location.getDataNodeId()
          && config.getInternalAddress().equals(location.getInternalEndPoint().getIp())
          && config.getInternalPort() == location.getInternalEndPoint().getPort()) {
        final TableDeviceCacheAttributeGuard guard =
            TableDeviceSchemaFetcher.getInstance().getAttributeGuard();
        guard.setVersion(regionStatistics.getSchemaRegionId(), version.get());
        guard.handleContainer(databaseName, container);
      } else {
        attributeUpdateMap.put(location, container);
        if (isDetails) {
          updateContainerStatistics.put(location, new UpdateDetailContainerStatistics());
        }
      }
    }
  }

  public static void serializeNodeLocation4AttributeUpdate(
      final TDataNodeLocation location, final ByteBuffer buffer) {
    ReadWriteIOUtils.write(location.getDataNodeId(), buffer);
    ThriftCommonsSerDeUtils.serializeTEndPoint(location.getInternalEndPoint(), buffer);
  }

  public static void serializeNodeLocation4AttributeUpdate(
      final TDataNodeLocation location, final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(location.getDataNodeId(), stream);
    ThriftCommonsSerDeUtils.serializeTEndPoint(location.getInternalEndPoint(), stream);
  }

  public static TDataNodeLocation deserializeNodeLocationForAttributeUpdate(
      final ByteBuffer buffer) {
    return new TDataNodeLocation(
        ReadWriteIOUtils.readInt(buffer),
        null,
        ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer),
        null,
        null,
        null);
  }

  public static TDataNodeLocation deserializeNodeLocationForAttributeUpdate(
      final InputStream inputStream) throws IOException {
    return new TDataNodeLocation(
        ReadWriteIOUtils.readInt(inputStream),
        null,
        ThriftCommonsSerDeUtils.deserializeTEndPoint(inputStream),
        null,
        null,
        null);
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
