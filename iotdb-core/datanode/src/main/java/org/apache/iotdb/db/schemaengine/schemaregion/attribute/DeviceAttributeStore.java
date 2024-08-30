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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.MemUsageUtil;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DeviceAttributeStore implements IDeviceAttributeStore {

  private static final Logger logger = LoggerFactory.getLogger(DeviceAttributeStore.class);

  public List<Map<String, String>> deviceAttributeList = new ArrayList<>();

  private final MemSchemaRegionStatistics regionStatistics;

  public DeviceAttributeStore(final MemSchemaRegionStatistics regionStatistics) {
    this.regionStatistics = regionStatistics;
  }

  @Override
  public void clear() {
    deviceAttributeList = new ArrayList<>();
  }

  @Override
  public synchronized boolean createSnapshot(final File targetDir) {
    final File snapshotTmp =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT_TMP);
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT);

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
            "Failed to delete old snapshot {} while creating device attribute snapshot.",
            snapshot.getName());
        return false;
      }
      if (!snapshotTmp.renameTo(snapshot)) {
        logger.error(
            "Failed to rename {} to {} while creating device attribute snapshot.",
            snapshotTmp.getName(),
            snapshot.getName());
        FileUtils.deleteFileIfExist(snapshot);
        return false;
      }

      return true;
    } catch (final IOException e) {
      logger.error("Failed to create mtree snapshot due to {}", e.getMessage(), e);
      FileUtils.deleteFileIfExist(snapshot);
      return false;
    } finally {
      FileUtils.deleteFileIfExist(snapshotTmp);
    }
  }

  @Override
  public void loadFromSnapshot(final File snapshotDir, final String sgSchemaDirPath)
      throws IOException {
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT);
    if (!snapshot.exists()) {
      logger.info(
          "Device attribute snapshot {} not found, consider it as upgraded from the older version, use empty attributes",
          snapshot);
      return;
    }
    try (final BufferedInputStream inputStream =
        new BufferedInputStream(Files.newInputStream(snapshot.toPath()))) {
      deserialize(inputStream);
    } catch (final IOException e) {
      logger.warn("Load device attribute snapshot from {} failed", snapshotDir);
      throw e;
    }
  }

  @Override
  public synchronized int createAttribute(final List<String> nameList, final Object[] valueList) {
    // todo implement storage for device of diverse data types
    long memUsage = 0L;
    final Map<String, String> attributeMap = new HashMap<>();
    String value;
    for (int i = 0; i < nameList.size(); i++) {
      value =
          valueList[i] == null
              ? null
              : ((Binary) valueList[i]).getStringValue(TSFileConfig.STRING_CHARSET);
      if (value != null) {
        attributeMap.put(nameList.get(i), value);
        memUsage += MemUsageUtil.computeKVMemUsageInMap(nameList.get(i), value);
      }
    }
    deviceAttributeList.add(attributeMap);
    requestMemory(memUsage);
    return deviceAttributeList.size() - 1;
  }

  @Override
  public Map<String, String> alterAttribute(
      final int pointer, final List<String> nameList, final Object[] valueList) {
    // todo implement storage for device of diverse data types
    long memUsageDelta = 0L;
    long originMemUsage;
    long updatedMemUsage;
    final Map<String, String> updateMap = new HashMap<>();
    final Map<String, String> attributeMap = deviceAttributeList.get(pointer);
    String value;
    for (int i = 0; i < nameList.size(); i++) {
      final String key = nameList.get(i);
      value = valueList[i] == null ? null : valueList[i].toString();

      originMemUsage =
          attributeMap.containsKey(key)
              ? MemUsageUtil.computeKVMemUsageInMap(key, attributeMap.get(key))
              : 0;
      if (value != null) {
        if (!Objects.equals(value, attributeMap.put(key, value))) {
          updateMap.put(key, value);
        }
        updatedMemUsage = MemUsageUtil.computeKVMemUsageInMap(key, value);
        memUsageDelta += (updatedMemUsage - originMemUsage);
      } else {
        if (Objects.nonNull(attributeMap.remove(key))) {
          updateMap.put(key, value);
        }
        memUsageDelta -= originMemUsage;
      }
    }
    if (memUsageDelta > 0) {
      requestMemory(memUsageDelta);
    } else if (memUsageDelta < 0) {
      releaseMemory(-memUsageDelta);
    }
    return updateMap;
  }

  @Override
  public String getAttribute(int pointer, String name) {
    return deviceAttributeList.get(pointer).get(name);
  }

  private void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(deviceAttributeList.size(), outputStream);
    for (Map<String, String> attributeMap : deviceAttributeList) {
      ReadWriteIOUtils.write(attributeMap, outputStream);
    }
  }

  private void deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      deviceAttributeList.add(ReadWriteIOUtils.readMap(inputStream));
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
