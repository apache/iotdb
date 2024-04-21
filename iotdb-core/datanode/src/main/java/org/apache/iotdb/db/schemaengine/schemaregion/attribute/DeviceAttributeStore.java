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
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.FileUtils;

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

public class DeviceAttributeStore implements IDeviceAttributeStore {

  private static final Logger logger = LoggerFactory.getLogger(DeviceAttributeStore.class);

  public List<Map<String, String>> deviceAttributeList = new ArrayList<>();

  @Override
  public void clear() {
    deviceAttributeList = new ArrayList<>();
  }

  @Override
  public synchronized boolean createSnapshot(File targetDir) {
    File snapshotTmp =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT_TMP);
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT);

    try {
      FileOutputStream fileOutputStream = new FileOutputStream(snapshotTmp);
      BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
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
    } catch (IOException e) {
      logger.error("Failed to create mtree snapshot due to {}", e.getMessage(), e);
      FileUtils.deleteFileIfExist(snapshot);
      return false;
    } finally {
      FileUtils.deleteFileIfExist(snapshotTmp);
    }
  }

  @Override
  public void loadFromSnapshot(File snapshotDir, String sgSchemaDirPath) throws IOException {
    try (BufferedInputStream inputStream =
        new BufferedInputStream(
            Files.newInputStream(
                SystemFileFactory.INSTANCE
                    .getFile(snapshotDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT)
                    .toPath()))) {
      deserialize(inputStream);
    } catch (IOException e) {
      logger.warn("Load device attribute snapshot from {} failed", snapshotDir);
      throw e;
    }
  }

  @Override
  public synchronized int createAttribute(List<String> nameList, List<String> valueList) {
    Map<String, String> attributeMap = new HashMap<>();
    for (int i = 0; i < nameList.size(); i++) {
      attributeMap.put(nameList.get(i), valueList.get(i));
    }
    deviceAttributeList.add(attributeMap);
    return deviceAttributeList.size() - 1;
  }

  @Override
  public void alterAttribute(int pointer, List<String> nameList, List<String> valueList) {
    Map<String, String> attributeMap = deviceAttributeList.get(pointer);
    for (int i = 0; i < nameList.size(); i++) {
      attributeMap.put(nameList.get(i), valueList.get(i));
    }
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
}
