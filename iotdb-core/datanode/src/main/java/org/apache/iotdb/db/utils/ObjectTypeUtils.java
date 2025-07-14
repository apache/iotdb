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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

public class ObjectTypeUtils {

  private static final Logger logger = LoggerFactory.getLogger(ObjectTypeUtils.class);
  private static final TierManager TIER_MANAGER = TierManager.getInstance();

  private ObjectTypeUtils() {}

  public static File getObjectPathFromBinary(Binary binary) {
    byte[] bytes = binary.getValues();
    String relativeObjectFilePath =
        new String(bytes, 8, bytes.length - 8, TSFileConfig.STRING_CHARSET);
    Optional<File> file = TIER_MANAGER.getAbsoluteObjectFilePath(relativeObjectFilePath);
    if (!file.isPresent()) {
      throw new ObjectFileNotExist(relativeObjectFilePath);
    }
    return file.get();
  }

  public static Optional<File> getNullableObjectPathFromBinary(
      Binary binary, boolean needTempFile) {
    byte[] bytes = binary.getValues();
    String relativeObjectFilePath =
        new String(bytes, 8, bytes.length - 8, TSFileConfig.STRING_CHARSET);
    return TIER_MANAGER.getAbsoluteObjectFilePath(relativeObjectFilePath, needTempFile);
  }

  public static void deleteObjectPathFromBinary(Binary binary) {
    Optional<File> file = ObjectTypeUtils.getNullableObjectPathFromBinary(binary, true);
    if (!file.isPresent()) {
      return;
    }
    File tmpFile = new File(file.get().getPath() + ".tmp");
    File bakFile = new File(file.get().getPath() + ".back");
    logger.info("Remove object file {}", file.get().getAbsolutePath());
    for (int i = 0; i < 2; i++) {
      try {
        if (Files.deleteIfExists(file.get().toPath())) {
          return;
        }
        if (Files.deleteIfExists(tmpFile.toPath())) {
          return;
        }
        if (Files.deleteIfExists(bakFile.toPath())) {
          return;
        }
      } catch (IOException e) {
        logger.error("Failed to remove object file {}", file.get().getAbsolutePath(), e);
      }
    }
  }
}
