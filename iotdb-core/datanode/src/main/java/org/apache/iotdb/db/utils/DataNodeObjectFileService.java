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

import org.apache.iotdb.calc.utils.IObjectFileService;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;

public class DataNodeObjectFileService implements IObjectFileService {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeObjectFileService.class);
  private static final TierManager TIER_MANAGER = TierManager.getInstance();
  public static final DataNodeObjectFileService INSTANCE = new DataNodeObjectFileService();

  private DataNodeObjectFileService() {}

  @Override
  public ByteBuffer readObjectContent(
      String relativePath, long offset, int readSize, boolean mayNotInCurrentNode) {
    Optional<File> objectFile = TIER_MANAGER.getAbsoluteObjectFilePath(relativePath, false);
    if (objectFile.isPresent()) {
      return readObjectContentFromLocalFile(objectFile.get(), offset, readSize);
    }
    if (mayNotInCurrentNode) {
      return readObjectContentFromRemoteFile(relativePath, offset, readSize);
    }
    throw new ObjectFileNotExist(relativePath);
  }

  @Override
  public Optional<File> getObjectPathFromBinary(Binary binary, boolean needTempFile) {
    String relativeObjectFilePath =
        ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary).getRight();
    return TIER_MANAGER.getAbsoluteObjectFilePath(relativeObjectFilePath, needTempFile);
  }

  @Override
  public void deleteObjectPathFromBinary(Binary binary) {
    Optional<File> file = getObjectPathFromBinary(binary, true);
    if (file.isPresent()) {
      deleteObjectPath(file.get());
    }
  }

  @Override
  public void deleteObjectPath(File file) {
    File tmpFile = new File(file.getPath() + ".tmp");
    File bakFile = new File(file.getPath() + ".back");
    for (int i = 0; i < 2; i++) {
      if (file.exists()) {
        FileMetrics.getInstance().decreaseObjectFileNum(1);
        FileMetrics.getInstance().decreaseObjectFileSize(file.length());
      }
      try {
        deleteObjectFile(file);
        deleteObjectFile(tmpFile);
        deleteObjectFile(bakFile);
      } catch (IOException e) {
        logger.error("Failed to remove object file {}", file.getAbsolutePath(), e);
      }
    }
    deleteEmptyParentDir(file);
  }

  private static ByteBuffer readObjectContentFromLocalFile(File file, long offset, long readSize) {
    byte[] bytes = new byte[(int) readSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      fileChannel.read(buffer, offset);
    } catch (IOException e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
    }
    buffer.flip();
    return buffer;
  }

  private static ByteBuffer readObjectContentFromRemoteFile(
      final String relativePath, final long offset, final int readSize) {
    throw new UnsupportedOperationException("readObjectContentFromRemoteFile");
  }

  private static void deleteEmptyParentDir(File file) {
    File dir = file.getParentFile();
    if (dir.isDirectory() && Objects.requireNonNull(dir.list()).length == 0) {
      try {
        Files.deleteIfExists(dir.toPath());
        deleteEmptyParentDir(dir);
      } catch (IOException e) {
        logger.error("Failed to remove empty object dir {}", dir.getAbsolutePath(), e);
      }
    }
  }

  private static void deleteObjectFile(File file) throws IOException {
    if (file.exists()) {
      logger.info("Remove object file {}, size is {}(byte)", file.getAbsolutePath(), file.length());
    }
    Files.deleteIfExists(file.toPath());
  }
}
