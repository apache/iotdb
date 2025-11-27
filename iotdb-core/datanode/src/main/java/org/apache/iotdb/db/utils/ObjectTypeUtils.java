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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ObjectTypeUtils {

  private static final Logger logger = LoggerFactory.getLogger(ObjectTypeUtils.class);
  private static final TierManager TIER_MANAGER = TierManager.getInstance();

  private ObjectTypeUtils() {}

  public static ByteBuffer readObjectContent(
      Binary binary, long offset, long length, boolean mayNotInCurrentNode) {
    Pair<Long, String> ObjectLengthPathPair = ObjectTypeUtils.parseObjectBinary(binary);
    long fileLength = ObjectLengthPathPair.getLeft();
    length = length < 0 ? fileLength : length;
    String relativePath = ObjectLengthPathPair.getRight();
    int actualReadSize =
        ObjectTypeUtils.getActualReadSize(relativePath, fileLength, offset, length);
    return ObjectTypeUtils.readObjectContent(
        relativePath, offset, actualReadSize, mayNotInCurrentNode);
  }

  public static ByteBuffer readObjectContent(
      String relativePath, long offset, long readSize, boolean mayNotInCurrentNode) {
    Optional<File> objectFile = TIER_MANAGER.getAbsoluteObjectFilePath(relativePath, false);
    if (objectFile.isPresent()) {
      return readObjectContentFromLocalFile(objectFile.get(), offset, readSize);
    }
    if (mayNotInCurrentNode) {
      return readObjectContentFromRemoteFile(relativePath, offset, readSize);
    }
    throw new ObjectFileNotExist(relativePath);
  }

  private static ByteBuffer readObjectContentFromLocalFile(File file, long offset, long readSize) {
    byte[] bytes = new byte[(int) readSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      fileChannel.read(buffer, offset);
    } catch (IOException e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    buffer.flip();
    return buffer;
  }

  private static ByteBuffer readObjectContentFromRemoteFile(
      String relativePath, long offset, long readSize) {
    byte[] bytes = new byte[(int) readSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    TConsensusGroupId consensusGroupId =
        new TConsensusGroupId(
            TConsensusGroupType.DataRegion,
            Integer.parseInt(Paths.get(relativePath).getName(0).toString()));
    List<TRegionReplicaSet> regionReplicaSetList =
        ClusterPartitionFetcher.getInstance()
            .getRegionReplicaSet(Collections.singletonList(consensusGroupId));
    TRegionReplicaSet regionReplicaSet = regionReplicaSetList.iterator().next();
    final int batchSize = 1024 * 1024 * 1024;
    final TReadObjectReq req = new TReadObjectReq();
    req.setRelativePath(relativePath);
    for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
      try (SyncDataNodeInternalServiceClient client =
          Coordinator.getInstance()
              .getInternalServiceClientManager()
              .borrowClient(dataNodeLocation.getInternalEndPoint())) {
        while (readSize > 0) {
          req.setOffset(offset + buffer.position());
          req.setSize(Math.min(readSize, batchSize));
          readSize -= req.getSize();
          ByteBuffer partial = client.readObject(req);
          buffer.put(partial);
        }
      } catch (ClientManagerException | TException e) {
        logger.error(e.getMessage(), e);
        throw new IoTDBRuntimeException(e, TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    }
    buffer.flip();
    return buffer;
  }

  public static int getActualReadSize(String filePath, long fileSize, long offset, long length) {
    if (offset >= fileSize) {
      throw new SemanticException(
          String.format(
              "offset %d is greater than object size %d, file path is %s",
              offset, fileSize, filePath));
    }
    long actualReadSize = Math.min(length < 0 ? fileSize : length, fileSize - offset);
    if (actualReadSize > Integer.MAX_VALUE) {
      throw new SemanticException(
          String.format(
              "Read object size %s is too large (size > 2G), file path is %s",
              actualReadSize, filePath));
    }
    return (int) actualReadSize;
  }

  public static Pair<Long, String> parseObjectBinary(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long length = buffer.getLong();
    String relativeObjectFilePath =
        new String(bytes, 8, bytes.length - 8, TSFileConfig.STRING_CHARSET);
    return new Pair<>(length, relativeObjectFilePath);
  }

  public static long getObjectLength(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer wrap = ByteBuffer.wrap(bytes);
    return wrap.getLong();
  }

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
    for (int i = 0; i < 2; i++) {
      if (file.get().exists()) {
        FileMetrics.getInstance().decreaseObjectFileNum(1);
        FileMetrics.getInstance().decreaseObjectFileSize(file.get().length());
      }
      try {
        deleteObjectFile(file.get());
        deleteObjectFile(tmpFile);
        deleteObjectFile(bakFile);
      } catch (IOException e) {
        logger.error("Failed to remove object file {}", file.get().getAbsolutePath(), e);
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
