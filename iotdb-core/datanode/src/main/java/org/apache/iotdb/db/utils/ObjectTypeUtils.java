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
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.IObjectPath;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DecoderWrapper;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
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
import java.util.Objects;
import java.util.Optional;

public class ObjectTypeUtils {

  private static final Logger logger = LoggerFactory.getLogger(ObjectTypeUtils.class);
  private static final TierManager TIER_MANAGER = TierManager.getInstance();

  private ObjectTypeUtils() {}

  public static ByteBuffer readObjectContent(
      Binary binary, long offset, int length, boolean mayNotInCurrentNode) {
    Pair<Long, String> objectLengthPathPair =
        ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary);
    long fileLength = objectLengthPathPair.getLeft();
    String relativePath = objectLengthPathPair.getRight();
    int actualReadSize =
        ObjectTypeUtils.getActualReadSize(
            relativePath, fileLength, offset, length < 0 ? fileLength : length);
    return ObjectTypeUtils.readObjectContent(
        relativePath, offset, actualReadSize, mayNotInCurrentNode);
  }

  public static ByteBuffer readObjectContent(
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
    int regionId;
    try {
      regionId = Integer.parseInt(Paths.get(relativePath).getName(0).toString());
    } catch (NumberFormatException e) {
      throw new IoTDBRuntimeException(
          "wrong object file path: " + relativePath,
          TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
    }
    TConsensusGroupId consensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId);
    List<TRegionReplicaSet> regionReplicaSetList =
        ClusterPartitionFetcher.getInstance()
            .getRegionReplicaSet(Collections.singletonList(consensusGroupId));
    if (regionReplicaSetList.isEmpty()) {
      throw new ObjectFileNotExist(relativePath);
    }
    TRegionReplicaSet regionReplicaSet = regionReplicaSetList.iterator().next();
    if (regionReplicaSet.getDataNodeLocations().isEmpty()) {
      throw new ObjectFileNotExist(relativePath);
    }
    final int batchSize = 1024 * 1024;
    final TReadObjectReq req = new TReadObjectReq();
    req.setRelativePath(relativePath);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    for (int i = 0; i < regionReplicaSet.getDataNodeLocations().size(); i++) {
      TDataNodeLocation dataNodeLocation = regionReplicaSet.getDataNodeLocations().get(i);
      int toReadSizeInCurrentDataNode = readSize;
      try (SyncDataNodeInternalServiceClient client =
          Coordinator.getInstance()
              .getInternalServiceClientManager()
              .borrowClient(dataNodeLocation.getInternalEndPoint())) {
        while (toReadSizeInCurrentDataNode > 0) {
          req.setOffset(offset + buffer.position());
          req.setSize(Math.min(toReadSizeInCurrentDataNode, batchSize));
          toReadSizeInCurrentDataNode -= req.getSize();
          ByteBuffer partial = client.readObject(req);
          buffer.put(partial);
        }
      } catch (Exception e) {
        logger.warn("Failed to read object from datanode: {}", dataNodeLocation, e);
        if (i == regionReplicaSet.getDataNodeLocations().size() - 1) {
          throw new IoTDBRuntimeException(e, TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
        }
        continue;
      }
      break;
    }
    buffer.flip();
    return buffer;
  }

  public static Binary generateObjectBinary(long objectSize, IObjectPath objectPath) {
    byte[] valueBytes = new byte[objectPath.getSerializeSizeToObjectValue() + Long.BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
    ReadWriteIOUtils.write(objectSize, buffer);
    objectPath.serializeToObjectValue(buffer);
    return new Binary(buffer.array());
  }

  public static DecoderWrapper getReplaceDecoder(final Decoder decoder, final int newRegionId) {
    return new ObjectRegionIdReplaceDecoder(decoder, newRegionId);
  }

  private static class ObjectRegionIdReplaceDecoder extends DecoderWrapper {

    private final int newRegionId;

    public ObjectRegionIdReplaceDecoder(Decoder decoder, int newRegionId) {
      super(decoder);
      this.newRegionId = newRegionId;
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
      Binary originValue = originDecoder.readBinary(buffer);
      return ObjectTypeUtils.replaceRegionIdForObjectBinary(newRegionId, originValue);
    }
  }

  public static Binary replaceRegionIdForObjectBinary(int newRegionId, Binary originValue) {
    Pair<Long, IObjectPath> pair =
        ObjectTypeUtils.parseObjectBinaryToSizeIObjectPathPair(originValue);
    IObjectPath objectPath = pair.getRight();
    try {
      IObjectPath newObjectPath = null;
      return ObjectTypeUtils.generateObjectBinary(pair.getLeft(), newObjectPath);
    } catch (NumberFormatException e) {
      throw new IoTDBRuntimeException(
          "wrong object file path: " + pair.getRight(),
          TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
    }
  }

  public static int getActualReadSize(String filePath, long fileSize, long offset, long length) {
    if (offset < 0) {
      throw new SemanticException(String.format("offset %d is less than 0.", offset));
    }
    if (offset >= fileSize) {
      throw new SemanticException(
          String.format(
              "offset %d is greater than or equal to object size %d, file path is %s",
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

  public static Pair<Long, String> parseObjectBinaryToSizeStringPathPair(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long length = buffer.getLong();
    String relativeObjectFilePath =
        IObjectPath.getDeserializer().deserializeFromObjectValue(buffer).toString();
    return new Pair<>(length, relativeObjectFilePath);
  }

  public static Pair<Long, IObjectPath> parseObjectBinaryToSizeIObjectPathPair(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long length = buffer.getLong();
    IObjectPath objectPath = IObjectPath.getDeserializer().deserializeFromObjectValue(buffer);
    return new Pair<>(length, objectPath);
  }

  public static long getObjectLength(Binary binary) {
    byte[] bytes = binary.getValues();
    ByteBuffer wrap = ByteBuffer.wrap(bytes);
    return wrap.getLong();
  }

  public static Optional<File> getObjectPathFromBinary(Binary binary) {
    return getObjectPathFromBinary(binary, false);
  }

  public static Optional<File> getObjectPathFromBinary(Binary binary, boolean needTempFile) {
    byte[] bytes = binary.getValues();
    ByteBuffer buffer = ByteBuffer.wrap(bytes, 8, bytes.length - 8);
    String relativeObjectFilePath =
        IObjectPath.getDeserializer().deserializeFromObjectValue(buffer).toString();
    return TIER_MANAGER.getAbsoluteObjectFilePath(relativeObjectFilePath, needTempFile);
  }

  public static void deleteObjectPathFromBinary(Binary binary) {
    Optional<File> file = ObjectTypeUtils.getObjectPathFromBinary(binary, true);
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

  public static void deleteObjectPath(File file) {
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
