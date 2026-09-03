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
import org.apache.iotdb.calc.utils.ObjectPathNaming;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.service.metrics.DataNodeExceptionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.TableDiskUsageIndex;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectReq;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectResp;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.os.cache.OSFileChannel;
import com.timecho.iotdb.os.exception.S3ConnectionException;
import com.timecho.iotdb.os.fileSystem.OSFile;
import com.timecho.iotdb.os.utils.RemoteStorageBlock;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
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
import java.util.concurrent.ConcurrentHashMap;

public class DataNodeObjectFileService implements IObjectFileService {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeObjectFileService.class);
  private static final Logger objectDeletionLogger =
      LoggerFactory.getLogger(IoTDBConstant.OBJECT_DELETION_LOGGER_NAME);
  private static final TierManager TIER_MANAGER = TierManager.getInstance();
  public static final DataNodeObjectFileService INSTANCE = new DataNodeObjectFileService();

  /**
   * Object roots taken from loaded TsFile {@link RemoteStorageBlock}s whose DataNode prefix is not
   * this node's. Same registration point as TsFile path remapping.
   */
  private static final ConcurrentHashMap<String, Integer> REMAPPED_OBJECT_ROOTS =
      new ConcurrentHashMap<>();

  private DataNodeObjectFileService() {}

  /**
   * Register a remapped object root from a TsFile {@link RemoteStorageBlock}. No-op when the root
   * is this node's own last-tier object dir.
   *
   * @return true if a foreign root was registered
   */
  public static boolean registerRemoteObjectRoot(RemoteStorageBlock block) {
    Optional<String> root = objectRootFromRemoteStorageBlock(block);
    if (!root.isPresent() || isLocalObjectRoot(root.get())) {
      return false;
    }
    REMAPPED_OBJECT_ROOTS.merge(root.get(), 1, Integer::sum);
    return true;
  }

  public static void unregisterRemoteObjectRoot(RemoteStorageBlock block) {
    objectRootFromRemoteStorageBlock(block)
        .ifPresent(
            root ->
                REMAPPED_OBJECT_ROOTS.computeIfPresent(
                    root, (k, count) -> count <= 1 ? null : count - 1));
  }

  /**
   * {@code os://bucket/{dnId}/object} (or HDFS equivalent) from a TsFile remote path {@code
   * {dataDir}/sequence|unsequence/...}.
   */
  static Optional<String> objectRootFromRemoteStorageBlock(RemoteStorageBlock block) {
    if (block == null || block.getPath() == null || block.getPath().isEmpty()) {
      return Optional.empty();
    }
    return objectRootFromTsFileRemotePath(block.getPath());
  }

  static Optional<String> objectRootFromTsFileRemotePath(String tsFileRemotePath) {
    int seqIdx = tsFileRemotePath.indexOf("/" + IoTDBConstant.SEQUENCE_FOLDER_NAME + "/");
    int unseqIdx = tsFileRemotePath.indexOf("/" + IoTDBConstant.UNSEQUENCE_FOLDER_NAME + "/");
    int marker = seqIdx < 0 ? unseqIdx : unseqIdx < 0 ? seqIdx : Math.min(seqIdx, unseqIdx);
    if (marker < 0) {
      return Optional.empty();
    }
    String dataDir = tsFileRemotePath.substring(0, marker);
    while (dataDir.endsWith("/") || dataDir.endsWith("\\")) {
      dataDir = dataDir.substring(0, dataDir.length() - 1);
    }
    if (dataDir.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(dataDir + "/" + IoTDBConstant.OBJECT_FOLDER_NAME);
  }

  private static boolean isLocalObjectRoot(String objectRoot) {
    String normalized = objectRoot.replace('\\', '/');
    for (String dir : TIER_MANAGER.getAllObjectFileFolders()) {
      if (dir.equals(objectRoot) || dir.replace('\\', '/').equals(normalized)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ByteBuffer readObjectContent(
      String relativePath, long offset, int readSize, boolean mayNotInCurrentNode) {
    Optional<File> objectFile = TIER_MANAGER.getAbsoluteObjectFilePath(relativePath, false);
    if (objectFile.isPresent()) {
      return readObjectContentFromLocalFile(objectFile.get(), offset, readSize);
    }
    for (String root : REMAPPED_OBJECT_ROOTS.keySet()) {
      File remapped = FSFactoryProducer.getFSFactory().getFile(root, relativePath);
      if (remapped.exists()) {
        return readObjectContentFromLocalFile(remapped, offset, readSize);
      }
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
  public void deleteObjectPath(
      String database, int regionId, long timePartition, String table, File file) {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    long time = parseObjectTime(file);
    File tmpFile =
        siblingOrSuffix(
            file,
            time >= 0 ? ObjectPathNaming.toTempFileName(time) : null,
            ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX,
            fsFactory);
    File bakFile =
        siblingOrSuffix(
            file,
            time >= 0 ? ObjectPathNaming.toBackFileName(time) : null,
            ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX,
            fsFactory);
    for (int i = 0; i < 2; i++) {
      long length = objectLengthIfPresent(file);
      try {
        if (deleteObjectFile(file)) {
          FileMetrics.getInstance().decreaseObjectFileNum(database, String.valueOf(regionId), 1);
          FileMetrics.getInstance()
              .decreaseObjectFileSize(database, String.valueOf(regionId), length);
          TableDiskUsageIndex.getInstance()
              .writeObjectDelta(database, regionId, timePartition, table, -length, -1);
        }
        deleteObjectFile(tmpFile);
        deleteObjectFile(bakFile);
      } catch (IOException e) {
        DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
        objectDeletionLogger.error(
            DataNodeMiscMessages.FAILED_REMOVE_OBJECT_FILE, file.getAbsolutePath(), e);
      }
    }
    if (FSUtils.getFSType(file) == FSType.LOCAL) {
      deleteEmptyParentDir(file);
    }
  }

  @Override
  public ByteBuffer readObjectContentFromLocalFile(File file, long offset, long readSize) {
    byte[] bytes = new byte[(int) readSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try {
      if (FSUtils.getFSType(file.getPath()) == FSType.OBJECT_STORAGE) {
        readObjectContentFromObjectStorage(file, buffer, offset);
      } else {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
          IOUtils.readFully(fileChannel, buffer, offset);
        }
      }
    } catch (IOException e) {
      DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
      throw new IoTDBRuntimeException(e, TSStatusCode.OBJECT_READ_ERROR.getStatusCode());
    }
    buffer.flip();
    return buffer;
  }

  private static void readObjectContentFromObjectStorage(File file, ByteBuffer buffer, long offset)
      throws IOException {
    OSFile osFile = file instanceof OSFile ? (OSFile) file : new OSFile(file.getPath());
    try (OSFileChannel channel = new OSFileChannel(osFile)) {
      channel.read(buffer, offset);
    }
    if (buffer.hasRemaining()) {
      throw new EOFException();
    }
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
          TReadObjectResp resp = client.readObject(req);
          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            buffer.put(resp.content);
          } else if (resp.getStatus().getCode() == TSStatusCode.OBJECT_NOT_EXISTS.getStatusCode()) {
            throw new ObjectFileNotExist(relativePath);
          } else {
            throw new IoTDBRuntimeException(resp.status);
          }
        }
      } catch (ObjectFileNotExist e) {
        throw e;
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

  private static long parseObjectTime(File file) {
    return ObjectPathNaming.parseTime(ObjectPathNaming.baseFileName(file.getPath()));
  }

  private static File siblingOrSuffix(
      File file, String siblingFileName, String suffix, FSFactory fsFactory) {
    File parent = file.getParentFile();
    if (parent != null && siblingFileName != null) {
      return fsFactory.getFile(parent, siblingFileName);
    }
    return fsFactory.getFile(file.getPath() + suffix);
  }

  private static long objectLengthIfPresent(File file) {
    try {
      if (FSUtils.getFSType(file) == FSType.LOCAL || file.exists()) {
        return file.length();
      }
    } catch (S3ConnectionException e) {
      objectDeletionLogger.warn(
          DataNodeMiscMessages
              .LOG_OBJECT_STORAGE_UNAVAILABLE_WHILE_PROBING_LENGTH_OF_ARG_TREAT_AS_ZERO_FOR_METRICS_8131DA24,
          file,
          e);
    }
    return 0L;
  }

  private static void deleteEmptyParentDir(File file) {
    File dir = file.getParentFile();
    if (dir == null || FSUtils.getFSType(dir) != FSType.LOCAL) {
      return;
    }
    if (dir.isDirectory() && Objects.requireNonNull(dir.list()).length == 0) {
      try {
        Files.deleteIfExists(dir.toPath());
        deleteEmptyParentDir(dir);
      } catch (IOException e) {
        DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
        objectDeletionLogger.error(
            DataNodeMiscMessages.FAILED_REMOVE_EMPTY_OBJECT_DIR, dir.getAbsolutePath(), e);
      }
    }
  }

  private static boolean deleteObjectFile(File file) throws IOException {
    if (file == null) {
      return false;
    }
    long length = objectLengthIfPresent(file);
    if (FSFactoryProducer.getFSFactory().deleteIfExists(file)) {
      objectDeletionLogger.info(
          DataNodeMiscMessages.REMOVE_OBJECT_FILE, file.getAbsolutePath(), length);
      return true;
    }
    return false;
  }
}
