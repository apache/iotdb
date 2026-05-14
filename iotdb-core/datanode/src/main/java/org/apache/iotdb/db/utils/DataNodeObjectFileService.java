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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.TableDiskUsageIndex;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectReq;
import org.apache.iotdb.mpp.rpc.thrift.TReadObjectResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
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

public class DataNodeObjectFileService implements IObjectFileService {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeObjectFileService.class);
  private static final Logger objectDeletionLogger =
      LoggerFactory.getLogger(IoTDBConstant.OBJECT_DELETION_LOGGER_NAME);
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
  public void deleteObjectPath(
      String database, int regionId, long timePartition, String table, File file) {
    File tmpFile = new File(file.getPath() + ".tmp");
    File bakFile = new File(file.getPath() + ".back");
    for (int i = 0; i < 2; i++) {
      boolean fileExistsBeforeDelete = file.exists();
      long length = file.length();
      try {
        deleteObjectFile(file);
        deleteObjectFile(tmpFile);
        deleteObjectFile(bakFile);
      } catch (IOException e) {
        objectDeletionLogger.error("Failed to remove object file {}", file.getAbsolutePath(), e);
      }
      if (fileExistsBeforeDelete && !file.exists()) {
        FileMetrics.getInstance().decreaseObjectFileNum(1);
        FileMetrics.getInstance().decreaseObjectFileSize(length);
        TableDiskUsageIndex.getInstance()
            .writeObjectDelta(database, regionId, timePartition, table, -length, -1);
      }
    }
    deleteEmptyParentDir(file);
  }

  @Override
  public ByteBuffer readObjectContentFromLocalFile(File file, long offset, long readSize) {
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

  private static void deleteEmptyParentDir(File file) {
    File dir = file.getParentFile();
    if (dir.isDirectory() && Objects.requireNonNull(dir.list()).length == 0) {
      try {
        Files.deleteIfExists(dir.toPath());
        deleteEmptyParentDir(dir);
      } catch (IOException e) {
        objectDeletionLogger.error(
            "Failed to remove empty object dir {}", dir.getAbsolutePath(), e);
      }
    }
  }

  private static void deleteObjectFile(File file) throws IOException {
    if (file.exists()) {
      objectDeletionLogger.info(
          "Remove object file {}, size is {}(byte)", file.getAbsolutePath(), file.length());
    }
    Files.deleteIfExists(file.toPath());
  }
}
