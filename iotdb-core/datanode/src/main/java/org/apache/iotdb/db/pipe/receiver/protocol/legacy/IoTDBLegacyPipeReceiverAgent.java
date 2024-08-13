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
 *
 */

package org.apache.iotdb.db.pipe.receiver.protocol.legacy;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.legacy.PipeData;
import org.apache.iotdb.db.pipe.connector.payload.legacy.TsFilePipeData;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class IoTDBLegacyPipeReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLegacyPipeReceiverAgent.class);

  private static final String PATCH_SUFFIX = ".patch";

  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currentConnectionId = new ThreadLocal<>();

  // Record the remote message for every rpc connection
  private final Map<Long, SyncIdentityInfo> connectionIdToIdentityInfoMap =
      new ConcurrentHashMap<>();

  // Record the remote message for every rpc connection
  private final Map<Long, Map<String, Long>> connectionIdToStartIndexRecord =
      new ConcurrentHashMap<>();

  private final Map<String, String> registeredDatabase = new ConcurrentHashMap<>();

  // The sync connectionId is unique in one IoTDB instance.
  private final AtomicLong connectionIdGenerator = new AtomicLong();

  //////////////////////// methods for RPC handler ////////////////////////

  /**
   * Release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    if (currentConnectionId.get() != null) {
      long id = currentConnectionId.get();
      connectionIdToIdentityInfoMap.remove(id);
      connectionIdToStartIndexRecord.remove(id);
      currentConnectionId.remove();
    }
  }

  /**
   * Create connection from sender.
   *
   * @return {@link TSStatusCode#PIPESERVER_ERROR} if fail to connect; {@link
   *     TSStatusCode#SUCCESS_STATUS} if success to connect.
   */
  public TSStatus handshake(
      final TSyncIdentityInfo syncIdentityInfo,
      final String remoteAddress,
      final IPartitionFetcher partitionFetcher,
      final ISchemaFetcher schemaFetcher) {
    final SyncIdentityInfo identityInfo = new SyncIdentityInfo(syncIdentityInfo, remoteAddress);
    LOGGER.info("Invoke handshake method from client ip = {}", identityInfo.getRemoteAddress());

    if (!new File(getFileDataDir(identityInfo)).exists()) {
      new File(getFileDataDir(identityInfo)).mkdirs();
    }
    createConnection(identityInfo);
    if (!StringUtils.isEmpty(identityInfo.getDatabase())
        && !registerDatabase(identityInfo.getDatabase(), partitionFetcher, schemaFetcher)) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR,
          String.format("Auto register database %s error.", identityInfo.getDatabase()));
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  private void createConnection(final SyncIdentityInfo identityInfo) {
    final long connectionId = connectionIdGenerator.incrementAndGet();
    currentConnectionId.set(connectionId);
    connectionIdToIdentityInfoMap.put(connectionId, identityInfo);
  }

  private boolean registerDatabase(
      final String database,
      final IPartitionFetcher partitionFetcher,
      final ISchemaFetcher schemaFetcher) {
    if (registeredDatabase.containsKey(database)) {
      return true;
    }
    try {
      final DatabaseSchemaStatement statement =
          new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
      statement.setDatabasePath(new PartialPath(database));
      final long queryId = SessionManager.getInstance().requestQueryId();
      final ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
                  "",
                  partitionFetcher,
                  schemaFetcher,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
          && result.status.code != TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        LOGGER.error(
            "Create Database error, statement: {}, result status : {}.", statement, result.status);
        return false;
      }
    } catch (final IllegalPathException e) {
      LOGGER.error("Parse database PartialPath {} error", database, e);
      return false;
    }

    registeredDatabase.put(database, "");
    return true;
  }

  /**
   * Receive {@link PipeData} and load it into IoTDB Engine.
   *
   * @return {@link TSStatusCode#PIPESERVER_ERROR} if fail to receive or load; {@link
   *     TSStatusCode#SUCCESS_STATUS} if load successfully.
   * @throws TException The connection between the sender and the receiver has not been established
   *     by {@link IoTDBLegacyPipeReceiverAgent#handshake}
   */
  public TSStatus transportPipeData(final ByteBuffer buff) throws TException {
    // step1. check connection
    final SyncIdentityInfo identityInfo = getCurrentSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    LOGGER.debug(
        "Invoke transportPipeData method from client ip = {}", identityInfo.getRemoteAddress());
    final String fileDir = getFileDataDir(identityInfo);

    // step2. deserialize PipeData
    final PipeData pipeData;
    try {
      final int length = buff.remaining();
      final byte[] byteArray = new byte[length];
      buff.get(byteArray);
      pipeData = PipeData.createPipeData(byteArray);
      if (pipeData instanceof TsFilePipeData) {
        TsFilePipeData tsFilePipeData = (TsFilePipeData) pipeData;
        tsFilePipeData.setDatabase(identityInfo.getDatabase());
        handleTsFilePipeData(tsFilePipeData, fileDir);
      }
    } catch (final IOException e) {
      LOGGER.error("Pipe data transport error, {}", e.getMessage());
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR, "Pipe data transport error, " + e.getMessage());
    }

    // step3. load PipeData
    LOGGER.info(
        "Start load pipeData with serialize number {} and type {},value={}",
        pipeData.getSerialNumber(),
        pipeData.getPipeDataType(),
        pipeData);
    try {
      pipeData.createLoader().load();
      LOGGER.info(
          "Load pipeData with serialize number {} successfully.", pipeData.getSerialNumber());
    } catch (final PipeException e) {
      LOGGER.error("Fail to load pipeData because {}.", e.getMessage());
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR, "Fail to load pipeData because " + e.getMessage());
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  /**
   * Get current SyncIdentityInfo.
   *
   * @return null if connection has been exited
   */
  private SyncIdentityInfo getCurrentSyncIdentityInfo() {
    final Long id = currentConnectionId.get();
    if (id != null) {
      return connectionIdToIdentityInfoMap.get(id);
    } else {
      return null;
    }
  }

  /**
   * handle when successfully receive tsFilePipeData. Rename .patch file and reset tsFilePipeData's
   * path.
   *
   * @param tsFilePipeData pipeData
   * @param fileDir path of file data dir
   */
  private void handleTsFilePipeData(final TsFilePipeData tsFilePipeData, final String fileDir) {
    final String tsFileName = tsFilePipeData.getTsFileName();
    final File dir = new File(fileDir);
    final File[] targetFiles =
        dir.listFiles((dir1, name) -> name.startsWith(tsFileName) && name.endsWith(PATCH_SUFFIX));
    if (targetFiles != null) {
      for (final File targetFile : targetFiles) {
        final File newFile =
            new File(
                dir,
                targetFile
                    .getName()
                    .substring(0, targetFile.getName().length() - PATCH_SUFFIX.length()));
        if (!targetFile.renameTo(newFile)) {
          LOGGER.error("Fail to rename file {} to {}", targetFile, newFile);
        }
      }
    }
    tsFilePipeData.setParentDirPath(dir.getAbsolutePath());
  }

  /**
   * Receive TsFile based on startIndex.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if receive successfully; {@link
   *     TSStatusCode#SYNC_FILE_REDIRECTION_ERROR} if startIndex needs to rollback because
   *     mismatched; {@link TSStatusCode#SYNC_FILE_ERROR} if fail to receive file.
   * @throws TException The connection between the sender and the receiver has not been established
   *     by {@link IoTDBLegacyPipeReceiverAgent#handshake}
   */
  public TSStatus transportFile(final TSyncTransportMetaInfo metaInfo, final ByteBuffer buff)
      throws TException {
    // step1. check connection
    final SyncIdentityInfo identityInfo = getCurrentSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    LOGGER.debug(
        "Invoke transportData method from client ip = {}", identityInfo.getRemoteAddress());

    final String fileDir = getFileDataDir(identityInfo);
    final String fileName = metaInfo.fileName;
    final long startIndex = metaInfo.startIndex;
    final File file = new File(fileDir, fileName + PATCH_SUFFIX);

    // step2. check startIndex
    final IndexCheckResult result = checkStartIndexValid(new File(fileDir, fileName), startIndex);
    if (!result.isResult()) {
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_REDIRECTION_ERROR, result.getIndex());
    }

    // step3. append file
    try (final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
      final int length = buff.remaining();
      randomAccessFile.seek(startIndex);
      final byte[] byteArray = new byte[length];
      buff.get(byteArray);
      randomAccessFile.write(byteArray);
      recordStartIndex(new File(fileDir, fileName), startIndex + length);
      LOGGER.debug("Sync {} start at {} to {} is done.", fileName, startIndex, startIndex + length);
    } catch (final IOException e) {
      LOGGER.error(e.getMessage());
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_ERROR, e.getMessage());
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  private IndexCheckResult checkStartIndexValid(final File file, final long startIndex) {
    // get local index from memory map
    long localIndex = getCurrentFileStartIndex(file.getAbsolutePath());
    // get local index from file
    if (localIndex < 0 && file.exists()) {
      localIndex = file.length();
      recordStartIndex(file, localIndex);
    }
    // compare and check
    if (localIndex < 0 && startIndex != 0) {
      LOGGER.error(
          "The start index {} of data sync is not valid. "
              + "The file is not exist and start index should equal to 0).",
          startIndex);
      return new IndexCheckResult(false, "0");
    } else if (localIndex >= 0 && localIndex != startIndex) {
      LOGGER.error(
          "The start index {} of data sync is not valid. "
              + "The start index of the file should equal to {}.",
          startIndex,
          localIndex);
      return new IndexCheckResult(false, String.valueOf(localIndex));
    }
    return new IndexCheckResult(true, "0");
  }

  /**
   * Get current FileStartIndex.
   *
   * @return startIndex of file: -1 if file doesn't exist
   */
  private long getCurrentFileStartIndex(final String absolutePath) {
    final Long id = currentConnectionId.get();
    if (id != null) {
      final Map<String, Long> map = connectionIdToStartIndexRecord.get(id);
      if (map != null && map.containsKey(absolutePath)) {
        return map.get(absolutePath);
      }
    }
    return -1;
  }

  private void recordStartIndex(final File file, final long position) {
    final Long id = currentConnectionId.get();
    if (id != null) {
      final Map<String, Long> map =
          connectionIdToStartIndexRecord.computeIfAbsent(id, i -> new ConcurrentHashMap<>());
      map.put(file.getAbsolutePath(), position);
    }
  }

  ///////////////////////// sync data dir structure /////////////////////////

  // data/sync
  // |----receiver dir
  // |      |-----receiver pipe dir
  // |              |----file data dir

  private static final String RECEIVER_DIR_NAME = "receiver";
  private static final String FILE_DATA_DIR_NAME = "file-data";

  private static String getFileDataDir(final SyncIdentityInfo identityInfo) {
    return getReceiverPipeDir(
            identityInfo.getPipeName(),
            identityInfo.getRemoteAddress(),
            identityInfo.getCreateTime())
        + File.separator
        + FILE_DATA_DIR_NAME;
  }

  private static String getReceiverPipeDir(
      final String pipeName, final String remoteIp, final long createTime) {
    return getReceiverDir()
        + File.separator
        + String.format("%s-%d-%s", pipeName, createTime, remoteIp);
  }

  private static String getReceiverDir() {
    return CommonDescriptor.getInstance().getConfig().getSyncDir()
        + File.separator
        + RECEIVER_DIR_NAME;
  }

  ///////////////////// helper classes //////////////////////

  private static class SyncIdentityInfo {

    private final String pipeName;
    private final long createTime;
    private final String version;
    private final String database;
    private final String remoteAddress;

    public SyncIdentityInfo(final TSyncIdentityInfo identityInfo, final String remoteAddress) {
      this.pipeName = identityInfo.getPipeName();
      this.createTime = identityInfo.getCreateTime();
      this.version = identityInfo.getVersion();
      this.database = identityInfo.getDatabase();
      this.remoteAddress = remoteAddress;
    }

    public String getPipeName() {
      return pipeName;
    }

    public long getCreateTime() {
      return createTime;
    }

    public String getVersion() {
      return version;
    }

    public String getRemoteAddress() {
      return remoteAddress;
    }

    public String getDatabase() {
      return database;
    }
  }

  private static class IndexCheckResult {

    private final boolean result;
    private final String index;

    public IndexCheckResult(final boolean result, final String index) {
      this.result = result;
      this.index = index;
    }

    public boolean isResult() {
      return result;
    }

    public String getIndex() {
      return index;
    }
  }
}
