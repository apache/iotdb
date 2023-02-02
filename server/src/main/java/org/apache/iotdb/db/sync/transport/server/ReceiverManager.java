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
package org.apache.iotdb.db.sync.transport.server;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.sync.PipeDataLoadException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.transport.SyncIdentityInfo;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for implementing the RPC processing on the receiver-side. It should
 * only be accessed by the {@linkplain org.apache.iotdb.db.sync.SyncService}
 */
public class ReceiverManager {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverManager.class);

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currentConnectionId;
  // Record the remote message for every rpc connection
  private final Map<Long, SyncIdentityInfo> connectionIdToIdentityInfoMap;
  // Record the remote message for every rpc connection
  private final Map<Long, Map<String, Long>> connectionIdToStartIndexRecord;
  private final Map<String, String> registeredDatabase;

  // The sync connectionId is unique in one IoTDB instance.
  private final AtomicLong connectionIdGenerator;

  public ReceiverManager() {
    currentConnectionId = new ThreadLocal<>();
    connectionIdToIdentityInfoMap = new ConcurrentHashMap<>();
    connectionIdToStartIndexRecord = new ConcurrentHashMap<>();
    registeredDatabase = new ConcurrentHashMap<>();
    connectionIdGenerator = new AtomicLong();
  }

  // region Interfaces and Implementation of Index Checker

  private class CheckResult {
    boolean result;
    String index;

    public CheckResult(boolean result, String index) {
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

  private CheckResult checkStartIndexValid(File file, long startIndex) throws IOException {
    // get local index from memory map
    long localIndex = getCurrentFileStartIndex(file.getAbsolutePath());
    // get local index from file
    if (localIndex < 0 && file.exists()) {
      localIndex = file.length();
      recordStartIndex(file, localIndex);
    }
    // compare and check
    if (localIndex < 0 && startIndex != 0) {
      logger.error(
          "The start index {} of data sync is not valid. "
              + "The file is not exist and start index should equal to 0).",
          startIndex);
      return new CheckResult(false, "0");
    } else if (localIndex >= 0 && localIndex != startIndex) {
      logger.error(
          "The start index {} of data sync is not valid. "
              + "The start index of the file should equal to {}.",
          startIndex,
          localIndex);
      return new CheckResult(false, String.valueOf(localIndex));
    }
    return new CheckResult(true, "0");
  }

  private void recordStartIndex(File file, long position) {
    Long id = currentConnectionId.get();
    if (id != null) {
      Map<String, Long> map =
          connectionIdToStartIndexRecord.computeIfAbsent(id, i -> new ConcurrentHashMap<>());
      map.put(file.getAbsolutePath(), position);
    }
  }

  // endregion

  // region Interfaces and Implementation of RPC Handler

  /**
   * Create connection from sender
   *
   * @return {@link TSStatusCode#PIPESERVER_ERROR} if fail to connect; {@link
   *     TSStatusCode#SUCCESS_STATUS} if success to connect.
   */
  public TSStatus handshake(
      TSyncIdentityInfo tIdentityInfo,
      String remoteAddress,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    SyncIdentityInfo identityInfo = new SyncIdentityInfo(tIdentityInfo, remoteAddress);
    logger.info("Invoke handshake method from client ip = {}", identityInfo.getRemoteAddress());
    // check ip address
    if (!verifyIPSegment(config.getIpWhiteList(), identityInfo.getRemoteAddress())) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR,
          String.format(
              "permission is not allowed: the sender IP <%s>, the white list of receiver <%s>",
              identityInfo.getRemoteAddress(), config.getIpWhiteList()));
    }
    // Version check
    if (!config.getIoTDBMajorVersion(identityInfo.version).equals(config.getIoTDBMajorVersion())) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR,
          String.format(
              "version mismatch: the sender <%s>, the receiver <%s>",
              identityInfo.version, config.getIoTDBVersion()));
    }

    if (!new File(SyncPathUtil.getFileDataDirPath(identityInfo)).exists()) {
      new File(SyncPathUtil.getFileDataDirPath(identityInfo)).mkdirs();
    }
    createConnection(identityInfo);
    if (!StringUtils.isEmpty(identityInfo.getDatabase())) {
      if (!registerDatabase(identityInfo.getDatabase(), partitionFetcher, schemaFetcher)) {
        return RpcUtils.getStatus(
            TSStatusCode.PIPESERVER_ERROR,
            String.format("Auto register database %s error.", identityInfo.getDatabase()));
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  /**
   * Verify IP address with IP white list which contains more than one IP segment. It's used by sync
   * sender.
   */
  private boolean verifyIPSegment(String ipWhiteList, String ipAddress) {
    String[] ipSegments = ipWhiteList.split(",");
    for (String IPsegment : ipSegments) {
      int subnetMask = Integer.parseInt(IPsegment.substring(IPsegment.indexOf('/') + 1));
      IPsegment = IPsegment.substring(0, IPsegment.indexOf('/'));
      if (verifyIP(IPsegment, ipAddress, subnetMask)) {
        return true;
      }
    }
    return false;
  }

  /** Verify IP address with IP segment. */
  private boolean verifyIP(String ipSegment, String ipAddress, int subnetMark) {
    String ipSegmentBinary;
    String ipAddressBinary;
    String[] ipSplits = ipSegment.split(SyncConstant.IP_SEPARATOR);
    DecimalFormat df = new DecimalFormat("00000000");
    StringBuilder ipSegmentBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipSegmentBuilder.append(
          df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipSegmentBinary = ipSegmentBuilder.toString();
    ipSegmentBinary = ipSegmentBinary.substring(0, subnetMark);
    ipSplits = ipAddress.split(SyncConstant.IP_SEPARATOR);
    StringBuilder ipAddressBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipAddressBuilder.append(
          df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipAddressBinary = ipAddressBuilder.toString();
    ipAddressBinary = ipAddressBinary.substring(0, subnetMark);
    return ipAddressBinary.equals(ipSegmentBinary);
  }

  /**
   * Receive {@link PipeData} and load it into IoTDB Engine.
   *
   * @return {@link TSStatusCode#PIPESERVER_ERROR} if fail to receive or load; {@link
   *     TSStatusCode#SUCCESS_STATUS} if load successfully.
   * @throws TException The connection between the sender and the receiver has not been established
   *     by {@link ReceiverManager#handshake}
   */
  public TSStatus transportPipeData(ByteBuffer buff) throws TException {
    // step1. check connection
    SyncIdentityInfo identityInfo = getCurrentSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    logger.debug(
        "Invoke transportPipeData method from client ip = {}", identityInfo.getRemoteAddress());
    String fileDir = SyncPathUtil.getFileDataDirPath(identityInfo);

    // step2. deserialize PipeData
    PipeData pipeData;
    try {
      int length = buff.capacity();
      byte[] byteArray = new byte[length];
      buff.get(byteArray);
      pipeData = PipeData.createPipeData(byteArray);
      if (pipeData instanceof TsFilePipeData) {
        TsFilePipeData tsFilePipeData = (TsFilePipeData) pipeData;
        tsFilePipeData.setDatabase(identityInfo.getDatabase());
        handleTsFilePipeData(tsFilePipeData, fileDir);
      }
    } catch (IOException | IllegalPathException e) {
      logger.error("Pipe data transport error, {}", e.getMessage());
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR, "Pipe data transport error, " + e.getMessage());
    }

    // step3. load PipeData
    logger.info(
        "Start load pipeData with serialize number {} and type {},value={}",
        pipeData.getSerialNumber(),
        pipeData.getPipeDataType(),
        pipeData);
    try {
      pipeData.createLoader().load();
      logger.info(
          "Load pipeData with serialize number {} successfully.", pipeData.getSerialNumber());
    } catch (PipeDataLoadException e) {
      logger.error("Fail to load pipeData because {}.", e.getMessage());
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR, "Fail to load pipeData because " + e.getMessage());
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  /**
   * Receive TsFile based on startIndex.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if receive successfully; {@link
   *     TSStatusCode#SYNC_FILE_REDIRECTION_ERROR} if startIndex needs to rollback because
   *     mismatched; {@link TSStatusCode#SYNC_FILE_ERROR} if fail to receive file.
   * @throws TException The connection between the sender and the receiver has not been established
   *     by {@link ReceiverManager#handshake}
   */
  public TSStatus transportFile(TSyncTransportMetaInfo metaInfo, ByteBuffer buff)
      throws TException {
    // step1. check connection
    SyncIdentityInfo identityInfo = getCurrentSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    logger.debug(
        "Invoke transportData method from client ip = {}", identityInfo.getRemoteAddress());

    String fileDir = SyncPathUtil.getFileDataDirPath(identityInfo);
    String fileName = metaInfo.fileName;
    long startIndex = metaInfo.startIndex;
    File file = new File(fileDir, fileName + SyncConstant.PATCH_SUFFIX);

    // step2. check startIndex
    try {
      CheckResult result = checkStartIndexValid(new File(fileDir, fileName), startIndex);
      if (!result.isResult()) {
        return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_REDIRECTION_ERROR, result.getIndex());
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_ERROR, e.getMessage());
    }

    // step3. append file
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
      int length = buff.capacity();
      randomAccessFile.seek(startIndex);
      byte[] byteArray = new byte[length];
      buff.get(byteArray);
      randomAccessFile.write(byteArray);
      recordStartIndex(new File(fileDir, fileName), startIndex + length);
      logger.debug(
          "Sync "
              + fileName
              + " start at "
              + startIndex
              + " to "
              + (startIndex + length)
              + " is done.");
    } catch (IOException e) {
      logger.error(e.getMessage());
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_ERROR, e.getMessage());
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  /**
   * handle when successfully receive tsFilePipeData. Rename .patch file and reset tsFilePipeData's
   * path.
   *
   * @param tsFilePipeData pipeData
   * @param fileDir path of file data dir
   */
  private void handleTsFilePipeData(TsFilePipeData tsFilePipeData, String fileDir) {
    String tsFileName = tsFilePipeData.getTsFileName();
    File dir = new File(fileDir);
    File[] targetFiles =
        dir.listFiles(
            (dir1, name) ->
                name.startsWith(tsFileName) && name.endsWith(SyncConstant.PATCH_SUFFIX));
    if (targetFiles != null) {
      for (File targetFile : targetFiles) {
        File newFile =
            new File(
                dir,
                targetFile
                    .getName()
                    .substring(
                        0, targetFile.getName().length() - SyncConstant.PATCH_SUFFIX.length()));
        targetFile.renameTo(newFile);
      }
    }
    tsFilePipeData.setParentDirPath(dir.getAbsolutePath());
  }

  // endregion

  // region Interfaces and Implementation of Connection Manager

  /** Check if the connection is legally established by handshaking */
  private boolean checkConnection() {
    return currentConnectionId.get() != null;
  }

  /**
   * Get current SyncIdentityInfo
   *
   * @return null if connection has been exited
   */
  private SyncIdentityInfo getCurrentSyncIdentityInfo() {
    Long id = currentConnectionId.get();
    if (id != null) {
      return connectionIdToIdentityInfoMap.get(id);
    } else {
      return null;
    }
  }

  /**
   * Get current FileStartIndex
   *
   * @return startIndex of file: -1 if file doesn't exist
   */
  private long getCurrentFileStartIndex(String absolutePath) {
    Long id = currentConnectionId.get();
    if (id != null) {
      Map<String, Long> map = connectionIdToStartIndexRecord.get(id);
      if (map != null && map.containsKey(absolutePath)) {
        return map.get(absolutePath);
      }
    }
    return -1;
  }

  private void createConnection(SyncIdentityInfo identityInfo) {
    long connectionId = connectionIdGenerator.incrementAndGet();
    currentConnectionId.set(connectionId);
    connectionIdToIdentityInfoMap.put(connectionId, identityInfo);
  }

  private boolean registerDatabase(
      String database, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    if (registeredDatabase.containsKey(database)) {
      return true;
    }
    try {
      DatabaseSchemaStatement statement =
          new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
      statement.setStorageGroupPath(new PartialPath(database));
      long queryId = SessionManager.getInstance().requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .execute(
                  statement,
                  queryId,
                  null,
                  "",
                  partitionFetcher,
                  schemaFetcher,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        logger.error("Create Database error, statement: {}.", statement);
        logger.error("Create database result status : {}.", result.status);
        return false;
      }
    } catch (IllegalPathException e) {
      logger.error(String.format("Parse database PartialPath %s error", database), e);
      return false;
    }

    registeredDatabase.put(database, "");
    return true;
  }

  /**
   * release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    if (checkConnection()) {
      long id = currentConnectionId.get();
      connectionIdToIdentityInfoMap.remove(id);
      connectionIdToStartIndexRecord.remove(id);
      currentConnectionId.remove();
    }
  }

  public List<SyncIdentityInfo> getAllTSyncIdentityInfos() {
    return new ArrayList<>(connectionIdToIdentityInfoMap.values());
  }

  // endregion
}
