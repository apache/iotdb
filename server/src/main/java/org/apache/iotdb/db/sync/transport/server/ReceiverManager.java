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
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportType;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.sync.SyncConstant.DATA_CHUNK_SIZE;

/**
 * This class is responsible for implementing the RPC processing on the receiver-side. It should
 * only be accessed by the {@linkplain org.apache.iotdb.db.sync.SyncService}
 */
public class ReceiverManager {
  private static Logger logger = LoggerFactory.getLogger(ReceiverManager.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String RECORD_SUFFIX = ".record";
  private static final String PATCH_SUFFIX = ".patch";

  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currentConnectionId;
  // Record the remote message for every rpc connection
  private final Map<Long, TSyncIdentityInfo> connectionIdToIdentityInfoMap;

  // The sync connectionId is unique in one IoTDB instance.
  private final AtomicLong connectionIdGenerator;

  public ReceiverManager() {
    currentConnectionId = new ThreadLocal<>();
    connectionIdToIdentityInfoMap = new ConcurrentHashMap<>();
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
    File recordFile = new File(file.getAbsolutePath() + RECORD_SUFFIX);

    if (!recordFile.exists() && startIndex != 0) {
      logger.error(
          "The start index {} of data sync is not valid. "
              + "The file {} is not exist and start index should equal to 0).",
          startIndex,
          recordFile.getAbsolutePath());
      return new CheckResult(false, "0");
    }

    if (recordFile.exists()) {
      try (InputStream inputStream = new FileInputStream(recordFile);
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
        String index = bufferedReader.readLine();

        if ((index == null) || (index.length() == 0)) {
          if (startIndex != 0) {
            logger.error(
                "The start index {} of data sync is not valid. "
                    + "The file {} is not exist and start index is should equal to 0.",
                startIndex,
                recordFile.getAbsolutePath());
            return new CheckResult(false, "0");
          }
        }

        if (Long.parseLong(index) != startIndex) {
          logger.error(
              "The start index {} of data sync is not valid. "
                  + "The start index of the file {} should equal to {}.",
              startIndex,
              recordFile.getAbsolutePath(),
              index);
          return new CheckResult(false, index);
        }
      }
    }

    return new CheckResult(true, "0");
  }

  // endregion

  // region Interfaces and Implementation of RPC Handler

  public TSStatus handshake(TSyncIdentityInfo identityInfo) {
    logger.debug("Invoke handshake method from client ip = {}", identityInfo.address);
    // check ip address
    if (!verifyIPSegment(config.getIpWhiteList(), identityInfo.address)) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR,
          "Sender IP is not in the white list of receiver IP and synchronization tasks are not allowed.");
    }
    // Version check
    if (!config.getIoTDBMajorVersion(identityInfo.version).equals(config.getIoTDBMajorVersion())) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPESERVER_ERROR,
          String.format(
              "Version mismatch: the sender <%s>, the receiver <%s>",
              identityInfo.version, config.getIoTDBVersion()));
    }

    if (!new File(SyncPathUtil.getFileDataDirPath(identityInfo)).exists()) {
      new File(SyncPathUtil.getFileDataDirPath(identityInfo)).mkdirs();
    }
    createConnection(identityInfo);
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

  public TSStatus transportData(TSyncTransportMetaInfo metaInfo, ByteBuffer buff, ByteBuffer digest)
      throws TException {
    TSyncIdentityInfo identityInfo = getCurrentTSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    logger.debug("Invoke transportData method from client ip = {}", identityInfo.address);

    String fileDir = SyncPathUtil.getFileDataDirPath(identityInfo);
    TSyncTransportType type = metaInfo.type;
    String fileName = metaInfo.fileName;
    long startIndex = metaInfo.startIndex;

    // Check file start index valid
    if (type == TSyncTransportType.FILE) {
      try {
        CheckResult result = checkStartIndexValid(new File(fileDir, fileName), startIndex);
        if (!result.isResult()) {
          return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_REBASE, result.getIndex());
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
        return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_ERROR, e.getMessage());
      }
    }

    // Check buff digest
    int pos = buff.position();
    MessageDigest messageDigest = null;
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      logger.error(e.getMessage());
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_ERROR, e.getMessage());
    }
    messageDigest.update(buff);
    byte[] digestBytes = new byte[digest.capacity()];
    digest.get(digestBytes);
    if (!Arrays.equals(messageDigest.digest(), digestBytes)) {
      return RpcUtils.getStatus(TSStatusCode.SYNC_FILE_RETRY, "Data digest check error");
    }

    if (type != TSyncTransportType.FILE) {
      buff.position(pos);
      int length = buff.capacity();
      byte[] byteArray = new byte[length];
      buff.get(byteArray);
      try {
        PipeData pipeData = PipeData.createPipeData(byteArray);
        if (type == TSyncTransportType.TSFILE) {
          // Do with file
          handleTsFilePipeData((TsFilePipeData) pipeData, fileDir);
        }
        logger.info(
            "Start load pipeData with serialize number {} and type {},value={}",
            pipeData.getSerialNumber(),
            pipeData.getType(),
            pipeData);
        pipeData.createLoader().load();
        logger.info(
            "Load pipeData with serialize number {} successfully.", pipeData.getSerialNumber());
      } catch (IOException | IllegalPathException e) {
        logger.error("Pipe data transport error, {}", e.getMessage());
        return RpcUtils.getStatus(
            TSStatusCode.SYNC_FILE_RETRY, "Data digest transport error " + e.getMessage());
      } catch (PipeDataLoadException e) {
        logger.error("Fail to load pipeData because {}.", e.getMessage());
        return RpcUtils.getStatus(
            TSStatusCode.SYNC_FILE_ERROR, "Fail to load pipeData because " + e.getMessage());
      }
    } else {
      // Write buff to {file}.patch
      buff.position(pos);
      File file = new File(fileDir, fileName + PATCH_SUFFIX);
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
        randomAccessFile.seek(startIndex);
        int length = buff.capacity();
        byte[] byteArray = new byte[length];
        buff.get(byteArray);
        randomAccessFile.write(byteArray);
        writeRecordFile(new File(fileDir, fileName + RECORD_SUFFIX), startIndex + length);
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
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
  }

  public TSStatus checkFileDigest(TSyncTransportMetaInfo metaInfo, ByteBuffer digest)
      throws TException {
    TSyncIdentityInfo identityInfo = getCurrentTSyncIdentityInfo();
    if (identityInfo == null) {
      throw new TException("Thrift connection is not alive.");
    }
    logger.debug("Invoke checkFileDigest method from client ip = {}", identityInfo.address);
    String fileDir = SyncPathUtil.getFileDataDirPath(identityInfo);
    synchronized (fileDir.intern()) {
      String fileName = metaInfo.fileName;
      MessageDigest messageDigest = null;
      try {
        messageDigest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        logger.error(e.getMessage());
        return RpcUtils.getStatus(TSStatusCode.PIPESERVER_ERROR, e.getMessage());
      }

      try (InputStream inputStream =
          new FileInputStream(new File(fileDir, fileName + PATCH_SUFFIX))) {
        byte[] block = new byte[DATA_CHUNK_SIZE];
        int length;
        while ((length = inputStream.read(block)) > 0) {
          messageDigest.update(block, 0, length);
        }

        String localDigest = (new BigInteger(1, messageDigest.digest())).toString(16);
        byte[] digestBytes = new byte[digest.capacity()];
        digest.get(digestBytes);
        if (!Arrays.equals(messageDigest.digest(), digestBytes)) {
          logger.error(
              "The file {} digest check error. "
                  + "The local digest is {} (should be equal to {}).",
              fileName,
              localDigest,
              digest);
          new File(fileDir, fileName + RECORD_SUFFIX).delete();
          return RpcUtils.getStatus(TSStatusCode.PIPESERVER_ERROR, "File digest check error.");
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
        return RpcUtils.getStatus(TSStatusCode.PIPESERVER_ERROR, e.getMessage());
      }

      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "");
    }
  }

  private void writeRecordFile(File recordFile, long position) throws IOException {
    File tmpFile = new File(recordFile.getAbsolutePath() + ".tmp");
    FileWriter fileWriter = new FileWriter(tmpFile, false);
    fileWriter.write(String.valueOf(position));
    fileWriter.close();
    Files.move(tmpFile.toPath(), recordFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
        dir.listFiles((dir1, name) -> name.startsWith(tsFileName) && name.endsWith(PATCH_SUFFIX));
    if (targetFiles != null) {
      for (File targetFile : targetFiles) {
        File newFile =
            new File(
                dir,
                targetFile
                    .getName()
                    .substring(0, targetFile.getName().length() - PATCH_SUFFIX.length()));
        targetFile.renameTo(newFile);
      }
    }
    tsFilePipeData.setParentDirPath(dir.getAbsolutePath());
    File recordFile = new File(fileDir, tsFileName + RECORD_SUFFIX);
    try {
      Files.deleteIfExists(recordFile.toPath());
    } catch (IOException e) {
      logger.warn(
          String.format("Delete record file %s error, because %s.", recordFile.getPath(), e));
    }
  }

  // endregion

  // region Interfaces and Implementation of Connection Manager

  /** Check if the connection is legally established by handshaking */
  private boolean checkConnection() {
    return currentConnectionId.get() != null;
  }

  /**
   * Get current TSyncIdentityInfo
   *
   * @return null if connection has been exited
   */
  private TSyncIdentityInfo getCurrentTSyncIdentityInfo() {
    Long id = currentConnectionId.get();
    if (id != null) {
      return connectionIdToIdentityInfoMap.get(id);
    } else {
      return null;
    }
  }

  private void createConnection(TSyncIdentityInfo identityInfo) {
    long connectionId = connectionIdGenerator.incrementAndGet();
    currentConnectionId.set(connectionId);
    connectionIdToIdentityInfoMap.put(connectionId, identityInfo);
  }

  /**
   * release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    if (checkConnection()) {
      long id = currentConnectionId.get();
      connectionIdToIdentityInfoMap.remove(id);
      currentConnectionId.remove();
    }
  }

  public List<TSyncIdentityInfo> getAllTSyncIdentityInfos() {
    return new ArrayList<>(connectionIdToIdentityInfoMap.values());
  }

  // endregion
}
