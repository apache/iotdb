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
package org.apache.iotdb.db.newsync.transport.server;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.pipedata.queue.PipeDataQueueFactory;
import org.apache.iotdb.db.newsync.receiver.ReceiverService;
import org.apache.iotdb.service.transport.thrift.IdentityInfo;
import org.apache.iotdb.service.transport.thrift.MetaInfo;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
import org.apache.iotdb.service.transport.thrift.TransportService;
import org.apache.iotdb.service.transport.thrift.TransportStatus;
import org.apache.iotdb.service.transport.thrift.Type;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.Arrays;

import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.CONFLICT_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.ERROR_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.REBASE_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.RETRY_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.SUCCESS_CODE;
import static org.apache.iotdb.db.sync.conf.SyncConstant.DATA_CHUNK_SIZE;

public class TransportServiceImpl implements TransportService.Iface {
  private static Logger logger = LoggerFactory.getLogger(TransportServiceImpl.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String RECORD_SUFFIX = ".record";
  private static final String PATCH_SUFFIX = ".patch";

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

  @Override
  public TransportStatus handshake(IdentityInfo identityInfo) throws TException {
    logger.debug("Invoke handshake method from client ip = {}", identityInfo.address);
    // check ip address
    if (!verifyIPSegment(config.getIpWhiteList(), identityInfo.address)) {
      return new TransportStatus(
          ERROR_CODE,
          "Sender IP is not in the white list of receiver IP and synchronization tasks are not allowed.");
    }
    // Version check
    if (!config.getIoTDBMajorVersion(identityInfo.version).equals(config.getIoTDBMajorVersion())) {
      return new TransportStatus(
          ERROR_CODE,
          String.format(
              "Version mismatch: the sender <%s>, the receiver <%s>",
              identityInfo.version, config.getIoTDBVersion()));
    }

    if (!new File(getFileDataDirPath(identityInfo)).exists()) {
      new File(getFileDataDirPath(identityInfo)).mkdirs();
    }
    return new TransportStatus(SUCCESS_CODE, "");
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

  @Override
  public TransportStatus transportData(
      IdentityInfo identityInfo, MetaInfo metaInfo, ByteBuffer buff, ByteBuffer digest) {
    logger.debug("Invoke transportData method from client ip = {}", identityInfo.address);

    String fileDir = getFileDataDirPath(identityInfo);
    Type type = metaInfo.type;
    String fileName = metaInfo.fileName;
    long startIndex = metaInfo.startIndex;

    // Check file start index valid
    if (type == Type.FILE) {
      try {
        CheckResult result = checkStartIndexValid(new File(fileDir, fileName), startIndex);
        if (!result.isResult()) {
          return new TransportStatus(REBASE_CODE, result.getIndex());
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }
    }

    // Check buff digest
    int pos = buff.position();
    MessageDigest messageDigest = null;
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      logger.error(e.getMessage());
      return new TransportStatus(ERROR_CODE, e.getMessage());
    }
    messageDigest.update(buff);
    byte[] digestBytes = new byte[digest.capacity()];
    digest.get(digestBytes);
    if (!Arrays.equals(messageDigest.digest(), digestBytes)) {
      return new TransportStatus(RETRY_CODE, "Data digest check error, retry.");
    }

    if (type != Type.FILE) {
      buff.position(pos);
      int length = buff.capacity();
      byte[] byteArray = new byte[length];
      buff.get(byteArray);
      try {
        PipeData pipeData = PipeData.deserialize(byteArray);
        if (type == Type.TSFILE) {
          // Do with file
          handleTsFilePipeData((TsFilePipeData) pipeData, fileDir);
        }
        PipeDataQueueFactory.getBufferedPipeDataQueue(getPipeLogDirPath(identityInfo))
            .offer(pipeData);
      } catch (IOException | IllegalPathException e) {
        logger.error("Pipe data transport error, {}", e.getMessage());
        return new TransportStatus(RETRY_CODE, "Data digest transport error " + e.getMessage());
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
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }
    }
    return new TransportStatus(SUCCESS_CODE, "");
  }

  @Override
  public TransportStatus checkFileDigest(
      IdentityInfo identityInfo, MetaInfo metaInfo, ByteBuffer digest) throws TException {
    logger.debug("Invoke checkFileDigest method from client ip = {}", identityInfo.address);

    String fileDir = getFileDataDirPath(identityInfo);
    synchronized (fileDir.intern()) {
      String fileName = metaInfo.fileName;
      MessageDigest messageDigest = null;
      try {
        messageDigest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        logger.error(e.getMessage());
        return new TransportStatus(ERROR_CODE, e.getMessage());
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
          return new TransportStatus(CONFLICT_CODE, "File digest check error.");
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }

      return new TransportStatus(SUCCESS_CODE, "");
    }
  }

  @Override
  public SyncResponse heartbeat(IdentityInfo identityInfo, SyncRequest syncRequest)
      throws TException {
    return ReceiverService.getInstance().recMsg(syncRequest);
  }

  private void writeRecordFile(File recordFile, long position) throws IOException {
    File tmpFile = new File(recordFile.getAbsolutePath() + ".tmp");
    FileWriter fileWriter = new FileWriter(tmpFile, false);
    fileWriter.write(String.valueOf(position));
    fileWriter.close();
    Files.move(tmpFile.toPath(), recordFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    // TODO: Handle client exit here.
    // do nothing now
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

  private String getFileDataDirPath(IdentityInfo identityInfo) {
    return SyncPathUtil.getReceiverFileDataDir(
        identityInfo.getPipeName(), identityInfo.getAddress(), identityInfo.getCreateTime());
  }

  private String getPipeLogDirPath(IdentityInfo identityInfo) {
    return SyncPathUtil.getReceiverPipeLogDir(
        identityInfo.getPipeName(), identityInfo.getAddress(), identityInfo.getCreateTime());
  }
}
