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
package org.apache.iotdb.db.sync.transport.client;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.service.SenderService;
import org.apache.iotdb.db.sync.transport.conf.TransportConstant;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.service.transport.thrift.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.apache.iotdb.db.sync.transport.conf.TransportConfig.isCheckFileDegistAgain;
import static org.apache.iotdb.db.sync.transport.conf.TransportConstant.REBASE_CODE;
import static org.apache.iotdb.db.sync.transport.conf.TransportConstant.RETRY_CODE;
import static org.apache.iotdb.db.sync.transport.conf.TransportConstant.SUCCESS_CODE;

public class TransportClient implements ITransportClient {

  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int TIMEOUT_MS = 2000_000;

  private static final int TRANSFER_BUFFER_SIZE_IN_BYTES = 1 * 1024 * 1024;

  private TTransport transport = null;

  private TransportService.Client serviceClient = null;

  private String ipAddress;

  private int port;

  private IdentityInfo identityInfo = null;

  private Pipe pipe;

  public TransportClient(Pipe pipe, String ipAddress, int port) {
    RpcTransportFactory.setThriftMaxFrameSize(config.getThriftMaxFrameSize());

    this.pipe = pipe;
    this.ipAddress = ipAddress;
    this.port = port;
  }

  public boolean handshake() {
    int handshakeCounter = 0;
    try {
      while (!handshakeWithVersion()) {
        handshakeCounter++;
        if (handshakeCounter > config.getMaxNumberOfSyncFileRetry()) {
          logger.error(
              String.format(
                  "Handshake failed %s times! Check network.",
                  config.getMaxNumberOfSyncFileRetry()));
          return false;
        }
        logger.info(
            String.format(
                "Handshake error, retry %d/%d.",
                handshakeCounter, config.getMaxNumberOfSyncFileRetry()));
      }
    } catch (SyncConnectionException e) {
      logger.error(String.format("Handshake failed and can not retry, because %s.", e), e);
      return false;
    }
    return true;
  }

  private boolean handshakeWithVersion() throws SyncConnectionException {
    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    try {
      transport = RpcTransportFactory.INSTANCE.getTransport(ipAddress, port, TIMEOUT_MS);
      TProtocol protocol;
      if (config.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      serviceClient = new TransportService.Client(protocol);

      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }

      identityInfo =
          new IdentityInfo(
              InetAddress.getLocalHost().getHostAddress(),
              pipe.getName(),
              pipe.getCreateTime(),
              config.getIoTDBMajorVersion());
      TransportStatus status = serviceClient.handshake(identityInfo);
      if (status.code != SUCCESS_CODE) {
        throw new SyncConnectionException(
            "The receiver rejected the synchronization task because " + status.msg);
      }
    } catch (TException e) {
      logger.warn("Cannot connect to the receiver. ", e);
      return false;
    } catch (UnknownHostException e) {
      logger.warn("Cannot confirm identity with the receiver. ", e);
      throw new SyncConnectionException(String.format("Get local host error, because %s.", e), e);
    }
    return true;
  }

  public boolean senderTransport(PipeData pipeData) throws SyncConnectionException {
    if (pipeData instanceof TsFilePipeData) {
      try {
        for (File file : ((TsFilePipeData) pipeData).getTsFiles(true)) {
          transportSingleFile(file);
        }
      } catch (IOException e) {
        logger.error(String.format("Get tsfiles error, because %s.", e), e);
        return false;
      } catch (NoSuchAlgorithmException e) {
        logger.error(String.format("Wrong message digest, because %s.", e), e);
        return false;
      }
    }

    int retryCount = 0;

    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        logger.error(
            String.format("After %s tries, stop the transport of current pipeData!", retryCount));
        throw new SyncConnectionException(
            String.format("Can not connect to receiver when transferring pipedata %s.", pipeData));
      }

      try {
        transportPipeData(pipeData);
        logger.info(String.format("Finish pipeData %s transport.", pipeData));
        break;
      } catch (SyncConnectionException e) {
        // handshake and retry
        if (!handshake()) {
          logger.error(
              String.format(
                  "Reconnect to receiver %s:%d error when transfer pipe data %s.",
                  ipAddress, port, pipeData));
          throw new SyncConnectionException(
              String.format(
                  "Reconnect to receiver error when transferring pipedata %s.", pipeData));
        }
      } catch (NoSuchAlgorithmException e) {
        logger.error("Transport failed. ", e);
        return false;
      }
    }
    return true;
  }

  /** Transfer data of a tsfile to the receiver. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void transportSingleFile(File file)
      throws SyncConnectionException, NoSuchAlgorithmException {

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

    int retryCount = 0;
    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format("Connect to receiver error when transferring file %s.", file.getName()));
      }

      try {
        transportSingleFilePieceByPiece(file, messageDigest);

        if (isCheckFileDegistAgain) {
          // Check file digest as entirety.
          try {
            if (!checkFileDigest(file, messageDigest)) {
              continue;
            }
          } catch (IOException e) {
            logger.warn(
                String.format(
                    "Read from disk to make digest error, skip check file %s, because %s.",
                    file.getName(), e));
          }
        }
        break;
      } catch (SyncConnectionException e) {
        if (!handshake()) {
          throw new SyncConnectionException(
              String.format(
                  "Connect to receiver error when transferring file %s.", file.getName()));
        }
      }
    }

    logger.info("Receiver has received {} successfully.", file.getAbsoluteFile());
  }

  private void transportSingleFilePieceByPiece(File file, MessageDigest messageDigest)
      throws SyncConnectionException {

    // Cut the file into pieces to send
    long position = 0;

    long limit = getFileSizeLimit(file);

    // Try small piece to rebase the file position.
    byte[] buffer = new byte[TRANSFER_BUFFER_SIZE_IN_BYTES];

    outer:
    while (true) {

      // Normal piece.
      if (position != 0L && buffer.length != TransportConstant.DATA_CHUNK_SIZE) {
        buffer = new byte[TransportConstant.DATA_CHUNK_SIZE];
      }

      int dataLength;
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
        if (limit <= position) {
          break;
        }
        randomAccessFile.seek(position);
        while ((dataLength =
                randomAccessFile.read(buffer, 0, Math.min(buffer.length, (int) (limit - position))))
            != -1) {
          messageDigest.reset();
          messageDigest.update(buffer, 0, dataLength);
          ByteBuffer buffToSend = ByteBuffer.wrap(buffer, 0, dataLength);
          MetaInfo metaInfo = new MetaInfo(Type.FILE, file.getName(), position);

          TransportStatus status = null;
          int retryCount = 0;
          while (true) {
            retryCount++;
            if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
              throw new SyncConnectionException(
                  String.format(
                      "Can not sync file %s after %s tries.",
                      file.getAbsoluteFile(), config.getMaxNumberOfSyncFileRetry()));
            }
            try {
              status =
                  serviceClient.transportData(
                      metaInfo, buffToSend, ByteBuffer.wrap(messageDigest.digest()));
            } catch (TException e) {
              // retry
              logger.error("TException happened! ", e);
              continue;
            }
            break;
          }

          if (status.code == REBASE_CODE) {
            position = Long.parseLong(status.msg);
            continue outer;
          } else if (status.code == RETRY_CODE) {
            logger.info(
                "Receiver failed to receive data from {} because {}, retry.",
                file.getAbsoluteFile(),
                status.msg);
            continue outer;
          } else if (status.code != SUCCESS_CODE) {
            logger.info(
                "Receiver failed to receive data from {} because {}, abort.",
                file.getAbsoluteFile(),
                status.msg);
            throw new SyncConnectionException(status.msg);
          } else { // Success
            position += dataLength;
            if (position >= limit) {
              break;
            }
          }
        }
      } catch (IOException e) {
        // retry
        logger.error("IOException happened! ", e);
      } catch (SyncConnectionException e) {
        logger.error("Cannot sync data with receiver. ", e);
        throw e;
      }
    }
  }

  private long getFileSizeLimit(File file) {
    File offset = new File(file.getPath() + SyncConstant.MODS_OFFSET_FILE_SUFFIX);
    if (offset.exists()) {
      try (BufferedReader br = new BufferedReader(new FileReader(offset))) {
        return Long.parseLong(br.readLine());
      } catch (IOException e) {
        logger.error(
            String.format("Deserialize offset of file %s error, because %s.", file.getPath(), e));
      }
    }
    return file.length();
  }

  private boolean checkFileDigest(File file, MessageDigest messageDigest)
      throws SyncConnectionException, IOException {
    messageDigest.reset();
    try (InputStream inputStream = new FileInputStream(file)) {
      byte[] block = new byte[TransportConstant.DATA_CHUNK_SIZE];
      int length;
      while ((length = inputStream.read(block)) > 0) {
        messageDigest.update(block, 0, length);
      }
    }

    MetaInfo metaInfo = new MetaInfo(Type.FILE, file.getName(), 0);
    TransportStatus status;
    int retryCount = 0;

    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format(
                "Can not sync file %s after %s tries.",
                file.getAbsoluteFile(), config.getMaxNumberOfSyncFileRetry()));
      }
      try {
        status = serviceClient.checkFileDigest(metaInfo, ByteBuffer.wrap(messageDigest.digest()));
      } catch (TException e) {
        // retry
        logger.error("TException happens! ", e);
        continue;
      }
      break;
    }

    if (status.code != SUCCESS_CODE) {
      logger.error("Digest check of tsfile {} failed, retry", file.getAbsoluteFile());
      return false;
    }
    return true;
  }

  private void transportPipeData(PipeData pipeData)
      throws SyncConnectionException, NoSuchAlgorithmException {

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

    int retryCount = 0;
    while (true) {

      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format(
                "Can not sync pipe data after %s tries.", config.getMaxNumberOfSyncFileRetry()));
      }

      try {
        byte[] buffer = pipeData.serialize();
        messageDigest.reset();
        messageDigest.update(buffer);
        ByteBuffer buffToSend = ByteBuffer.wrap(buffer);

        MetaInfo metaInfo =
            new MetaInfo(Type.findByValue(pipeData.getType().ordinal()), "fileName", 0);
        TransportStatus status =
            serviceClient.transportData(
                metaInfo, buffToSend, ByteBuffer.wrap(messageDigest.digest()));

        if (status.code == SUCCESS_CODE) {
          break;
        } else {
          logger.error("Digest check of pipeData failed, retry");
        }
      } catch (IOException | TException e) {
        // retry
        logger.error("Exception happened!", e);
      }
    }
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's <code>run</code> method to be called in that separately
   * executing thread.
   *
   * <p>The general contract of the method <code>run</code> is that it may take any action
   * whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    try {
      if (!handshake()) {
        throw new SyncConnectionException(
            String.format("Handshake with receiver %s:%d error.", ipAddress, port));
      }
      SenderService.getInstance()
          .receiveMsg(
              heartbeat(
                  new SyncRequest(
                      RequestType.START,
                      pipe.getName(),
                      InetAddress.getLocalHost().getHostAddress(),
                      pipe.getCreateTime())));
      while (!Thread.currentThread().isInterrupted()) {
        PipeData pipeData = pipe.take();
        if (!senderTransport(pipeData)) {
          logger.warn(String.format("Can not transfer pipedata %s, skip it.", pipeData));
          // can do something.
          SenderService.getInstance()
              .receiveMsg(
                  new SyncResponse(
                      ResponseType.WARN,
                      SyncPathUtil.createMsg(
                          String.format("Transfer piepdata %s error, skip it.", pipeData))));
          continue;
        }
        pipe.commit();
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by pipe, exit transport.");
    } catch (SyncConnectionException | UnknownHostException e) {
      logger.error(
          String.format("Connect to receiver %s:%d error, because %s.", ipAddress, port, e));
      SenderService.getInstance()
          .receiveMsg(
              new SyncResponse(
                  ResponseType.ERROR,
                  SyncPathUtil.createMsg(
                      String.format(
                          "Can not connect to %s:%d, because %s, please check receiver and internet.",
                          ipAddress, port, e.getMessage()))));
    } finally {
      close();
    }
  }

  @Override
  public SyncResponse heartbeat(SyncRequest syncRequest) throws SyncConnectionException {
    int retryCount = 0;
    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format(
                "%s request connects to receiver %s:%d error.",
                syncRequest.type.name(), ipAddress, port));
      }

      try (TTransport heartbeatTransport =
          RpcTransportFactory.INSTANCE.getTransport(ipAddress, port, TIMEOUT_MS)) {
        TProtocol protocol;
        if (config.isRpcThriftCompressionEnable()) {
          protocol = new TCompactProtocol(heartbeatTransport);
        } else {
          protocol = new TBinaryProtocol(heartbeatTransport);
        }
        TransportService.Client heartbeatClient = new TransportService.Client(protocol);
        if (!heartbeatTransport.isOpen()) {
          heartbeatTransport.open();
        }

        return heartbeatClient.heartbeat(syncRequest);
      } catch (TException e) {
        logger.info(
            String.format(
                "Heartbeat connect to receiver %s:%d error, retry %d/%d.",
                ipAddress, port, retryCount, config.getMaxNumberOfSyncFileRetry()));
      }
    }
  }

  public void close() {
    if (transport != null) {
      transport.close();
    }
  }
}
