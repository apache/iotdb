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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.sync.SyncConstant.DATA_CHUNK_SIZE;

public class IoTDBSinkTransportClient implements ITransportClient {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBSinkTransportClient.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int TRANSFER_BUFFER_SIZE_IN_BYTES = 1 * 1024 * 1024;

  private final ClientWrapper serviceClient;

  /* remote IP address*/
  private final String ipAddress;
  /* remote port */
  private final int port;
  /* local IP address*/
  private final String localIP;

  private final Pipe pipe;

  /**
   * @param pipe sync task
   * @param ipAddress remote ip address
   * @param port remote port
   * @param localIP local ip address
   */
  public IoTDBSinkTransportClient(Pipe pipe, String ipAddress, int port, String localIP) {
    RpcTransportFactory.setThriftMaxFrameSize(config.getThriftMaxFrameSize());
    this.pipe = pipe;
    this.ipAddress = ipAddress;
    this.port = port;
    this.localIP = localIP;
    serviceClient = new ClientWrapper(pipe, ipAddress, port, localIP);
  }

  /**
   * Create thrift connection to receiver. (1) register pipe message, including pipeName, localIp
   * and createTime (2) check IoTDB version to make sure compatibility
   *
   * @return true if success; false if failed MaxNumberOfSyncFileRetry times.
   * @throws SyncConnectionException cannot create connection to receiver
   */
  public synchronized boolean handshake() throws SyncConnectionException {
    return serviceClient.handshakeWithVersion();
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
      }
    }
    try {
      transportPipeData(pipeData);
    } catch (SyncConnectionException e) {
      // handshake and retry
      try {
        if (!handshake()) {
          logger.error(
              String.format(
                  "Handshake to receiver %s:%d error when transfer pipe data %s.",
                  ipAddress, port, pipeData));
          return false;
        }
      } catch (SyncConnectionException syncConnectionException) {
        logger.error(
            String.format(
                "Reconnect to receiver %s:%d error when transfer pipe data %s.",
                ipAddress, port, pipeData));
        throw new SyncConnectionException(
            String.format("Reconnect to receiver error when transferring pipedata %s.", pipeData));
      }
    }
    return true;
  }

  /** Transfer data of a tsfile to the receiver. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void transportSingleFile(File file) throws SyncConnectionException {
    int retryCount = 0;
    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumberOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format("Connect to receiver error when transferring file %s.", file.getName()));
      }

      try {
        transportSingleFilePieceByPiece(file);
        break;
      } catch (SyncConnectionException e) {
        // handshake and retry
        try {
          if (!handshake()) {
            throw new SyncConnectionException(
                String.format(
                    "Handshake with receiver error when transferring file %s.", file.getName()));
          }
        } catch (SyncConnectionException syncConnectionException) {
          throw new SyncConnectionException(
              String.format(
                  "Connect to receiver error when transferring file %s.", file.getName()));
        }
      }
    }

    logger.info("Receiver has received {} successfully.", file.getAbsoluteFile());
  }

  private void transportSingleFilePieceByPiece(File file) throws SyncConnectionException {
    // Cut the file into pieces to send
    long position = 0;
    long limit = getFileSizeLimit(file);

    // Try small piece to rebase the file position.
    byte[] buffer = new byte[TRANSFER_BUFFER_SIZE_IN_BYTES];

    outer:
    while (true) {

      // Normal piece.
      if (position != 0L && buffer.length != DATA_CHUNK_SIZE) {
        buffer = new byte[DATA_CHUNK_SIZE];
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
          ByteBuffer buffToSend = ByteBuffer.wrap(buffer, 0, dataLength);
          TSyncTransportMetaInfo metaInfo = new TSyncTransportMetaInfo(file.getName(), position);

          TSStatus status = null;
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
              status = serviceClient.getClient().transportFile(metaInfo, buffToSend);
            } catch (TException e) {
              // retry
              logger.error("TException happened! ", e);
              continue;
            }
            break;
          }

          if (status.code == TSStatusCode.SYNC_FILE_REBASE.getStatusCode()) {
            position = Long.parseLong(status.message);
            continue outer;
          } else if (status.code == TSStatusCode.SYNC_FILE_RETRY.getStatusCode()) {
            logger.info(
                "Receiver failed to receive data from {} because {}, retry.",
                file.getAbsoluteFile(),
                status.message);
            continue outer;
          } else if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            logger.info(
                "Receiver failed to receive data from {} because {}, abort.",
                file.getAbsoluteFile(),
                status.message);
            throw new SyncConnectionException(status.message);
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

  private void transportPipeData(PipeData pipeData) throws SyncConnectionException {
    try {
      byte[] buffer = pipeData.serialize();
      ByteBuffer buffToSend = ByteBuffer.wrap(buffer);
      TSStatus status = serviceClient.getClient().transportPipeData(buffToSend);
      if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.info("Transport PipeData {} Successfully", pipeData);
      } else if (status.code == TSStatusCode.PIPESERVER_ERROR.getStatusCode()) {
        logger.error("Receiver failed to load PipeData {}, skip it.", pipeData);
      }
    } catch (IOException e) {
      // TODO: internal error
      logger.error("Exception happened!", e);
    } catch (TException e) {
      throw new SyncConnectionException(e);
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
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!handshake()) {
            SyncService.getInstance()
                .receiveMsg(
                    PipeMessage.MsgType.ERROR,
                    String.format("Can not handshake with %s:%d.", ipAddress, port));
          }
          while (!Thread.currentThread().isInterrupted()) {
            PipeData pipeData = pipe.take();
            if (!senderTransport(pipeData)) {
              logger.error(String.format("Can not transfer pipedata %s, skip it.", pipeData));
              // can do something.
              SyncService.getInstance()
                  .receiveMsg(
                      PipeMessage.MsgType.WARN,
                      String.format(
                          "Transfer piepdata %s error, skip it.", pipeData.getSerialNumber()));
              continue;
            }
            pipe.commit();
          }
        } catch (SyncConnectionException e) {
          logger.error(
              String.format("Connect to receiver %s:%d error, because %s.", ipAddress, port, e));
          // TODO: wait and retry
        }
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by pipe, exit transport.");
    } finally {
      close();
    }
  }

  public void close() {
    serviceClient.close();
  }
}
