/*
 * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.consensus.natraft.protocol.log.snapshot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DirectorySnapshot extends Snapshot {

  private static final Logger logger = LoggerFactory.getLogger(DirectorySnapshot.class);
  private File directory;
  private List<Path> filePaths;
  private TEndPoint source;
  private String memberName;

  public DirectorySnapshot() {}

  public DirectorySnapshot(File directory, List<Path> filePaths, List<Peer> peers) {
    this.directory = directory;
    this.filePaths = filePaths;
    this.currNodes = peers;
  }

  @Override
  public ByteBuffer serialize() {

    PublicBAOS baos = new PublicBAOS();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);

    serializeBase(dataOutputStream);

    try {
      byte[] bytes = directory.getAbsolutePath().getBytes(StandardCharsets.UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
      dataOutputStream.writeInt(filePaths.size());
      for (Path filePath : filePaths) {
        byte[] pathBytes = filePath.toString().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(pathBytes.length);
        dataOutputStream.write(pathBytes);
      }
    } catch (IOException e) {
      // unreachable
    }
    return ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    deserializeBase(buffer);

    int size = buffer.getInt();
    byte[] bytes = new byte[size];
    buffer.get(bytes);
    directory = new File(new String(bytes, StandardCharsets.UTF_8));

    int fileNum = buffer.getInt();
    filePaths = new ArrayList<>(fileNum);
    for (int i = 0; i < fileNum; i++) {
      int pathSize = buffer.getInt();
      bytes = new byte[pathSize];
      buffer.get(bytes);
      filePaths.add(new File(new String(bytes, StandardCharsets.UTF_8)).toPath());
    }
  }

  @Override
  public TSStatus install(RaftMember member) {
    String localSnapshotTmpDirPath = member.getLocalSnapshotTmpDir(directory.getName());
    TSStatus tsStatus = downloadSnapshot(localSnapshotTmpDirPath, member);
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return tsStatus;
    }
    member.getStateMachine().loadSnapshot(new File(localSnapshotTmpDirPath));
    member.getLogManager().applySnapshot(this);
    FileUtils.deleteDirectory(new File(localSnapshotTmpDirPath));
    return tsStatus;
  }

  public void setSource(TEndPoint source) {
    this.source = source;
  }

  public void setMemberName(String memberName) {
    this.memberName = memberName;
  }

  private TSStatus downloadSnapshot(String localSnapshotTmpDirPath, RaftMember member) {
    File localSnapshotTmpDir = new File(localSnapshotTmpDirPath);
    localSnapshotTmpDir.mkdirs();
    logger.info(
        "Downloading {} files from {} to {}", filePaths.size(), source, localSnapshotTmpDir);
    String snapshotName = directory.getName();
    for (Path path : filePaths) {
      logger.info("Downloading {} of {}", path, snapshotName);
      String pathStr = path.toString();
      String relativePath =
          pathStr.substring(pathStr.indexOf(snapshotName) + snapshotName.length());
      String targetFilePath = localSnapshotTmpDirPath + File.separator + relativePath;
      TSStatus tsStatus = downloadFile(pathStr, targetFilePath, member);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.warn("Downloading failed: {}", tsStatus);
        return tsStatus;
      }
      logger.info("File downloaded");
    }
    logger.info("Downloaded {} files from {}", filePaths.size(), source);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus downloadFile(String sourceFilePath, String targetFilePath, RaftMember member) {
    int pullFileRetry = 5;
    File targetFile = new File(targetFilePath);
    targetFile.getParentFile().mkdirs();
    for (int i = 0; i < pullFileRetry; i++) {
      try (BufferedOutputStream bufferedOutputStream =
          new BufferedOutputStream(Files.newOutputStream(targetFile.toPath()))) {
        downloadFileAsync(source, sourceFilePath, bufferedOutputStream, member);

        if (logger.isInfoEnabled()) {
          logger.info(
              "{}: remote file {} is pulled at {}, length: {}",
              memberName,
              sourceFilePath,
              targetFilePath,
              new File(targetFilePath).length());
        }
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } catch (TException e) {
        logger.warn(
            "{}: Cannot pull file {} from {}, wait 5s to retry",
            memberName,
            sourceFilePath,
            source,
            e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(
            "{}: Pulling file {} from {} interrupted", memberName, sourceFilePath, source, e);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
            .setMessage("Interrupted");
      } catch (IOException e) {
        logger.warn("{}: Pulling file {} from {} failed", memberName, sourceFilePath, source, e);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
            .setMessage(e.getMessage());
      }

      try {
        Files.delete(new File(targetFilePath).toPath());
        Thread.sleep(5000);
      } catch (IOException e) {
        logger.warn("Cannot delete file when pulling {} from {} failed", sourceFilePath, source);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        logger.warn(
            "{}: Pulling file {} from {} interrupted", memberName, sourceFilePath, source, ex);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
            .setMessage("Interrupted");
      }
      // next try
    }
    return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
        .setMessage(String.format("Cannot pull file %s from %s", sourceFilePath, source));
  }

  private void downloadFileAsync(
      TEndPoint node, String remotePath, OutputStream dest, RaftMember member)
      throws IOException, TException, InterruptedException {
    long offset = 0;
    // TODO-Cluster: use elaborate downloading techniques
    int fetchSize = 4 * 1024 * 1024;

    while (true) {
      AsyncRaftServiceClient client = member.getClient(node);
      if (client == null) {
        throw new IOException("No available client for " + node.toString());
      }
      ByteBuffer buffer;
      buffer = SyncClientAdaptor.readFile(client, remotePath, offset, fetchSize);
      int len = writeBuffer(buffer, dest);
      if (len == 0) {
        break;
      }
      offset += len;
    }
    dest.flush();
  }

  private int writeBuffer(ByteBuffer buffer, OutputStream dest) throws IOException {
    if (buffer == null || buffer.limit() - buffer.position() == 0) {
      return 0;
    }

    // notice: the buffer returned by thrift is a slice of a larger buffer which contains
    // the whole response, so buffer.position() is not 0 initially and buffer.limit() is
    // not the size of the downloaded chunk
    dest.write(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.remaining());
    return buffer.remaining();
  }
}
