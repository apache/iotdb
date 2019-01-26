/**
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
package org.apache.iotdb.db.postback.sender;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.iotdb.db.postback.conf.PostBackSenderConfig;
import org.apache.iotdb.db.postback.conf.PostBackSenderDescriptor;
import org.apache.iotdb.db.postback.receiver.ServerService;
import org.apache.iotdb.db.utils.PostbackUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class is to transfer tsfiles that needs to postback to receiver.
 *
 * @author lta
 */
public class FileSenderImpl implements FileSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSenderImpl.class);
  private TTransport transport;
  private ServerService.Client clientOfServer;
  private List<String> schema = new ArrayList<>();
  private String uuid;// Mark the identity of sender
  /**
   * Mark whether connection of sender and receiver has broken down or not.
   */
  private boolean connectionOrElse;
  private PostBackSenderConfig config = PostBackSenderDescriptor.getInstance().getConfig();
  private Date lastPostBackTime = new Date(); // Mark the start time of last postback
  private boolean postBackStatus = false; // If true, postback is in execution.
  private static final String POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED = "IoTDB post back sender : postback process has failed!";
  private Map<String, Set<String>> sendingFileSnapshotList = new HashMap<>();

  private FileSenderImpl() {
  }

  public static final FileSenderImpl getInstance() {
    return TransferHolder.INSTANCE;
  }

  /**
   * Create a sender and send files to the receiver.
   *
   * @param args not used
   */
  public static void main(String[] args) throws InterruptedException {
    FileSenderImpl fileSenderImpl = new FileSenderImpl();
    fileSenderImpl.verifyPort();
    Thread monitor = new Thread(fileSenderImpl::monitorPostbackStatus);
    monitor.start();
    fileSenderImpl.timedTask();
  }

  public void setConfig(PostBackSenderConfig config) {
    this.config = config;
  }

  private void getConnection(String serverIP, int serverPort) {
    connectToReceiver(serverIP, serverPort);
    if (connectionOrElse && !transferUUID(config.getUuidPath())) {
      LOGGER.error(
          "IoTDB post back sender: Sorry! You do not have the permission to "
              + "connect to postback receiver!");
      connectionOrElse = false;

    }
  }

  /**
   * Establish a connection between the sender and the receiver.
   *
   * @param serverIp the ip address of the receiver
   * @param serverPort must be same with port receiver set.
   */
  @Override
  public void connectToReceiver(String serverIp, int serverPort) {
    transport = new TSocket(serverIp, serverPort);
    TProtocol protocol = new TBinaryProtocol(transport);
    clientOfServer = new ServerService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      LOGGER.error("IoTDB post back sender: cannot connect to server because {}",
          e.getMessage());
      connectionOrElse = false;
    }
  }

  /**
   * UUID marks the identity of sender for receiver.
   */
  @Override
  public boolean transferUUID(String uuidPath) {
    File file = new File(uuidPath);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (!file.exists()) {
      try (FileOutputStream out = new FileOutputStream(file)) {
        if (!file.createNewFile()) {
          LOGGER.error("IoTDB post back sender: cannot create file {}",
              file.getAbsoluteFile());
        }
        uuid = "PB" + UUID.randomUUID().toString().replaceAll("-", "");
        out.write(uuid.getBytes());
      } catch (Exception e) {
        LOGGER.error("IoTDB post back sender: cannot write UUID to file because {}",
            e.getMessage());
        connectionOrElse = false;
      }
    } else {
      try (BufferedReader bf = new BufferedReader((new FileReader(uuidPath)))) {
        uuid = bf.readLine();
      } catch (IOException e) {
        LOGGER.error("IoTDB post back sender: cannot read UUID from file because {}",
            e.getMessage());
        connectionOrElse = false;
      }
    }
    boolean legalConnectionOrNot = true;
    try {
      legalConnectionOrNot = clientOfServer.getUUID(uuid,
          InetAddress.getLocalHost().getHostAddress());
    } catch (TException e) {
      LOGGER.error("IoTDB post back sender: cannot send UUID to receiver because {}",
          e.getMessage());
      connectionOrElse = false;
    } catch (UnknownHostException e) {
      LOGGER.error("IoTDB post back sender: unable to get local host because {}", e.getMessage());
      legalConnectionOrNot = false;
    }
    return legalConnectionOrNot;
  }

  /**
   * Create snapshots for those sending files.
   */
  @Override
  public Set<String> makeFileSnapshot(Set<String> sendingFileList) {
    Set<String> sendingSnapshotFileList = new HashSet<>();
    try {
      for (String filePath : sendingFileList) {
        String snapshotFilePath = PostbackUtils.getSnapshotFilePath(filePath);
        sendingSnapshotFileList.add(snapshotFilePath);
        File newFile = new File(snapshotFilePath);
        if (!newFile.getParentFile().exists()) {
          newFile.getParentFile().mkdirs();
        }
        Path link = FileSystems.getDefault().getPath(snapshotFilePath);
        Path target = FileSystems.getDefault().getPath(filePath);
        Files.createLink(link, target);
      }
    } catch (IOException e) {
      LOGGER.error("IoTDB post back sender: can not make fileSnapshot because {}", e.getMessage());
    }
    return sendingSnapshotFileList;
  }

  /**
   * Transfer data of a storage group to receiver.
   *
   * @param fileSnapshotList list of sending snapshot files in a storage group.
   */
  @Override
  public void transferData(Set<String> fileSnapshotList) {
    try {
      int num = 0;
      for (String snapshotFilePath : fileSnapshotList) {
        num++;
        File file = new File(snapshotFilePath);
        List<String> filePathSplit = new ArrayList<>();
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
          String[] name = snapshotFilePath.split(File.separator + File.separator);
          filePathSplit.add("data");
          filePathSplit.add(name[name.length - 2]);
          filePathSplit.add(name[name.length - 1]);
        } else {
          String[] name = snapshotFilePath.split(File.separator);
          filePathSplit.add("data");
          filePathSplit.add(name[name.length - 2]);
          filePathSplit.add(name[name.length - 1]);
        }
        while (true) {
          // Send all data to receiver
          try (FileInputStream fis = new FileInputStream(file)) {
            int mBufferSize = 64 * 1024 * 1024;
            ByteArrayOutputStream bos = new ByteArrayOutputStream(mBufferSize);
            byte[] buffer = new byte[mBufferSize];
            int n;
            while ((n = fis.read(buffer)) != -1) { // cut the file into pieces to send
              bos.write(buffer, 0, n);
              ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
              bos.reset();
              clientOfServer.startReceiving(null, filePathSplit, buffToSend, 1);
            }
            bos.close();
          }

          // Get md5 of the file.
          MessageDigest md = MessageDigest.getInstance("MD5");
          try (FileInputStream fis = new FileInputStream(file)) {
            int mBufferSize = 8 * 1024 * 1024;
            byte[] buffer = new byte[mBufferSize];
            int m;
            while ((m = fis.read(buffer)) != -1) {
              md.update(buffer, 0, m);
            }
          }

          // the file is sent successfully
          String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
          String md5OfReceiver = clientOfServer.startReceiving(md5OfSender, filePathSplit,
              null, 0);
          if (md5OfSender.equals(md5OfReceiver)) {
            LOGGER.info("IoTDB sender: receiver has received {} successfully.", snapshotFilePath);
            break;
          }
        }
        LOGGER.info(String
            .format("IoTDB sender : Task of sending files to receiver has completed %d/%d.", num,
                fileSnapshotList.size()));
      }
    } catch (TException e) {
      LOGGER.error("IoTDB post back sender: cannot sending data because receiver has broken down.");
      connectionOrElse = false;
    } catch (Exception e) {
      LOGGER.error("IoTDB post back sender: cannot sending data because {}", e.getMessage());
      connectionOrElse = false;
    }
  }

  /**
   * Sending schema to receiver.
   *
   * @param schemaPath the path of the schema file.
   */
  @Override
  public void sendSchema(String schemaPath) {
    try (FileInputStream fis = new FileInputStream(new File(schemaPath))) {
      int mBufferSize = 4 * 1024 * 1024;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(mBufferSize);
      byte[] buffer = new byte[mBufferSize];
      int n;
      while ((n = fis.read(buffer)) != -1) { // cut the file into pieces to send
        bos.write(buffer, 0, n);
        ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
        bos.reset();
        // 1 represents there is still schema buffer to send.
        clientOfServer.getSchema(buffToSend, 1);
      }
      bos.close();
      // 0 represents the schema file has been transferred completely.
      clientOfServer.getSchema(null, 0);
    } catch (Exception e) {
      LOGGER.error("IoTDB post back sender : cannot send schema from mlog.txt because {}",
          e.getMessage());
      connectionOrElse = false;
    }
  }

  @Override
  public boolean afterSending() {
    boolean successOrNot = false;
    try {
      successOrNot = clientOfServer.merge();
    } catch (TException e) {
      LOGGER.error(
          "IoTDB post back sender : can not finish postback process because postback "
              + "receiver has broken down.");
      transport.close();
    }
    return successOrNot;
  }

  public List<String> getSchema() {
    return schema;
  }

  /**
   * The method is to verify whether the client port is bind or not, ensuring that only one client
   * is running.
   */
  private void verifyPort() {
    try {
      Socket socket = new Socket("localhost", config.getClientPort());
      socket.close();
      LOGGER.error("The postback client has been started!");
      System.exit(0);
    } catch (IOException e) {
      try {
        ServerSocket listenerSocket = new ServerSocket(config.getClientPort());
        Thread listener = new Thread(() -> {
          while (true) {
            try {
              listenerSocket.accept();
            } catch (IOException e12) {
              LOGGER.error("IoTDB post back sender: unable to  listen to port{}, because {}",
                  config.getClientPort(), e12.getMessage());
            }
          }
        });
        listener.start();
      } catch (IOException e1) {
        LOGGER.error("IoTDB post back sender: unable to listen to port{}, because {}",
            config.getClientPort(), e1.getMessage());
      }
    }
  }

  /**
   * Monitor postback status.
   */
  private void monitorPostbackStatus() {
    Date oldTime = new Date();
    while (true) {
      Date currentTime = new Date();
      if (currentTime.getTime() / 1000 == oldTime.getTime() / 1000) {
        continue;
      }
      if ((currentTime.getTime() - lastPostBackTime.getTime())
          % (config.getUploadCycleInSeconds() * 1000) == 0) {
        oldTime = currentTime;
        if (postBackStatus) {
          LOGGER.info("IoTDB post back sender : postback process is in execution!");
        }
      }
    }
  }

  /**
   * Start postback task in a certain time.
   */
  public void timedTask() throws InterruptedException {
    postback();
    lastPostBackTime = new Date();
    Date currentTime;
    while (true) {
      Thread.sleep(2000);
      currentTime = new Date();
      if (currentTime.getTime() - lastPostBackTime.getTime()
          > config.getUploadCycleInSeconds() * 1000) {
        lastPostBackTime = currentTime;
        postback();
      }
    }
  }

  /**
   * Execute a postback task.
   */
  @Override
  public void postback() {

    for (String snapshotPath : config.getSnapshotPaths()) {
      if (new File(snapshotPath).exists() && new File(snapshotPath).list().length != 0) {
        // it means that the last task of postback does not succeed! Clear the files and
        // start to postback again
        try {
          PostbackUtils.deleteFile(new File(snapshotPath));
        } catch (IOException e) {
          LOGGER.error("can not delete file {}, {}", snapshotPath, e.getMessage());
        }
      }
    }

    postBackStatus = true;
    connectionOrElse = true;

    // connect to postback server
    getConnection(config.getServerIp(), config.getServerPort());
    if (!connectionOrElse) {
      LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
      postBackStatus = false;
      return;
    }

    FileManager fileManager = FileManager.getInstance();
    fileManager.init();
    Map<String, Set<String>> sendingFileList = fileManager.getSendingFiles();
    Map<String, Set<String>> nowLocalFileList = fileManager.getCurrentLocalFiles();
    if (PostbackUtils.isEmpty(sendingFileList)) {
      LOGGER.info("IoTDB post back sender : there has no file to postback !");
      postBackStatus = false;
      return;
    }

    // create snapshot
    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
      sendingFileSnapshotList.put(entry.getKey(), makeFileSnapshot(entry.getValue()));
    }

    sendSchema(config.getSchemaPath());
    if (!connectionOrElse) {
      transport.close();
      LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
      postBackStatus = false;
      return;
    }
    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
      Set<String> sendingList = entry.getValue();
      Set<String> sendingSnapshotList = sendingFileSnapshotList.get(entry.getKey());
      if (sendingSnapshotList.isEmpty()) {
        continue;
      }
      LOGGER.info("IoTDB post back sender : postback process starts to transfer data of "
          + "storage group {}.", entry.getKey());
      try {
        clientOfServer.init(entry.getKey());
      } catch (TException e) {
        connectionOrElse = false;
        LOGGER.error("IoTDB post back sender : unable to connect to receiver because {}",
            e.getMessage());
      }
      if (!connectionOrElse) {
        transport.close();
        LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
        postBackStatus = false;
        return;
      }
      transferData(sendingSnapshotList);
      if (!connectionOrElse) {
        transport.close();
        LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
        postBackStatus = false;
        return;
      }
      if (afterSending()) {
        nowLocalFileList.get(entry.getKey()).addAll(sendingList);
        fileManager.setCurrentLocalFiles(nowLocalFileList);
        fileManager.backupNowLocalFileInfo(config.getLastFileInfo());
        LOGGER.info("IoTDB post back sender : the postBack has finished storage group {}.",
            entry.getKey());
      } else {
        LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
        postBackStatus = false;
        return;
      }
    }
    for (String snapshotPath : config.getSnapshotPaths()) {
      try {
        PostbackUtils.deleteFile(new File(snapshotPath));
      } catch (IOException e) {
        LOGGER.error(" ", e);
      }
    }
    try {
      clientOfServer.afterReceiving();
    } catch (TException e) {
      connectionOrElse = false;
      LOGGER.error("IoTDB post back sender : unable to connect to receiver because {}",
          e.getMessage());
    }
    if (!connectionOrElse) {
      transport.close();
      LOGGER.info(POST_BACK_SENDER_POSTBACK_PROCESS_HAS_FAILED);
      postBackStatus = false;
      return;
    }
    transport.close();
    LOGGER.info("IoTDB post back sender : postback process has finished!");
    postBackStatus = false;
  }

  private static class TransferHolder {

    private static final FileSenderImpl INSTANCE = new FileSenderImpl();
  }
}
