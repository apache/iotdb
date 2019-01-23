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
  private final String JDBC_DRIVER_NAME = "org.apache.iotdb.jdbc.IoTDBDriver";
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
  /**
   * Clear data after postback has finished or not.
   */
  private boolean clearOrNot = config.isClearEnable;
  private Map<String, Set<String>> sendingFileSnapshotList = new HashMap<>();

  private FileSenderImpl() {
  }

  public static final FileSenderImpl getInstance() {
    return TransferHolder.INSTANCE;
  }

  /**
   * Create a sender and send files to the receiver.
   * @param args not used
   */
  public static void main(String[] args) {
    /* TODO: is this a test method? If so, better put it somewhere else. Or the arguements should be
      adjustable and some instructions should be added.
    */
    FileSenderImpl fileSenderImpl = new FileSenderImpl();
    fileSenderImpl.verifyPort();
    Thread monitor = new Thread(new Runnable() {
      public void run() {
        fileSenderImpl.monitorPostbackStatus();
      }
    });
    monitor.start();
    fileSenderImpl.timedTask();
  }

  public void setConfig(PostBackSenderConfig config) {
    this.config = config;
  }

  private void getConnection(String serverIP, int serverPort) {
    connectToReceiver(serverIP, serverPort);
    if (connectionOrElse) {
      if (!transferUUID(config.uuidPath)) {
        LOGGER.error(
            "IoTDB post back sender: Sorry! You do not have the permission to "
                + "connect to postback receiver!");
        connectionOrElse = false;
      }
    }
  }

  /**
   * Establish a connection between the sender and the receiver.
   *
   * @param serverIp the ip address of the receiver
   * @param serverPort
   *            must be same with port receiver set.
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
    BufferedReader bf;
    FileOutputStream out;
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (!file.exists()) {
      try {
        file.createNewFile();
        uuid = "PB" + UUID.randomUUID().toString().replaceAll("-", "");
        out = new FileOutputStream(file);
        out.write(uuid.getBytes());
        out.close();
      } catch (Exception e) {
        LOGGER.error("IoTDB post back sender: cannot write UUID to file because {}",
            e.getMessage());
        connectionOrElse = false;
      }
    } else {
      try {
        bf = new BufferedReader(new FileReader(uuidPath));
        uuid = bf.readLine();
        bf.close();
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
   * @param fileSnapshotList
   *            list of sending snapshot files in a storage group.
   *
   */
  @Override
  public void startSending(Set<String> fileSnapshotList) {
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
          FileInputStream fis = new FileInputStream(file);
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
          fis.close();

          // Get md5 of the file.
          fis = new FileInputStream(file);
          MessageDigest md = MessageDigest.getInstance("MD5");
          mBufferSize = 8 * 1024 * 1024;
          buffer = new byte[mBufferSize];
          int m;
          while ((m = fis.read(buffer)) != -1) {
            md.update(buffer, 0, m);
          }
          fis.close();

          // the file is sent successfully
          String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
          String md5OfReceiver = clientOfServer.startReceiving(md5OfSender, filePathSplit,
              null, 0);
          if (md5OfSender.equals(md5OfReceiver)) {
            LOGGER.info("IoTDB sender: receiver has received {} successfully.", snapshotFilePath);
            break;
          }
        }
        LOGGER.info("IoTDB sender : Task of sending files to receiver has completed " + num + "/"
            + fileSnapshotList.size() + ".");
      }
    } catch (TException e) {
      LOGGER.error("IoTDB post back sender: cannot sending data because receiver has broken down.");
      connectionOrElse = false;
      return;
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
    try {
      FileInputStream fis = new FileInputStream(new File(schemaPath));
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
      fis.close();
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

  /**
   * Delete data of a storage group after postback process has finished.
   */
  private void deleteData(Set<String> snapshotFileList) {

    // Connection connection = null;
    // Statement statement = null;
    // //TsRandomAccessLocalFileReader input = null;
    // String deleteFormat = "delete from %s.* where time <= %s";
    // try {
    // Class.forName(JDBC_DRIVER_NAME);
    // connection = DriverManager.getConnection(
    // "jdbc:iotdb://localhost:" + TsfileDBDescriptor.getInstance().getConfig().rpcPort + "/",
    // "root","root");
    // statement = connection.createStatement();
    // int count = 0;
    //
    // for (String filePath : snapshotFileList) {
    // input = new TsRandomAccessLocalFileReader(filePath);
    // org.apache.iotdb.tsfile.read.FileReader reader = new org.apache.iotdb.tsfile.read.FileReader(
    // input);
    // Map<String, TsDevice> deviceIdMap = reader.getFileMetaData().getDeviceMap();
    // Iterator<String> it = deviceIdMap.keySet().iterator();
    // while (it.hasNext()) {
    // String key = it.next(); // key represent device
    // TsDevice deltaObj = deviceIdMap.get(key);
    // String sql = String.format(deleteFormat, key, deltaObj.endTime);
    // statement.addBatch(sql);
    // count++;
    // if (count > 100) {
    // statement.executeBatch();
    // statement.clearBatch();
    // count = 0;
    // }
    // }
    // }
    // statement.executeBatch();
    // statement.clearBatch();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB post bck sender can not parse tsfile into delete SQL because{}",
    // e.getMessage());
    // } catch (SQLException | ClassNotFoundException e) {
    // LOGGER.error("IoTDB post back sender: jdbc cannot connect to IoTDB because {}",
    // e.getMessage());
    // } finally {
    // try {
    // input.close();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB post back sender : Cannot close file stream because {}", e.getMessage());
    // }
    // try {
    // if (statement != null)
    // statement.close();
    // if (connection != null)
    // connection.close();
    // } catch (SQLException e) {
    // LOGGER.error("IoTDB post back sender : Can not close JDBC connection because {}",
    // e.getMessage());
    // }
    // }
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
      Socket socket = new Socket("localhost", config.clientPort);
      socket.close();
      LOGGER.error("The postback client has been started!");
      System.exit(0);
    } catch (IOException e) {
      try {
        ServerSocket listenerSocket = new ServerSocket(config.clientPort);
        Thread listener = new Thread(new Runnable() {
          public void run() {
            while (true) {
              try {
                listenerSocket.accept();
              } catch (IOException e) {
                LOGGER.error("IoTDB post back sender: unable to  listen to port{}, because {}",
                    config.clientPort, e.getMessage());
              }
            }
          }
        });
        listener.start();
      } catch (IOException e1) {
        LOGGER.error("IoTDB post back sender: unable to listen to port{}, because {}",
            config.clientPort, e1.getMessage());
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
          % (config.uploadCycleInSeconds * 1000) == 0) {
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
  public void timedTask() {
    postback();
    lastPostBackTime = new Date();
    Date currentTime;
    while (true) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOGGER.error("IoTDB post back sender : Thread {} cannot sleep.",
            Thread.currentThread().getName());
      }
      currentTime = new Date();
      if (currentTime.getTime() - lastPostBackTime.getTime() > config.uploadCycleInSeconds * 1000) {
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

    for (String snapshotPath : config.snapshotPaths) {
      if (new File(snapshotPath).exists() && new File(snapshotPath).list().length != 0) {
        // it means that the last task of postback does not succeed! Clear the files and
        // start to postback again
        try {
          PostbackUtils.deleteFile(new File(snapshotPath));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    postBackStatus = true;
    connectionOrElse = true;

    // connect to postback server
    getConnection(config.serverIp, config.serverPort);
    if (!connectionOrElse) {
      LOGGER.info("IoTDB post back sender : postback process has failed!");
      postBackStatus = false;
      return;
    }

    FileManager fileManager = FileManager.getInstance();
    fileManager.init();
    Map<String, Set<String>> sendingFileList = fileManager.getSendingFiles();
    Map<String, Set<String>> nowLocalFileList = fileManager.getNowLocalFiles();
    if (PostbackUtils.isEmpty(sendingFileList)) {
      LOGGER.info("IoTDB post back sender : there has no file to postback !");
      postBackStatus = false;
      return;
    }

    // create snapshot
    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
      sendingFileSnapshotList.put(entry.getKey(), makeFileSnapshot(entry.getValue()));
    }

    sendSchema(config.schemaPath);
    if (!connectionOrElse) {
      transport.close();
      LOGGER.info("IoTDB post back sender : postback process has failed!");
      postBackStatus = false;
      return;
    }
    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
      Set<String> sendingList = entry.getValue();
      Set<String> sendingSnapshotList = sendingFileSnapshotList.get(entry.getKey());
      if (sendingSnapshotList.size() == 0) {
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
        LOGGER.info("IoTDB post back sender : postback process has failed!");
        postBackStatus = false;
        return;
      }
      startSending(sendingSnapshotList);
      if (!connectionOrElse) {
        transport.close();
        LOGGER.info("IoTDB post back sender : postback process has failed!");
        postBackStatus = false;
        return;
      }
      if (afterSending()) {
        nowLocalFileList.get(entry.getKey()).addAll(sendingList);
        fileManager.setNowLocalFiles(nowLocalFileList);
        fileManager.backupNowLocalFileInfo(config.lastFileInfo);
        if (clearOrNot) {
          deleteData(sendingSnapshotList);
        }
        LOGGER.info("IoTDB post back sender : the postBack has finished storage group {}.",
            entry.getKey());
      } else {
        LOGGER.info("IoTDB post back sender : postback process has failed!");
        postBackStatus = false;
        return;
      }
    }
    for (String snapshotPath : config.snapshotPaths) {
      try {
        PostbackUtils.deleteFile(new File(snapshotPath));
      } catch (IOException e) {
        e.printStackTrace();
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
      LOGGER.info("IoTDB post back sender : postback process has failed!");
      postBackStatus = false;
      return;
    }
    transport.close();
    LOGGER.info("IoTDB post back sender : postback process has finished!");
    postBackStatus = false;
    return;
  }

  private static class TransferHolder {

    private static final FileSenderImpl INSTANCE = new FileSenderImpl();
  }
}
