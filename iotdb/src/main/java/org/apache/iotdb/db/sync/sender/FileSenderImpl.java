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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.sync.sender;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
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
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.conf.Constans;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.service.sync.thrift.SyncDataStatus;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSenderImpl is used to transfer tsfiles that needs to sync to receiver.
 */
public class FileSenderImpl implements FileSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSenderImpl.class);
  private TTransport transport;
  private SyncService.Client serviceClient;
  private List<String> schema = new ArrayList<>();

  /**
   * Mark the identity of sender
   **/
  private String uuid;

  /**
   * Monitor sync status
   **/
  private Thread syncMonitor;

  /**
   * Files that need to be synchronized
   */
  private Map<String, Set<String>> validAllFiles;

  /**
   * All tsfiles in data directory
   **/
  private Map<String, Set<String>> currentLocalFiles;

  /**
   * Mark the start time of last sync
   **/
  private Date lastSyncTime = new Date();

  /**
   * If true, sync is in execution.
   **/
  private volatile boolean syncStatus = false;

  /**
   * Key means storage group, Set means corresponding tsfiles
   **/
  private Map<String, Set<String>> validFileSnapshot = new HashMap<>();

  private FileManager fileManager = FileManager.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();

  /**
   * Monitor sync status.
   */
  private final Runnable monitorSyncStatus = () -> {
    Date oldTime = new Date();
    while (true) {
      if (Thread.interrupted()) {
        break;
      }
      Date currentTime = new Date();
      if (currentTime.getTime() / 1000 == oldTime.getTime() / 1000) {
        continue;
      }
      if ((currentTime.getTime() - lastSyncTime.getTime())
          % (config.getUploadCycleInSeconds() * 1000) == 0) {
        oldTime = currentTime;
        if (syncStatus) {
          LOGGER.info("Sync process is in execution!");
        }
      }
    }
  };

  private FileSenderImpl() {
  }

  public static final FileSenderImpl getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Create a sender and sync files to the receiver.
   *
   * @param args not used
   */
  public static void main(String[] args)
      throws InterruptedException, IOException, SyncConnectionException {
    Thread.currentThread().setName(ThreadName.SYNC_CLIENT.getName());
    FileSenderImpl fileSenderImpl = new FileSenderImpl();
    fileSenderImpl.verifySingleton();
    fileSenderImpl.startMonitor();
    fileSenderImpl.timedTask();
  }

  /**
   * Start Monitor Thread
   */
  public void startMonitor() {
    syncMonitor = new Thread(monitorSyncStatus, ThreadName.SYNC_MONITOR.getName());
    syncMonitor.setDaemon(true);
    syncMonitor.start();
  }

  /**
   * Start sync task in a certain time.
   */
  public void timedTask() throws InterruptedException, SyncConnectionException, IOException {
    sync();
    lastSyncTime = new Date();
    Date currentTime;
    while (true) {
      if (Thread.interrupted()) {
        break;
      }
      Thread.sleep(2000);
      currentTime = new Date();
      if (currentTime.getTime() - lastSyncTime.getTime()
          > config.getUploadCycleInSeconds() * 1000) {
        lastSyncTime = currentTime;
        sync();
      }
    }
  }

  /**
   * Execute a sync task.
   */
  @Override
  public void sync() throws SyncConnectionException, IOException {

    //1. Clear old snapshots if necessary
    for (String snapshotPath : config.getSnapshotPaths()) {
      if (new File(snapshotPath).exists() && new File(snapshotPath).list().length != 0) {
        /** It means that the last task of sync does not succeed! Clear the files and start to sync again **/
        SyncUtils.deleteFile(new File(snapshotPath));
      }
    }

    // 2. Acquire valid files and check
    fileManager.init();
    validAllFiles = fileManager.getValidAllFiles();
    currentLocalFiles = fileManager.getCurrentLocalFiles();
    if (SyncUtils.isEmpty(validAllFiles)) {
      LOGGER.info("There has no file to sync !");
      return;
    }

    // 3. Connect to sync server and Confirm Identity
    establishConnection(config.getServerIp(), config.getServerPort());
    if (!confirmIdentity(config.getUuidPath())) {
      LOGGER.error("Sorry, you do not have the permission to connect to sync receiver.");
      return;
    }

    // 4. Create snapshot
    for (Entry<String, Set<String>> entry : validAllFiles.entrySet()) {
      validFileSnapshot.put(entry.getKey(), makeFileSnapshot(entry.getValue()));
    }

    syncStatus = true;

    // 5. Sync schema
    syncSchema();

    // 6. Sync data
    syncAllData();

    // 7. clear snapshot
    for (String snapshotPath : config.getSnapshotPaths()) {
      SyncUtils.deleteFile(new File(snapshotPath));
    }

    // 8. notify receiver that synchronization finish
    // At this point the synchronization has finished even if connection fails
    try {
      serviceClient.cleanUp();
    } catch (TException e) {
      LOGGER.error("Unable to connect to receiver.", e);
    }
    transport.close();
    LOGGER.info("Sync process has finished.");
    syncStatus = false;
  }

  @Override
  public void syncAllData() throws SyncConnectionException {
    for (Entry<String, Set<String>> entry : validAllFiles.entrySet()) {
      Set<String> validFiles = entry.getValue();
      Set<String> validSnapshot = validFileSnapshot.get(entry.getKey());
      if (validSnapshot.isEmpty()) {
        continue;
      }
      LOGGER.info("Sync process starts to transfer data of storage group {}", entry.getKey());
      try {
        serviceClient.init(entry.getKey());
      } catch (TException e) {
        throw new SyncConnectionException("Unable to connect to receiver", e);
      }
      syncData(validSnapshot);
      if (afterSynchronization()) {
        currentLocalFiles.get(entry.getKey()).addAll(validFiles);
        fileManager.setCurrentLocalFiles(currentLocalFiles);
        fileManager.backupNowLocalFileInfo(config.getLastFileInfo());
        LOGGER.info("Sync process has finished storage group {}.", entry.getKey());
      } else {
        throw new SyncConnectionException(
            "Receiver cannot sync data, abandon this synchronization");
      }
    }
  }

  /**
   * Establish a connection between the sender and the receiver.
   *
   * @param serverIp the ip address of the receiver
   * @param serverPort must be same with port receiver set.
   */
  @Override
  public void establishConnection(String serverIp, int serverPort) throws SyncConnectionException {
    transport = new TSocket(serverIp, serverPort);
    TProtocol protocol = new TBinaryProtocol(transport);
    serviceClient = new SyncService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      syncStatus = false;
      LOGGER.error("Cannot connect to server");
      throw new SyncConnectionException(e);
    }
  }

  /**
   * UUID marks the identity of sender for receiver.
   */
  @Override
  public boolean confirmIdentity(String uuidPath) throws SyncConnectionException, IOException {
    File file = new File(uuidPath);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (!file.exists()) {
      try (FileOutputStream out = new FileOutputStream(file)) {
        file.createNewFile();
        uuid = generateUUID();
        out.write(uuid.getBytes());
      } catch (IOException e) {
        LOGGER.error("Cannot write UUID to file {}", file.getPath());
        throw new IOException(e);
      }
    } else {
      try (BufferedReader bf = new BufferedReader((new FileReader(uuidPath)))) {
        uuid = bf.readLine();
      } catch (IOException e) {
        LOGGER.error("Cannot read UUID from file{}", file.getPath());
        throw new IOException(e);
      }
    }
    boolean legalConnection;
    try {
      legalConnection = serviceClient.checkIdentity(uuid,
          InetAddress.getLocalHost().getHostAddress());
    } catch (Exception e) {
      LOGGER.error("Cannot confirm identity with receiver");
      throw new SyncConnectionException(e);
    }
    return legalConnection;
  }

  private String generateUUID() {
    return Constans.SYNC_CLIENT + UUID.randomUUID().toString().replaceAll("-", "");
  }

  /**
   * Create snapshots for valid files.
   */
  @Override
  public Set<String> makeFileSnapshot(Set<String> validFiles) throws IOException {
    Set<String> validFilesSnapshot = new HashSet<>();
    try {
      for (String filePath : validFiles) {
        String snapshotFilePath = SyncUtils.getSnapshotFilePath(filePath);
        validFilesSnapshot.add(snapshotFilePath);
        File newFile = new File(snapshotFilePath);
        if (!newFile.getParentFile().exists()) {
          newFile.getParentFile().mkdirs();
        }
        Path link = FileSystems.getDefault().getPath(snapshotFilePath);
        Path target = FileSystems.getDefault().getPath(filePath);
        Files.createLink(link, target);
      }
    } catch (IOException e) {
      LOGGER.error("Can not make fileSnapshot");
      throw new IOException(e);
    }
    return validFilesSnapshot;
  }

  /**
   * Transfer data of a storage group to receiver.
   *
   * @param fileSnapshotList list of sending snapshot files in a storage group.
   */
  public void syncData(Set<String> fileSnapshotList) throws SyncConnectionException {
    try {
      int successNum = 0;
      for (String snapshotFilePath : fileSnapshotList) {
        successNum++;
        File file = new File(snapshotFilePath);
        List<String> filePathSplit = new ArrayList<>();
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
          String[] name = snapshotFilePath.split(File.separator + File.separator);
          filePathSplit.add(name[name.length - 2]);
          filePathSplit.add(name[name.length - 1]);
        } else {
          String[] name = snapshotFilePath.split(File.separator);
          filePathSplit.add(name[name.length - 2]);
          filePathSplit.add(name[name.length - 1]);
        }
        while (true) {
          // Sync all data to receiver
          byte[] buffer = new byte[Constans.DATA_CHUNK_SIZE];
          int data;
          try (FileInputStream fis = new FileInputStream(file)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(Constans.DATA_CHUNK_SIZE);
            while ((data = fis.read(buffer)) != -1) { // cut the file into pieces to send
              bos.write(buffer, 0, data);
              ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
              bos.reset();
              serviceClient
                  .syncData(null, filePathSplit, buffToSend, SyncDataStatus.PROCESSING_STATUS);
            }
            bos.close();
          }

          // Get md5 of the file.
          MessageDigest md = MessageDigest.getInstance("MD5");
          try (FileInputStream fis = new FileInputStream(file)) {
            while ((data = fis.read(buffer)) != -1) {
              md.update(buffer, 0, data);
            }
          }

          // the file is sent successfully
          String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
          String md5OfReceiver = serviceClient.syncData(md5OfSender, filePathSplit,
              null, SyncDataStatus.SUCCESS_STATUS);
          if (md5OfSender.equals(md5OfReceiver)) {
            LOGGER.info("Receiver has received {} successfully.", snapshotFilePath);
            break;
          }
        }
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(String.format("Task of synchronization has completed %d/%d.", successNum,
              fileSnapshotList.size()));
        }
      }
    } catch (Exception e) {
      throw new SyncConnectionException("Cannot sync data with receiver.", e);
    }
  }

  /**
   * Sync schema with receiver.
   */
  @Override
  public void syncSchema() throws SyncConnectionException {
    try (FileInputStream fis = new FileInputStream(new File(config.getSchemaPath()))) {
      int mBufferSize = 4 * 1024 * 1024;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(mBufferSize);
      byte[] buffer = new byte[mBufferSize];
      int n;
      while ((n = fis.read(buffer)) != -1) { // cut the file into pieces to send
        bos.write(buffer, 0, n);
        ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
        bos.reset();
        // 1 represents there is still schema buffer to send.
        serviceClient.syncSchema(buffToSend, SyncDataStatus.PROCESSING_STATUS);
      }
      bos.close();
      // 0 represents the schema file has been transferred completely.
      serviceClient.syncSchema(null, SyncDataStatus.SUCCESS_STATUS);
    } catch (Exception e) {
      LOGGER.error("Cannot sync schema ", e);
      throw new SyncConnectionException(e);
    }
  }

  @Override
  public boolean afterSynchronization() throws SyncConnectionException {
    boolean successOrNot;
    try {
      successOrNot = serviceClient.load();
    } catch (TException e) {
      throw new SyncConnectionException(
          "Can not finish sync process because sync receiver has broken down.", e);
    }
    return successOrNot;
  }

  /**
   * The method is to verify whether the client lock file is locked or not, ensuring that only one
   * client is running.
   */
  private void verifySingleton() throws IOException {
    File lockFile = new File(config.getLockFilePath());
    if (!lockFile.getParentFile().exists()) {
      lockFile.getParentFile().mkdirs();
    }
    if (!lockFile.exists()) {
      lockFile.createNewFile();
    }
    if (!lockInstance(config.getLockFilePath())) {
      LOGGER.error("Sync client is running.");
      System.exit(1);
    }
  }

  /**
   * Try to lock lockfile. if failed, it means that sync client has benn started.
   *
   * @param lockFile path of lockfile
   */
  private static boolean lockInstance(final String lockFile) {
    try {
      final File file = new File(lockFile);
      final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
      final FileLock fileLock = randomAccessFile.getChannel().tryLock();
      if (fileLock != null) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            fileLock.release();
            randomAccessFile.close();
            file.delete();
          } catch (Exception e) {
            LOGGER.error("Unable to remove lock file: {}", lockFile, e);
          }
        }));
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Unable to create and/or lock file: {}", lockFile, e);
    }
    return false;
  }

  private static class InstanceHolder {

    private static final FileSenderImpl INSTANCE = new FileSenderImpl();
  }

  public void setConfig(SyncSenderConfig config) {
    this.config = config;
  }

  public List<String> getSchema() {
    return schema;
  }
}
