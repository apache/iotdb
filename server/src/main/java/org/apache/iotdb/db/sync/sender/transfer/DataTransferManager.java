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
package org.apache.iotdb.db.sync.sender.transfer;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.sender.SyncFileManager;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;
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
 * SyncSenderImpl is used to transfer tsfiles that needs to sync to receiver.
 */
public class DataTransferManager implements IDataTransferManager {

  private static final Logger logger = LoggerFactory.getLogger(DataTransferManager.class);

  private TTransport transport;

  private SyncService.Client serviceClient;

  private List<String> schema = new ArrayList<>();

  private static SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();

  /**
   * Files that need to be synchronized
   */
  private Map<String, Set<String>> validAllFiles;

  /**
   * All tsfiles in data directory
   **/
  private Map<String, Set<String>> currentLocalFiles;

  /**
   * If true, sync is in execution.
   **/
  private volatile boolean syncStatus = false;

  /**
   * Key means storage group, Set means corresponding tsfiles
   **/
  private Map<String, Set<String>> validFileSnapshot = new HashMap<>();

  private SyncFileManager syncFileManager = SyncFileManager.getInstance();

  private ScheduledExecutorService executorService;

  private DataTransferManager() {
    init();
  }

  public static final DataTransferManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Create a sender and sync files to the receiver.
   *
   * @param args not used
   */
  public static void main(String[] args) throws IOException {
    Thread.currentThread().setName(ThreadName.SYNC_CLIENT.getName());
    DataTransferManager fileSenderImpl = new DataTransferManager();
    fileSenderImpl.verifySingleton();
    fileSenderImpl.startMonitor();
    fileSenderImpl.startTimedTask();
  }

  @Override
  public void init() {
    if (executorService == null) {
      executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(2,
          "sync-client-timer");
    }
  }

  /**
   * Start Monitor Thread, monitor sync status
   */
  private void startMonitor() {
    executorService.scheduleWithFixedDelay(() -> {
      if (syncStatus) {
        logger.info("Sync process is in execution!");
      }
    }, Constans.SYNC_MONITOR_DELAY, Constans.SYNC_MONITOR_PERIOD, TimeUnit.SECONDS);
  }

  /**
   * Start sync task in a certain time.
   */
  private void startTimedTask() {
    executorService.scheduleWithFixedDelay(() -> {
      try {
        sync();
      } catch (SyncConnectionException | IOException e) {
        logger.error("Sync failed", e);
        stop();
      }
    }, Constans.SYNC_PROCESS_DELAY, Constans.SYNC_PROCESS_PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    executorService.shutdownNow();
    executorService = null;
  }

  /**
   * Execute a sync task.
   */
  @Override
  public void sync() throws SyncConnectionException, IOException {

    //1. Clear old snapshots if necessary
    for (String snapshotPath : config.getSnapshotPaths()) {
      if (new File(snapshotPath).exists() && new File(snapshotPath).list().length != 0) {
        // It means that the last task of sync does not succeed! Clear the files and start to sync again
        FileUtils.deleteDirectory(new File(snapshotPath));
      }
    }

    // 2. Acquire valid files and check
    syncFileManager.init();
    validAllFiles = syncFileManager.getValidAllFiles();
    currentLocalFiles = syncFileManager.getCurrentLocalFiles();
    if (SyncUtils.isEmpty(validAllFiles)) {
      logger.info("There has no file to sync !");
      return;
    }

    // 3. Connect to sync server and Confirm Identity
    establishConnection(config.getServerIp(), config.getServerPort());
    if (!confirmIdentity(config.getUuidPath())) {
      logger.error("Sorry, you do not have the permission to connect to sync receiver.");
      System.exit(1);
    }

    // 4. Create snapshot
    for (Entry<String, Set<String>> entry : validAllFiles.entrySet()) {
      validFileSnapshot.put(entry.getKey(), makeFileSnapshot(entry.getValue()));
    }

    syncStatus = true;

    try {
      // 5. Sync schema
      syncSchema();

      // 6. Sync data
      syncAllData();
    } catch (SyncConnectionException e) {
      logger.error("cannot finish sync process", e);
      syncStatus = false;
      return;
    }

    // 7. clear snapshot
    for (String snapshotPath : config.getSnapshotPaths()) {
      FileUtils.deleteDirectory(new File(snapshotPath));
    }

    // 8. notify receiver that synchronization finish
    // At this point the synchronization has finished even if connection fails
    try {
      serviceClient.cleanUp();
    } catch (TException e) {
      logger.error("Unable to connect to receiver.", e);
    }
    transport.close();
    logger.info("Sync process has finished.");
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
      logger.info("Sync process starts to transfer data of storage group {}", entry.getKey());
      try {
        if (!serviceClient.init(entry.getKey())) {
          throw new SyncConnectionException("unable init receiver");
        }
      } catch (TException e) {
        throw new SyncConnectionException("Unable to connect to receiver", e);
      }
      syncData(validSnapshot);
      if (afterSynchronization()) {
        currentLocalFiles.get(entry.getKey()).addAll(validFiles);
        syncFileManager.setCurrentLocalFiles(currentLocalFiles);
        syncFileManager.backupNowLocalFileInfo(config.getLastFileInfo());
        logger.info("Sync process has finished storage group {}.", entry.getKey());
      } else {
        logger.error("Receiver cannot sync data, abandon this synchronization of storage group {}",
            entry.getKey());
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
      logger.error("Cannot connect to server");
      throw new SyncConnectionException(e);
    }
  }

  /**
   * UUID marks the identity of sender for receiver.
   */
  @Override
  public boolean confirmIdentity(String uuidPath) throws SyncConnectionException, IOException {
    File file = new File(uuidPath);
    /** Mark the identity of sender **/
    String uuid;
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (!file.exists()) {
      try (FileOutputStream out = new FileOutputStream(file)) {
        file.createNewFile();
        uuid = generateUUID();
        out.write(uuid.getBytes());
      } catch (IOException e) {
        logger.error("Cannot insert UUID to file {}", file.getPath());
        throw new IOException(e);
      }
    } else {
      try (BufferedReader bf = new BufferedReader((new FileReader(uuidPath)))) {
        uuid = bf.readLine();
      } catch (IOException e) {
        logger.error("Cannot read UUID from file{}", file.getPath());
        throw new IOException(e);
      }
    }
    boolean legalConnection;
    try {
      legalConnection = serviceClient.checkIdentity(uuid,
          InetAddress.getLocalHost().getHostAddress());
    } catch (Exception e) {
      logger.error("Cannot confirm identity with receiver");
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
      logger.error("Can not make fileSnapshot");
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
        int retryCount = 0;
        // Get md5 of the file.
        MessageDigest md = MessageDigest.getInstance("MD5");
        outer:
        while (true) {
          retryCount++;
          // Sync all data to receiver
          if (retryCount > Constans.MAX_SYNC_FILE_TRY) {
            throw new SyncConnectionException(String
                .format("can not sync file %s after %s tries.", snapshotFilePath,
                    Constans.MAX_SYNC_FILE_TRY));
          }
          md.reset();
          byte[] buffer = new byte[Constans.DATA_CHUNK_SIZE];
          int dataLength;
          try (FileInputStream fis = new FileInputStream(file);
              ByteArrayOutputStream bos = new ByteArrayOutputStream(Constans.DATA_CHUNK_SIZE)) {
            while ((dataLength = fis.read(buffer)) != -1) { // cut the file into pieces to send
              bos.write(buffer, 0, dataLength);
              md.update(buffer, 0, dataLength);
              ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
              bos.reset();
              if (!Boolean.parseBoolean(serviceClient
                  .syncData(null, filePathSplit, buffToSend, SyncDataStatus.PROCESSING_STATUS))) {
                logger.info("Receiver failed to receive data from {}, retry.", snapshotFilePath);
                continue outer;
              }
            }
          }

          // the file is sent successfully
          String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
          String md5OfReceiver = serviceClient.syncData(md5OfSender, filePathSplit,
              null, SyncDataStatus.FINISH_STATUS);
          if (md5OfSender.equals(md5OfReceiver)) {
            logger.info("Receiver has received {} successfully.", snapshotFilePath);
            break;
          }
        }
        logger.info(String.format("Task of synchronization has completed %d/%d.", successNum,
            fileSnapshotList.size()));
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
    int retryCount = 0;
    outer:
    while (true) {
      retryCount++;
      if (retryCount > Constans.MAX_SYNC_FILE_TRY) {
        throw new SyncConnectionException(String
            .format("can not sync schema after %s tries.", Constans.MAX_SYNC_FILE_TRY));
      }
      byte[] buffer = new byte[Constans.DATA_CHUNK_SIZE];
      try (FileInputStream fis = new FileInputStream(new File(config.getSchemaPath()));
          ByteArrayOutputStream bos = new ByteArrayOutputStream(Constans.DATA_CHUNK_SIZE)) {
        // Get md5 of the file.
        MessageDigest md = MessageDigest.getInstance("MD5");
        int dataLength;
        while ((dataLength = fis.read(buffer)) != -1) { // cut the file into pieces to send
          bos.write(buffer, 0, dataLength);
          md.update(buffer, 0, dataLength);
          ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
          bos.reset();
          // PROCESSING_STATUS represents there is still schema buffer to send.
          if (!Boolean.parseBoolean(
              serviceClient.syncSchema(null, buffToSend, SyncDataStatus.PROCESSING_STATUS))) {
            logger.error("Receiver failed to receive metadata, retry.");
            continue outer;
          }
        }
        bos.close();
        String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
        String md5OfReceiver = serviceClient
            .syncSchema(md5OfSender, null, SyncDataStatus.FINISH_STATUS);
        if (md5OfSender.equals(md5OfReceiver)) {
          logger.info("Receiver has received schema successfully.");
          /** receiver start to load metadata **/
          if (Boolean
              .parseBoolean(serviceClient.syncSchema(null, null, SyncDataStatus.SUCCESS_STATUS))) {
            throw new SyncConnectionException("Receiver failed to load metadata");
          }
          break;
        }
      } catch (Exception e) {
        logger.error("Cannot sync schema ", e);
        throw new SyncConnectionException(e);
      }
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
      logger.error("Sync client is running.");
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
            FileUtils.forceDelete(file);
          } catch (Exception e) {
            logger.error("Unable to remove lock file: {}", lockFile, e);
          }
        }));
        return true;
      }
    } catch (Exception e) {
      logger.error("Unable to create and/or lock file: {}", lockFile, e);
    }
    return false;
  }

  private static class InstanceHolder {

    private static final DataTransferManager INSTANCE = new DataTransferManager();
  }

  public void setConfig(SyncSenderConfig config) {
    this.config = config;
  }

  public List<String> getSchema() {
    return schema;
  }
}
