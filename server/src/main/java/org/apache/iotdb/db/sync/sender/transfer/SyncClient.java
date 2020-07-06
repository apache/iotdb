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
 */
package org.apache.iotdb.db.sync.sender.transfer;

import static org.apache.iotdb.db.sync.conf.SyncConstant.CONFLICT_CODE;
import static org.apache.iotdb.db.sync.conf.SyncConstant.SUCCESS_CODE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
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
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.exception.SyncDeviceOwnerConflictException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.manage.ISyncFileManager;
import org.apache.iotdb.db.sync.sender.manage.SyncFileManager;
import org.apache.iotdb.db.sync.sender.recover.ISyncSenderLogger;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogAnalyzer;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogger;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.service.sync.thrift.ConfirmInfo;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.service.sync.thrift.SyncStatus;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncClient implements ISyncClient {

  private static final Logger logger = LoggerFactory.getLogger(SyncClient.class);

  private static SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();

  private static final int BATCH_LINE = 1000;

  private static final int TIMEOUT_MS = 1000;

  /**
   * When transferring schema information, it is a better choice to transfer only new schema
   * information, avoiding duplicate data transmission. The schema log is self-increasing, so the
   * location is recorded once after each synchronization task for the next synchronization task to
   * use.
   */
  private int schemaFileLinePos;

  private TTransport transport;

  private SyncService.Client serviceClient;

  private Map<String, Set<Long>> allSG;

  private Map<String, Map<Long, Set<File>>> toBeSyncedFilesMap;

  private Map<String, Map<Long, Set<File>>> deletedFilesMap;

  private Map<String, Map<Long, Set<File>>> lastLocalFilesMap;

  /**
   * If true, sync is in execution.
   **/
  private volatile boolean syncStatus = false;

  /**
   * Record sync progress in log.
   */
  private ISyncSenderLogger syncLog;

  private ISyncFileManager syncFileManager = SyncFileManager.getInstance();

  private ScheduledExecutorService executorService;

  private SyncClient() {
    init();
  }

  public static SyncClient getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Create a sender and sync files to the receiver periodically.
   */
  public static void main(String[] args) throws IOException {
    Thread.currentThread().setName(ThreadName.SYNC_CLIENT.getName());
    ISyncClient fileSenderImpl = new SyncClient();
    fileSenderImpl.verifySingleton();
    fileSenderImpl.startMonitor();
    fileSenderImpl.startTimedTask();
  }

  @Override
  public void verifySingleton() throws IOException {
    File lockFile = getLockFile();
    if (!lockFile.getParentFile().exists()) {
      lockFile.getParentFile().mkdirs();
    }
    if (!lockFile.exists()) {
      lockFile.createNewFile();
    }
    if (!lockInstance(lockFile)) {
      logger.error("Sync client is already running.");
      System.exit(1);
    }
  }

  /**
   * Try to lock lockfile. if failed, it means that sync client has been started.
   *
   * @param lockFile lock file
   */
  private boolean lockInstance(File lockFile) {
    try {
      final RandomAccessFile randomAccessFile = new RandomAccessFile(lockFile, "rw");
      final FileLock fileLock = randomAccessFile.getChannel().tryLock();
      if (fileLock != null) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            fileLock.release();
            randomAccessFile.close();
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


  @Override
  public void init() {
    if (executorService == null) {
      executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(2,
          "sync-client-timer");
    }
  }

  @Override
  public void startMonitor() {
    executorService.scheduleWithFixedDelay(() -> {
      if (syncStatus) {
        logger.info("Sync process for receiver {} is in execution!", config.getSyncReceiverName());
      }
    }, SyncConstant.SYNC_MONITOR_DELAY, SyncConstant.SYNC_MONITOR_PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void startTimedTask() {
    executorService.scheduleWithFixedDelay(() -> {
      try {
        syncAll();
      } catch (Exception e) {
        logger.error("Sync failed", e);
      }
    }, SyncConstant.SYNC_PROCESS_DELAY, SyncConstant.SYNC_PROCESS_PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    executorService.shutdownNow();
    executorService = null;
  }

  @Override
  public void syncAll() throws SyncConnectionException, IOException, TException {

    // 1. Connect to sync receiver and confirm identity
    establishConnection(config.getServerIp(), config.getServerPort());
    confirmIdentity();
    serviceClient.startSync();

    // 2. Sync Schema
    syncSchema();

    // 3. Sync all data
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    logger.info("There are {} data dirs to be synced.", dataDirs.length);
    for (int i = 0 ; i < dataDirs.length; i++) {
      String dataDir = dataDirs[i];
      logger.info("Start to sync data in data dir {}, the process is {}/{}", dataDir, i + 1,
          dataDirs.length);

      config.update(dataDir);
      syncFileManager.getValidFiles(dataDir);
      allSG = syncFileManager.getAllSGs();
      lastLocalFilesMap = syncFileManager.getLastLocalFilesMap();
      deletedFilesMap = syncFileManager.getDeletedFilesMap();
      toBeSyncedFilesMap = syncFileManager.getToBeSyncedFilesMap();
      checkRecovery();
      if (SyncUtils.isEmpty(deletedFilesMap) && SyncUtils.isEmpty(toBeSyncedFilesMap)) {
        logger.info("There has no data to sync in data dir {}", dataDir);
        continue;
      }
      sync();
      endSync();
      logger.info("Finish to sync data in data dir {}, the process is {}/{}", dataDir, i + 1,
          dataDirs.length);
    }

    // 4. notify receiver that synchronization finish
    // At this point the synchronization has finished even if connection fails
    try {
      serviceClient.endSync();
      transport.close();
      logger.info("Sync process has finished.");
    } catch (TException e) {
      logger.error("Unable to connect to receiver.", e);
    }
  }

  private void checkRecovery() throws IOException {
    new SyncSenderLogAnalyzer(config.getSenderFolderPath()).recover();
  }

  @Override
  public void establishConnection(String serverIp, int serverPort) throws SyncConnectionException {
    transport = new TFastFramedTransport(new TSocket(serverIp, serverPort, TIMEOUT_MS));
    TProtocol protocol = new TBinaryProtocol(transport);
    serviceClient = new SyncService.Client(protocol);
    try {
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      logger.error("Cannot connect to the receiver.");
      throw new SyncConnectionException(e);
    }
  }

  @Override
  public void confirmIdentity() throws SyncConnectionException {
    try (Socket socket = new Socket(config.getServerIp(), config.getServerPort())) {
      ConfirmInfo info = new ConfirmInfo(socket.getLocalAddress().getHostAddress(),
          getOrCreateUUID(getUuidFile()),
          IoTDBDescriptor.getInstance().getConfig().getPartitionInterval(), IoTDBConstant.VERSION);
      SyncStatus status = serviceClient
          .check(info);
      if (status.code != SUCCESS_CODE) {
        throw new SyncConnectionException(
            "The receiver rejected the synchronization task because " + status.msg);
      }
    } catch (Exception e) {
      logger.error("Cannot confirm identity with the receiver.");
      throw new SyncConnectionException(e);
    }
  }

  /**
   * UUID marks the identity of sender for receiver.
   */
  private String getOrCreateUUID(File uuidFile) throws IOException {
    String uuid;
    if (!uuidFile.getParentFile().exists()) {
      uuidFile.getParentFile().mkdirs();
    }
    if (!uuidFile.exists()) {
      try (FileOutputStream out = new FileOutputStream(uuidFile)) {
        uuid = generateUUID();
        out.write(uuid.getBytes());
      } catch (IOException e) {
        logger.error("Cannot insert UUID to file {}", uuidFile.getPath());
        throw new IOException(e);
      }
    } else {
      try (BufferedReader bf = new BufferedReader((new FileReader(uuidFile.getAbsolutePath())))) {
        uuid = bf.readLine();
      } catch (IOException e) {
        logger.error("Cannot read UUID from file{}", uuidFile.getPath());
        throw new IOException(e);
      }
    }
    return uuid;
  }

  private String generateUUID() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  @Override
  public void syncSchema() throws SyncConnectionException, TException {
    if (!getSchemaLogFile().exists()) {
      logger.info("Schema file {} doesn't exist.", getSchemaLogFile().getName());
      return;
    }
    int retryCount = 0;
    serviceClient.initSyncData(MetadataConstant.METADATA_LOG);
    while (true) {
      if (retryCount > config.getMaxNumOfSyncFileRetry()) {
        throw new SyncConnectionException(String
            .format("Can not sync schema after %s retries.", config.getMaxNumOfSyncFileRetry()));
      }
      if (tryToSyncSchema()) {
        writeSyncSchemaPos(getSchemaPosFile());
        break;
      }
      retryCount++;
    }
  }

  private boolean tryToSyncSchema() {
    int schemaPos = readSyncSchemaPos(getSchemaPosFile());

    // start to sync file data and get md5 of this file.
    try (BufferedReader br = new BufferedReader(new FileReader(getSchemaLogFile()));
        ByteArrayOutputStream bos = new ByteArrayOutputStream(SyncConstant.DATA_CHUNK_SIZE)) {
      schemaFileLinePos = 0;
      while (schemaFileLinePos < schemaPos) {
        br.readLine();
        schemaFileLinePos++;
      }
      MessageDigest md = MessageDigest.getInstance(SyncConstant.MESSAGE_DIGIT_NAME);
      String line;
      int cntLine = 0;
      while ((line = br.readLine()) != null) {
        schemaFileLinePos++;
        byte[] singleLineData = BytesUtils.stringToBytes(line);
        bos.write(singleLineData);
        bos.write("\r\n".getBytes());
        if (cntLine++ == BATCH_LINE) {
          md.update(bos.toByteArray());
          ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
          bos.reset();
          SyncStatus status = serviceClient.syncData(buffToSend);
          if (status.code != SUCCESS_CODE) {
            logger.error("Receiver failed to receive metadata because {}, retry.", status.msg);
            return false;
          }
          cntLine = 0;
        }
      }
      if (bos.size() != 0) {
        md.update(bos.toByteArray());
        ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
        bos.reset();
        SyncStatus status = serviceClient.syncData(buffToSend);
        if (status.code != SUCCESS_CODE) {
          logger.error("Receiver failed to receive metadata because {}, retry.", status.msg);
          return false;
        }
      }

      // check md5
      return checkMD5ForSchema(new BigInteger(1, md.digest()).toString(16));
    } catch (NoSuchAlgorithmException | IOException | TException e) {
      logger.error("Can not finish transfer schema to receiver", e);
      return false;
    }
  }

  /**
   * Check MD5 of schema to make sure that the receiver receives the schema correctly
   */
  private boolean checkMD5ForSchema(String md5OfSender) throws TException {
    SyncStatus status = serviceClient.checkDataMD5(md5OfSender);
    if (status.code == SUCCESS_CODE && md5OfSender.equals(status.msg)) {
      logger.info("Receiver has received schema successfully.");
      return true;
    } else {
      logger
          .error("MD5 check of schema file {} failed, retry", getSchemaLogFile().getAbsoluteFile());
      return false;
    }
  }

  private int readSyncSchemaPos(File syncSchemaLogFile) {
    try {
      if (syncSchemaLogFile.exists()) {
        try (BufferedReader br = new BufferedReader(new FileReader(syncSchemaLogFile))) {
          String pos = br.readLine();
          if(pos != null) {
            return Integer.parseInt(pos);
          }
        }
      }
    } catch (IOException e) {
      logger.error("Can not find file {}", syncSchemaLogFile.getAbsoluteFile(), e);
    } catch (NumberFormatException e){
      logger.error("Sync schema pos is not valid", e);
    }
    return 0;
  }

  private void writeSyncSchemaPos(File syncSchemaLogFile) {
    try {
      if (!syncSchemaLogFile.exists()) {
        syncSchemaLogFile.createNewFile();
      }
      try (BufferedWriter br = new BufferedWriter(new FileWriter(syncSchemaLogFile))) {
        br.write(Integer.toString(schemaFileLinePos));
      }
    } catch (IOException e) {
      logger.error("Can not find file {}", syncSchemaLogFile.getAbsoluteFile(), e);
    }
  }

  @Override
  public void sync() throws IOException {
    try {
      syncStatus = true;

      List<String> storageGroups = config.getStorageGroupList();
      for (Entry<String, Set<Long>> entry : allSG.entrySet()) {
        String sgName = entry.getKey();
        if (!storageGroups.isEmpty() && !storageGroups.contains(sgName)) {
          continue;
        }
        lastLocalFilesMap.putIfAbsent(sgName, new HashMap<>());
        syncLog = new SyncSenderLogger(getSyncLogFile());
        try {
          SyncStatus status = serviceClient.init(sgName);
          if (status.code != SUCCESS_CODE) {
            throw new SyncConnectionException("Unable init receiver because " + status.msg);
          }
        } catch (TException | SyncConnectionException e) {
          throw new SyncConnectionException("Unable to connect to receiver", e);
        }
        logger.info(
            "Sync process starts to transfer data of storage group {}, it has {} time ranges.",
            sgName, entry.getValue().size());
        try {
          for (Long timeRangeId : entry.getValue()) {
            lastLocalFilesMap.get(sgName).putIfAbsent(timeRangeId, new HashSet<>());
            syncDeletedFilesNameInOneGroup(sgName, timeRangeId,
                deletedFilesMap.getOrDefault(sgName, Collections.emptyMap())
                    .getOrDefault(timeRangeId, Collections.emptySet()));
            syncDataFilesInOneGroup(sgName, timeRangeId,
                toBeSyncedFilesMap.getOrDefault(sgName, Collections.emptyMap())
                    .getOrDefault(timeRangeId, Collections.emptySet()));
          }
        } catch (SyncDeviceOwnerConflictException e) {
          deletedFilesMap.remove(sgName);
          toBeSyncedFilesMap.remove(sgName);
          storageGroups.remove(sgName);
          config.setStorageGroupList(storageGroups);
          logger.error("Skip the data files of the storage group {}", sgName, e);
        }
        logger.info(
            "Sync process finished the task to sync data of storage group {}.", sgName);
      }

    } catch (SyncConnectionException e) {
      logger.error("cannot finish sync process", e);
    } finally {
      if (syncLog != null) {
        syncLog.close();
      }
      syncStatus = false;
    }
  }

  @Override
  public void syncDeletedFilesNameInOneGroup(String sgName, Long timeRangeId, Set<File> deletedFilesName)
      throws IOException {
    if (deletedFilesName.isEmpty()) {
      logger.info("There has no deleted files to be synced in storage group {}", sgName);
      return;
    }
    syncLog.startSyncDeletedFilesName();
    logger.info("Start to sync names of deleted files in storage group {}", sgName);
    for (File file : deletedFilesName) {
      try {
        if (serviceClient.syncDeletedFileName(file.getName()).code == SUCCESS_CODE) {
          logger.info("Receiver has received deleted file name {} successfully.", file.getName());
          lastLocalFilesMap.get(sgName).get(timeRangeId).remove(file);
          syncLog.finishSyncDeletedFileName(file);
        }
      } catch (TException e) {
        logger.error("Can not sync deleted file name {}, skip it.", file);
      }
    }
    logger.info("Finish to sync names of deleted files in storage group {}", sgName);
  }

  @Override
  public void syncDataFilesInOneGroup(String sgName, Long timeRangeId, Set<File> toBeSyncFiles)
      throws SyncConnectionException, IOException, SyncDeviceOwnerConflictException {
    if (toBeSyncFiles.isEmpty()) {
      logger.info("There has no new tsfiles to be synced in storage group {}", sgName);
      return;
    }
    syncLog.startSyncTsFiles();
    logger.info("Sync process starts to transfer data of storage group {}", sgName);
    int cnt = 0;
    for (File tsfile : toBeSyncFiles) {
      cnt++;
      try {
        File snapshotFile = makeFileSnapshot(tsfile);
        // firstly sync .resource file, then sync tsfile
        syncSingleFile(new File(snapshotFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
        syncSingleFile(snapshotFile);
        lastLocalFilesMap.get(sgName).get(timeRangeId).add(tsfile);
        syncLog.finishSyncTsfile(tsfile);
        logger.info("Task of synchronization has completed {}/{}.", cnt, toBeSyncFiles.size());
      } catch (IOException e) {
        logger.info(
            "Tsfile {} can not make snapshot, so skip the tsfile and continue to sync other tsfiles",
            tsfile, e);
      }
    }
    logger.info("Sync process has finished storage group {}.", sgName);
  }

  /**
   * Make snapshot<hard link> for new tsfile and its .restore file.
   *
   * @param file new tsfile to be synced
   */
  File makeFileSnapshot(File file) throws IOException {
    File snapshotFile = SyncUtils.getSnapshotFile(file);
    if (!snapshotFile.getParentFile().exists()) {
      snapshotFile.getParentFile().mkdirs();
    }
    Path link = FileSystems.getDefault().getPath(snapshotFile.getAbsolutePath());
    Path target = FileSystems.getDefault().getPath(file.getAbsolutePath());
    Files.createLink(link, target);
    link = FileSystems.getDefault()
        .getPath(snapshotFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    target = FileSystems.getDefault()
        .getPath(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    Files.createLink(link, target);
    return snapshotFile;
  }

  /**
   * Transfer data of a tsfile to the receiver.
   */
  private void syncSingleFile(File snapshotFile)
      throws SyncConnectionException, SyncDeviceOwnerConflictException {
    try {
      int retryCount = 0;
      MessageDigest md = MessageDigest.getInstance(SyncConstant.MESSAGE_DIGIT_NAME);
      serviceClient.initSyncData(snapshotFile.getName());
      outer:
      while (true) {
        retryCount++;
        if (retryCount > config.getMaxNumOfSyncFileRetry()) {
          throw new SyncConnectionException(String
              .format("Can not sync file %s after %s tries.", snapshotFile.getAbsoluteFile(),
                  config.getMaxNumOfSyncFileRetry()));
        }
        md.reset();
        byte[] buffer = new byte[SyncConstant.DATA_CHUNK_SIZE];
        int dataLength;
        try (FileInputStream fis = new FileInputStream(snapshotFile);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(SyncConstant.DATA_CHUNK_SIZE)) {
          while ((dataLength = fis.read(buffer)) != -1) { // cut the file into pieces to send
            bos.write(buffer, 0, dataLength);
            md.update(buffer, 0, dataLength);
            ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
            bos.reset();
            SyncStatus status = serviceClient.syncData(buffToSend);
            if(status.code == CONFLICT_CODE){
              throw new SyncDeviceOwnerConflictException(status.msg);
            }
            if (status.code != SUCCESS_CODE) {
              logger.info("Receiver failed to receive data from {} because {}, retry.",
                  status.msg, snapshotFile.getAbsoluteFile());
              continue outer;
            }
          }
        }

        // the file is sent successfully
        String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
        SyncStatus status = serviceClient.checkDataMD5(md5OfSender);
        if (status.code == SUCCESS_CODE && md5OfSender.equals(status.msg)) {
          logger.info("Receiver has received {} successfully.", snapshotFile.getAbsoluteFile());
          break;
        } else {
          logger.error("MD5 check of tsfile {} failed, retry", snapshotFile.getAbsoluteFile());
        }
      }
    } catch (IOException | TException | NoSuchAlgorithmException e) {
      throw new SyncConnectionException("Cannot sync data with receiver.", e);
    }
  }

  private void endSync() throws IOException {
    File currentLocalFile = getCurrentLogFile();
    File lastLocalFile = new File(config.getLastFileInfoPath());

    // 1. Write file list to currentLocalFile
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(currentLocalFile))) {
      for (Map<Long, Set<File>> currentLocalFiles : lastLocalFilesMap.values()) {
        for (Set<File> files : currentLocalFiles.values()) {
          for(File file: files) {
            bw.write(file.getAbsolutePath());
            bw.newLine();
          }
          bw.flush();
        }
      }
    } catch (IOException e) {
      logger.error("Can not clear sync log {}", lastLocalFile.getAbsoluteFile(), e);
    }

    // 2. Rename currentLocalFile to lastLocalFile
    lastLocalFile.delete();
    FileUtils.moveFile(currentLocalFile, lastLocalFile);

    // 3. delete snapshot directory
    try {
      FileUtils.deleteDirectory(new File(config.getSnapshotPath()));
    } catch (IOException e) {
      logger.error("Can not clear snapshot directory {}", config.getSnapshotPath(), e);
    }

    // 4. delete sync log file
    getSyncLogFile().delete();
  }


  private File getSchemaPosFile() {
    return new File(IoTDBDescriptor.getInstance().getConfig().getSyncDir(),
        config.getSyncReceiverName() + File.separator + SyncConstant.SCHEMA_POS_FILE_NAME);
  }

  private File getSchemaLogFile() {
    return new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir(),
        MetadataConstant.METADATA_LOG);
  }

  private File getLockFile() {
    return new File(IoTDBDescriptor.getInstance().getConfig().getSyncDir(),
        config.getSyncReceiverName() + File.separator + SyncConstant.LOCK_FILE_NAME);
  }

  private File getUuidFile() {
    return new File(IoTDBDescriptor.getInstance().getConfig().getSyncDir(),
        SyncConstant.UUID_FILE_NAME);
  }

  private static class InstanceHolder {

    private static final SyncClient INSTANCE = new SyncClient();
  }

  private File getSyncLogFile() {
    return new File(config.getSenderFolderPath(), SyncConstant.SYNC_LOG_NAME);
  }

  private File getCurrentLogFile() {
    return new File(config.getSenderFolderPath(), SyncConstant.CURRENT_LOCAL_FILE_NAME);
  }

  public void setConfig(SyncSenderConfig config) {
    SyncClient.config = config;
  }
}
