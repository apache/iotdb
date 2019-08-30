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
package org.apache.iotdb.db.sync.receiver.transfer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.sync.receiver.load.FileLoader;
import org.apache.iotdb.db.sync.receiver.load.FileLoaderManager;
import org.apache.iotdb.db.sync.receiver.recover.SyncReceiverLogger;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.service.sync.thrift.ResultStatus;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncServiceImpl implements SyncService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(SyncServiceImpl.class);

  private static final MManager METADATA_MANGER = MManager.getInstance();

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ThreadLocal<String> syncFolderPath = new ThreadLocal<>();

  private ThreadLocal<String> currentSG = new ThreadLocal<>();

  private ThreadLocal<SyncReceiverLogger> syncLog = new ThreadLocal<>();

  private ThreadLocal<String> senderName = new ThreadLocal<>();

  private ThreadLocal<File> currentFile = new ThreadLocal<>();

  private ThreadLocal<FileChannel> currentFileWriter = new ThreadLocal<>();

  private ThreadLocal<MessageDigest> messageDigest = new ThreadLocal<>();

  /**
   * Verify IP address of sender
   */
  @Override
  public ResultStatus check(String ipAddress, String uuid) {
    Thread.currentThread().setName(ThreadName.SYNC_SERVER.getName());
    if (SyncUtils.verifyIPSegment(config.getIpWhiteList(), ipAddress)) {
      senderName.set(ipAddress + Constans.SYNC_DIR_NAME_SEPARATOR + uuid);
      if (checkRecovery()) {
        return getSuccessResult();
      } else {
        return getErrorResult("Receiver is processing data from previous sync tasks");
      }
    } else {
      return getErrorResult(
          "Sender IP is not in the white list of receiver IP and synchronization tasks are not allowed.");
    }
  }

  private boolean checkRecovery() {
    try {
      if (currentFileWriter.get() != null && currentFileWriter.get().isOpen()) {
        currentFileWriter.get().close();
      }
      return true;
    } catch (IOException e) {
      logger.error("Check recovery state fail", e);
      return false;
    }
  }

  @Override
  public ResultStatus startSync() {
    try {
      initPath();
      currentSG.remove();
      FileLoader.createFileLoader(senderName.get(), syncFolderPath.get());
      syncLog.set(new SyncReceiverLogger(new File(getSyncDataPath(), Constans.SYNC_LOG_NAME)));
      return getSuccessResult();
    } catch (DiskSpaceInsufficientException | IOException e) {
      logger.error("Can not receiver data from sender", e);
      return getErrorResult(e.getMessage());
    }
  }

  /**
   * Init file path and clear data if last sync process failed.
   */
  private void initPath() throws DiskSpaceInsufficientException {
    String dataDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    syncFolderPath
        .set(FilePathUtils.regularizePath(dataDir) + Constans.SYNC_RECEIVER + File.separatorChar
            + senderName.get());
  }

  /**
   * Init threadLocal variable and delete old useless files.
   */
  @Override
  public ResultStatus init(String storageGroup) {
    logger.info("Sync process starts to receive data of storage group {}", storageGroup);
    currentSG.set(storageGroup);
    try {
      syncLog.get().startSyncDeletedFilesName();
    } catch (IOException e) {
      logger.error("Can not init sync process", e);
      return getErrorResult(e.getMessage());
    }
    return getSuccessResult();
  }

  @Override
  public ResultStatus syncDeletedFileName(String fileName) throws TException {
    try {
      syncLog.get().finishSyncDeletedFileName(new File(fileName));
      FileLoaderManager.getInstance().getFileLoader(senderName.get())
          .addDeletedFileName(new File(fileName));
    } catch (IOException e) {
      logger.error("Can not sync deleted file", e);
      return getErrorResult(
          String.format("Can not sync deleted file %s because %s", fileName, e.getMessage()));
    }
    return getSuccessResult();
  }

  @Override
  public ResultStatus initSyncData(String filename) throws TException {
    try {
      File file;
      if (currentSG.get() == null) { // schema mlog.txt file
        file = new File(getSyncDataPath(), filename);
      } else {
        file = new File(getSyncDataPath(), currentSG.get() + File.separatorChar + filename);
      }
      currentFile.set(file);
      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdirs();
      }
      if (currentFileWriter.get() != null && currentFileWriter.get().isOpen()) {
        currentFileWriter.get().close();
      }
      currentFileWriter.set(new FileOutputStream(file).getChannel());
      syncLog.get().startSyncTsFiles();
    } catch (IOException e) {
      logger.error("Can not init sync resource for file {}", filename, e);
      return getErrorResult(
          String.format("Can not init sync resource for file %s because %s", filename,
              e.getMessage()));
    }
    return getSuccessResult();
  }

  @Override
  public ResultStatus syncData(ByteBuffer buff) {
    try {
      currentFileWriter.get().write(buff);
      messageDigest.get().update(buff);
    } catch (IOException e) {
      logger.error("Can not sync data for file {}", currentFile.get().getAbsoluteFile(), e);
      return getErrorResult(String
          .format("Can not sync data for file %s because %s", currentFile.get().getName(),
              e.getMessage()));
    }
    return getSuccessResult();
  }

  @Override
  public ResultStatus checkDataMD5(String md5OfSender) throws TException {
    String md5OfReceiver = (new BigInteger(1, messageDigest.get().digest())).toString(16);
    try {
      currentFileWriter.get().close();
      if (!md5OfSender.equals(md5OfReceiver)) {
        FileUtils.forceDelete(currentFile.get());
        currentFileWriter.set(new FileOutputStream(currentFile.get()).getChannel());
      } else {
        if (currentFile.get().getName().endsWith(MetadataConstant.METADATA_LOG)) {
          loadMetadata();
        } else {
          if (!currentFile.get().getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
            syncLog.get().finishSyncTsfile(currentFile.get());
            FileLoaderManager.getInstance().getFileLoader(senderName.get())
                .addTsfile(currentFile.get());
          }
        }
      }
    } catch (IOException e) {
      logger.error("Can not check data MD5 for file {}", currentFile.get().getAbsoluteFile(), e);
      return getErrorResult(String
          .format("Can not check data MD5 for file %s because %s", currentFile.get().getName(),
              e.getMessage()));
    }
    return new ResultStatus(true, null, md5OfReceiver);
  }

  private boolean loadMetadata() {
    if (currentFile.get().exists()) {
      try (BufferedReader br = new BufferedReader(
          new java.io.FileReader(currentFile.get()))) {
        String metadataOperation;
        while ((metadataOperation = br.readLine()) != null) {
          operation(metadataOperation);
        }
      } catch (FileNotFoundException e) {
        logger.error("Cannot read the file {}.", currentFile.get().getAbsoluteFile(), e);
        return false;
      } catch (IOException | MetadataErrorException | PathErrorException e) {
        // multiple insert schema, ignore it.
      }
    }
    return true;
  }

  /**
   * Operate metadata operation in MManager
   *
   * @param cmd metadata operation
   */
  private void operation(String cmd)
      throws PathErrorException, IOException, MetadataErrorException {
    String[] args = cmd.trim().split(",");
    switch (args[0]) {
      case MetadataOperationType.ADD_PATH_TO_MTREE:
        Map<String, String> props;
        String[] kv;
        props = new HashMap<>(args.length - 5 + 1, 1);
        for (int k = 5; k < args.length; k++) {
          kv = args[k].split("=");
          props.put(kv[0], kv[1]);
        }
        METADATA_MANGER
            .addPathToMTree(new Path(args[1]), TSDataType.deserialize(Short.valueOf(args[2])),
                TSEncoding.deserialize(Short.valueOf(args[3])),
                CompressionType.deserialize(Short.valueOf(args[4])),
                props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        METADATA_MANGER.deletePaths(Collections.singletonList(new Path(args[1])));
        break;
      case MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE:
        METADATA_MANGER.setStorageLevelToMTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PTREE:
        METADATA_MANGER.addAPTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PATH_TO_PTREE:
        METADATA_MANGER.addPathToPTree(args[1]);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_PTREE:
        METADATA_MANGER.deletePathFromPTree(args[1]);
        break;
      case MetadataOperationType.LINK_MNODE_TO_PTREE:
        METADATA_MANGER.linkMNodeToPTree(args[1], args[2]);
        break;
      case MetadataOperationType.UNLINK_MNODE_FROM_PTREE:
        METADATA_MANGER.unlinkMNodeFromPTree(args[1], args[2]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  @Override
  public ResultStatus endSync() throws TException {
    try {
      syncLog.get().close();
      new File(getSyncDataPath(), Constans.SYNC_END).createNewFile();
      FileLoaderManager.getInstance().getFileLoader(senderName.get()).endSync();
    } catch (IOException e) {
      logger.error("Can not end sync", e);
      return getErrorResult(String.format("Can not end sync because %s", e.getMessage()));
    }
    return getSuccessResult();
  }

  private String getSyncDataPath() {
    return syncFolderPath.get() + File.separatorChar + Constans.RECEIVER_DATA_FOLDER_NAME;
  }

  private ResultStatus getSuccessResult() {
    return new ResultStatus(true, null, null);
  }

  private ResultStatus getErrorResult(String errorMsg) {
    return new ResultStatus(false, errorMsg, null);
  }

}