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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.service.sync.thrift.ResultStatus;
import org.apache.iotdb.service.sync.thrift.SyncDataStatus;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncServiceImpl implements SyncService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(SyncServiceImpl.class);

  private static final StorageEngine STORAGE_GROUP_MANAGER = StorageEngine.getInstance();
  /**
   * Metadata manager
   **/
  private static final MManager metadataManger = MManager.getInstance();

  private static final String SYNC_SERVER = Constans.SYNC_RECEIVER;

  /**
   * String means storage group,List means the set of new files(path) in local IoTDB and String
   * means path of new Files
   **/
  private ThreadLocal<Map<String, List<String>>> fileNodeMap = new ThreadLocal<>();

  /**
   * Total num of files that needs to be loaded
   */
  private ThreadLocal<Integer> fileNum = new ThreadLocal<>();

  /**
   * IoTDB config
   **/
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * IoTDB data directory
   **/
  private String baseDir = config.getBaseDir();

  /**
   * IoTDB  multiple bufferWrite directory
   **/
  private String[] bufferWritePaths = config.getDataDirs();

  /**
   * The path to store metadata file of sender
   */
  private ThreadLocal<String> schemaFromSenderPath = new ThreadLocal<>();

  /**
   * Sync folder path of server
   **/
  private ThreadLocal<String> syncFolderPath = new ThreadLocal<>();

  /**
   * Sync data path of server
   */
  private ThreadLocal<String> syncDataPath = new ThreadLocal<>();

  private ThreadLocal<String> currentSG = new ThreadLocal<>();


  /**
   * Verify IP address of sender
   */
  @Override
  public ResultStatus checkIdentity(String ipAddress) {
    Thread.currentThread().setName(ThreadName.SYNC_SERVER.getName());
    initPath();
    return SyncUtils.verifyIPSegment(config.getIpWhiteList(), ipAddress) ? ResultStatus.SUCCESS
        : ResultStatus.FAILURE;
  }

  /**
   * Init threadLocal variable and delete old useless files.
   */
  @Override
  public ResultStatus init(String storageGroup) {
    logger.info("Sync process starts to receive data of storage group {}", storageGroup);
    currentSG.set(storageGroup);
    fileNum.set(0);
    fileNodeMap.set(new HashMap<>());
    try {
      FileUtils.deleteDirectory(new File(syncDataPath));
    } catch (IOException e) {
      logger.error("cannot delete directory {} ", syncFolderPath);
      return ResultStatus.FAILURE;
    }
    for (String bufferWritePath : bufferWritePaths) {
      bufferWritePath = FilePathUtils.regularizePath(bufferWritePath);
      String backupPath = bufferWritePath + SYNC_SERVER + File.separator;
      File backupDirectory = new File(backupPath, this.uuid.get());
      if (backupDirectory.exists() && backupDirectory.list().length != 0) {
        try {
          FileUtils.deleteDirectory(backupDirectory);
        } catch (IOException e) {
          logger.error("cannot delete directory {} ", syncFolderPath);
          return ResultStatus.FAILURE;
        }
      }
    }
    return ResultStatus.SUCCESS;
  }

  /**
   * Init file path and clear data if last sync process failed.
   */
  private void initPath() {
    baseDir = FilePathUtils.regularizePath(baseDir);
    syncFolderPath = baseDir + SYNC_SERVER + File.separatorChar + this.uuid.get();
    syncDataPath = syncFolderPath + File.separatorChar + Constans.DATA_SNAPSHOT_NAME;
    schemaFromSenderPath
        .set(syncFolderPath + File.separator + MetadataConstant.METADATA_LOG);
  }

  /**
   * Acquire schema from sender
   *
   * @param status: FINIFSH_STATUS, SUCCESS_STATUS or PROCESSING_STATUS. status = FINISH_STATUS :
   * finish receiving schema file, start to sync schema. status = PROCESSING_STATUS : the schema
   * file has not received completely.SUCCESS_STATUS: load metadata.
   */
  @Override
  public String syncSchema(String md5, ByteBuffer schema, SyncDataStatus status) {
    String md5OfReceiver = Boolean.toString(Boolean.TRUE);
    if (status == SyncDataStatus.SUCCESS_STATUS) {
      /** sync metadata, include storage group and timeseries **/
      return Boolean.toString(loadMetadata());
    } else if (status == SyncDataStatus.PROCESSING_STATUS) {
      File file = new File(schemaFromSenderPath.get());
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          logger.error("Cannot make schema file {}.", file.getPath(), e);
          md5OfReceiver = Boolean.toString(Boolean.FALSE);
        }
      }
      try (FileOutputStream fos = new FileOutputStream(file, true);
          FileChannel channel = fos.getChannel()) {
        channel.write(schema);
      } catch (Exception e) {
        logger.error("Cannot insert data to file {}.", file.getPath(), e);
        md5OfReceiver = Boolean.toString(Boolean.FALSE);
      }
    } else {
      try (FileInputStream fis = new FileInputStream(schemaFromSenderPath.get())) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] buffer = new byte[Constans.DATA_CHUNK_SIZE];
        int n;
        while ((n = fis.read(buffer)) != -1) {
          md.update(buffer, 0, n);
        }
        md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
        if (!md5.equals(md5OfReceiver)) {
          FileUtils.forceDelete(new File(schemaFromSenderPath.get()));
        }
      } catch (Exception e) {
        logger.error("Receiver cannot generate md5 {}", schemaFromSenderPath.get(), e);
      }
    }
    return md5OfReceiver;
  }

  @Override
  public String checkDataMD5(String md5) throws TException {
    return null;
  }

  @Override
  public void endSync() throws TException {

  }

  /**
   * Load metadata from sender
   */
  private boolean loadMetadata() {
    if (new File(schemaFromSenderPath.get()).exists()) {
      try (BufferedReader br = new BufferedReader(
          new java.io.FileReader(schemaFromSenderPath.get()))) {
        String metadataOperation;
        while ((metadataOperation = br.readLine()) != null) {
          operation(metadataOperation);
        }
      } catch (FileNotFoundException e) {
        logger.error("Cannot read the file {}.",
            schemaFromSenderPath.get(), e);
        return false;
      } catch (IOException e) {
        /** multiple insert schema, ignore it **/
      } catch (Exception e) {
        logger.error("Parse metadata operation failed.", e);
        return false;
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
        metadataManger.addPathToMTree(new Path(args[1]), TSDataType.deserialize(Short.valueOf(args[2])),
            TSEncoding.deserialize(Short.valueOf(args[3])),
            CompressionType.deserialize(Short.valueOf(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        metadataManger.deletePaths(Collections.singletonList(new Path(args[1])));
        break;
      case MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE:
        metadataManger.setStorageLevelToMTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PTREE:
        metadataManger.addAPTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PATH_TO_PTREE:
        metadataManger.addPathToPTree(args[1]);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_PTREE:
        metadataManger.deletePathFromPTree(args[1]);
        break;
      case MetadataOperationType.LINK_MNODE_TO_PTREE:
        metadataManger.linkMNodeToPTree(args[1], args[2]);
        break;
      case MetadataOperationType.UNLINK_MNODE_FROM_PTREE:
        metadataManger.unlinkMNodeFromPTree(args[1], args[2]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  /**
   * Start receiving tsfile from sender
   *
   * @param status status = SUCCESS_STATUS : finish receiving one tsfile status = PROCESSING_STATUS
   * : tsfile has not received completely.
   */
  @Override
  public String syncData(String md5OfSender, List<String> filePathSplit,
      ByteBuffer dataToReceive, SyncDataStatus status) {
    String md5OfReceiver = Boolean.toString(Boolean.TRUE);
    FileChannel channel;
    /** Recombination File Path **/
    String filePath = StringUtils.join(filePathSplit, File.separatorChar);
    syncDataPath = FilePathUtils.regularizePath(syncDataPath);
    filePath = syncDataPath + filePath;
    if (status == SyncDataStatus.PROCESSING_STATUS) { // there are still data stream to add
      File file = new File(filePath);
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          logger.error("cannot make file {}", file.getPath(), e);
          md5OfReceiver = Boolean.toString(Boolean.FALSE);
        }
      }
      try (FileOutputStream fos = new FileOutputStream(file, true)) {// append new data
        channel = fos.getChannel();
        channel.write(dataToReceive);
      } catch (IOException e) {
        logger.error("cannot insert data to file {}", file.getPath(), e);
        md5OfReceiver = Boolean.toString(Boolean.FALSE);

      }
    } else { // all data in the same file has received successfully
      try (FileInputStream fis = new FileInputStream(filePath)) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] buffer = new byte[Constans.DATA_CHUNK_SIZE];
        int n;
        while ((n = fis.read(buffer)) != -1) {
          md.update(buffer, 0, n);
        }
        md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
        if (md5OfSender.equals(md5OfReceiver)) {
          fileNum.set(fileNum.get() + 1);

          logger.info(String.format("Receiver has received %d files from sender", fileNum.get()));
        } else {
          FileUtils.forceDelete(new File(filePath));
        }
      } catch (Exception e) {
        logger.error("Receiver cannot generate md5 {}", filePath, e);
      }
    }
    return md5OfReceiver;
  }


  @Override
  public boolean load() {
    try {
      getFileNodeInfo();
      loadData();
    } catch (Exception e) {
      logger.error("fail to load data", e);
      return false;
    }
    return true;
  }

  /**
   * Open all tsfile reader and cache
   */
  private Map<String, ReadOnlyTsFile> openReaders(String filePath, List<String> overlapFiles)
      throws IOException {
    Map<String, ReadOnlyTsFile> tsfileReaders = new HashMap<>();
    tsfileReaders.put(filePath, new ReadOnlyTsFile(new TsFileSequenceReader(filePath)));
    for (String overlapFile : overlapFiles) {
      tsfileReaders.put(overlapFile, new ReadOnlyTsFile(new TsFileSequenceReader(overlapFile)));
    }
    return tsfileReaders;
  }

  /**
   * Close all tsfile reader
   */
  private void closeReaders(Map<String, ReadOnlyTsFile> readers) throws IOException {
    for (ReadOnlyTsFile tsfileReader : readers.values()) {
      tsfileReader.close();
    }
  }

  /**
   * Release threadLocal variable resources
   */
  @Override
  public void cleanUp() {
    fileNum.remove();
    fileNodeMap.remove();
    schemaFromSenderPath.remove();
    try {
      FileUtils.deleteDirectory(new File(syncFolderPath));
    } catch (IOException e) {
      logger.error("can not delete directory {}", syncFolderPath, e);
    }
    logger.info("Synchronization has finished!");
  }

  public Map<String, List<String>> getFileNodeMap() {
    return fileNodeMap.get();
  }

  public void setFileNodeMap(Map<String, List<String>> fileNodeMap) {
    this.fileNodeMap.set(fileNodeMap);
  }

}