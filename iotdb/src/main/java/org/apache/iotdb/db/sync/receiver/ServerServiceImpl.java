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
package org.apache.iotdb.db.sync.receiver;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.filenode.OverflowChangeType;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.sync.conf.Constans;
import org.apache.iotdb.db.utils.SyncUtils;
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

/**
 * @author Tianan Li
 */
public class ServerServiceImpl implements SyncService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(ServerServiceImpl.class);

  private static final FileNodeManager fileNodeManager = FileNodeManager.getInstance();
  /**
   * Metadata manager
   **/
  private static final MManager metadataManger = MManager.getInstance();

  private static final String SYNC_SERVER = Constans.SYNC_SERVER;

  private ThreadLocal<String> uuid = new ThreadLocal<>();
  /**
   * String means storage group,List means the set of new files(path) in local IoTDB and String
   * means path of new Files
   **/
  private ThreadLocal<Map<String, List<String>>> fileNodeMap = new ThreadLocal<>();
  /**
   * Map String1 means timeseries String2 means path of new Files, long means startTime
   **/
  private ThreadLocal<Map<String, Map<String, Long>>> fileNodeStartTime = new ThreadLocal<>();
  /**
   * Map String1 means timeseries String2 means path of new Files, long means endTime
   **/
  private ThreadLocal<Map<String, Map<String, Long>>> fileNodeEndTime = new ThreadLocal<>();

  /**
   * Total num of files that needs to be loaded
   */
  private ThreadLocal<Integer> fileNum = new ThreadLocal<>();

  /**
   * IoTDB ioTDBConfig
   **/
  private IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();

  /**
   * IoTDB data directory
   **/
  private String dataPath = ioTDBConfig.getDataDir();

  /**
   * IoTDB  multiple bufferWrite directory
   **/
  private String[] bufferWritePaths = ioTDBConfig.getBufferWriteDirs();

  /**
   * The path to store metadata file of sender
   */
  private ThreadLocal<String> schemaFromSenderPath = new ThreadLocal<>();

  /** Sync path of server **/
  private String syncPath;

  /**
   * Init threadLocal variable
   */
  @Override
  public void init(String storageGroup) {
    if (logger.isInfoEnabled()) {
      logger.info("Sync process starts to receive data of storage group {}", storageGroup);
    }
    fileNum.set(0);
    fileNodeMap.set(new HashMap<>());
    fileNodeStartTime.set(new HashMap<>());
    fileNodeEndTime.set(new HashMap<>());
  }

  /**
   * Verify IP address of sender
   */
  @Override
  public boolean checkIdentity(String uuid, String ipAddress) throws TException {
    this.uuid.set(uuid);
    initPath();
    return SyncUtils.verifyIPSegment(ioTDBConfig.getIpWhiteList(), ipAddress);
  }

  /**
   * Init file path and clear data if last sync process failed.
   */
  private void initPath() throws TException {
    if (dataPath.length() > 0 && dataPath.charAt(dataPath.length() - 1) != File.separatorChar) {
      dataPath = dataPath + File.separatorChar;
    }
    syncPath = dataPath + SYNC_SERVER + File.separator;
    schemaFromSenderPath
        .set(syncPath + this.uuid.get() + File.separator + MetadataConstant.METADATA_LOG);
    File syncFileDirectory = new File(syncPath,this.uuid.get());
    if (syncFileDirectory.exists()
        && Objects.requireNonNull(syncFileDirectory.list()).length != 0) {
      try {
        SyncUtils.deleteFile(syncFileDirectory);
      } catch (IOException e) {
        logger.error("Cannot clear useless metadata file.");
        throw new TException(e);
      }
    }
    for (String bufferWritePath : bufferWritePaths) {
      if (bufferWritePath.length() > 0
          && bufferWritePath.charAt(bufferWritePath.length() - 1) != File.separatorChar) {
        bufferWritePath = bufferWritePath + File.separatorChar;
      }
      String backupPath = bufferWritePath + SYNC_SERVER + File.separator;
      File backupDirectory = new File(backupPath, this.uuid.get());
      if (backupDirectory.exists() && Objects.requireNonNull(backupDirectory.list()).length != 0) {
        /** if does not exist, it means that the last time sync failed, clear data in the uuid directory and receive the data again **/
        try {
          SyncUtils.deleteFile(backupDirectory);
        } catch (IOException e) {
          logger.error("Cannot clear useless backup data file");
          throw new TException(e);
        }
      }
    }
  }

  /**
   * Acquire schema from sender
   *
   * @param status: SUCCESS_STATUS or PROCESSING_STATUS. status = SUCCESS_STATUS : finish receiving schema file, start to sync schema.
   * status = SUCCESS_STATUS : the schema file has not received completely.
   */
  @Override
  public void syncSchema(ByteBuffer schema, SyncDataStatus status) {
    if (status == SyncDataStatus.SUCCESS_STATUS) {
      /** sync metadata, include storage group and timeseries **/
      loadMetadata();
    } else {
      File file = new File(schemaFromSenderPath.get());
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          if (!file.createNewFile()) {
            logger.error("Cannot create file {}", file.getPath());
          }
        } catch (IOException e) {
          logger.error("Cannot make schema file {}.", file.getPath(),e);
        }
      }
      try (FileOutputStream fos = new FileOutputStream(file, true);
          FileChannel channel = fos.getChannel()) {
        channel.write(schema);
      } catch (Exception e) {
        logger.error("Cannot write data to file {}.", file.getPath(),e);
      }
    }

  }

  /**
   * Load metadata from sender
   */
  private void loadMetadata() {
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
      } catch (IOException e) {
        logger.error("Cannot insert schema to IoTDB.", e);
      } catch (Exception e) {
        logger.error("Parse metadata operation failed.", e);
      }
    }
  }

  /**
   * Operate metadata operation in MManager
   *
   * @param cmd metadata operation
   */
  private void operation(String cmd)
      throws PathErrorException, IOException, MetadataArgsErrorException {
    String[] args = cmd.trim().split(",");
    switch (args[0]) {
      case MetadataOperationType.ADD_PATH_TO_MTREE:
        Map<String, String> props = null;
        String[] kv;
        props = new HashMap<>(args.length - 5 + 1, 1);
        for (int k = 5; k < args.length; k++) {
          kv = args[k].split("=");
          props.put(kv[0], kv[1]);
        }
        metadataManger.addPathToMTree(args[1], TSDataType.deserialize(Short.valueOf(args[2])),
            TSEncoding.deserialize(Short.valueOf(args[3])),
            CompressionType.deserialize(Short.valueOf(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        metadataManger.deletePathFromMTree(args[1]);
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
   * @param status status = SUCCESS_STATUS : finish receiving one tsfile status = PROCESSING_STATUS : tsfile has not received
   * completely.
   */
  @Override
  public String syncData(String md5OfSender, List<String> filePathSplit,
      ByteBuffer dataToReceive, SyncDataStatus status) {
    String md5OfReceiver = "";
    StringBuilder filePathBuilder = new StringBuilder();
    FileChannel channel;
    /**Recombination File Path**/
    for (int i = 0; i < filePathSplit.size(); i++) {
      if (i == filePathSplit.size() - 1) {
        filePathBuilder.append(filePathSplit.get(i));
      } else {
        filePathBuilder.append(filePathSplit.get(i)).append(File.separator);
      }
    }
    String filePath = filePathBuilder.toString();
    if(syncPath.length() > 0 && syncPath.charAt(syncPath.length()-1)!=File.separatorChar){
      syncPath = syncPath+File.separatorChar;
    }
    filePath = syncPath + uuid.get() + File.separator + filePath;
    if (status == SyncDataStatus.PROCESSING_STATUS) { // there are still data stream to add
      File file = new File(filePath);
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          if (!file.createNewFile()) {
            logger.error("cannot create file {}", file.getPath());
          }
        } catch (IOException e) {
          logger.error("cannot make file {}", file.getPath(), e);
        }
      }
      try (FileOutputStream fos = new FileOutputStream(file, true)) {// append new data
        channel = fos.getChannel();
        channel.write(dataToReceive);
      } catch (IOException e) {
        logger.error("cannot write data to file {}",file.getPath(), e);

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
          if (logger.isInfoEnabled()) {
            logger.info(String.format("Receiver has received %d files from sender", fileNum.get()));
          }
        } else {
          if (!new File(filePath).delete()) {
            logger.error("Receiver can not delete file {}", new File(filePath).getPath());
          }
        }
      } catch (Exception e) {
        logger.error("Receiver cannot generate md5", e);
      }
    }
    return md5OfReceiver;
  }


  @Override
  public boolean load() throws TException {
    getFileNodeInfo();
    loadData();
    try {
      SyncUtils.deleteFile(new File(syncPath + this.uuid.get()));
    } catch (IOException e) {
      throw new TException(e);
    }
    for (String bufferWritePath : bufferWritePaths) {
      if (bufferWritePath.length() > 0
          && bufferWritePath.charAt(bufferWritePath.length() - 1) != File.separatorChar) {
        bufferWritePath = bufferWritePath + File.separatorChar;
      }
      String backupPath = bufferWritePath + SYNC_SERVER + File.separator;
      File backupDirectory = new File(backupPath, this.uuid.get());
      if (backupDirectory.exists() && Objects.requireNonNull(backupDirectory.list()).length != 0) {
        try {
          SyncUtils.deleteFile(backupDirectory);
        } catch (IOException e) {
          logger.error("Cannot clear useless backup data file");
          throw new TException(e);
        }
      }
    }
    return true;
  }

  /**
   * Get all tsfiles' info which are sent from sender, it is prepare for merging these data
   */
  public void getFileNodeInfo() {
    File dataFileRoot = new File(syncPath,this.uuid.get());
    File[] files = dataFileRoot.listFiles();
    int processedNum = 0;
    for (File storageGroupPB : files) {
      List<String> filesPath = new ArrayList<>();
      File[] filesSG = storageGroupPB.listFiles();
      for (File fileTF : filesSG) { // fileTF means TsFiles
        Map<String, Long> startTimeMap = new HashMap<>();
        Map<String, Long> endTimeMap = new HashMap<>();
        TsFileSequenceReader reader = null;
        try {
          reader = new TsFileSequenceReader(fileTF.getPath());
          Map<String, TsDeviceMetadataIndex> deviceIdMap = reader.readFileMetadata().getDeviceMap();
          Iterator<String> it = deviceIdMap.keySet().iterator();
          while (it.hasNext()) {
            String key = it.next();
            TsDeviceMetadataIndex device = deviceIdMap.get(key);
            startTimeMap.put(key, device.getStartTime());
            endTimeMap.put(key, device.getEndTime());
          }
        } catch (Exception e) {
          logger.error("Unable to read tsfile {}", fileTF.getPath(), e);
        } finally {
          try {
            if (reader != null) {
              reader.close();
            }
          } catch (IOException e) {
            logger.error("Cannot close file stream {}", fileTF.getPath(), e);
          }
        }
        fileNodeStartTime.get().put(fileTF.getAbsolutePath(), startTimeMap);
        fileNodeEndTime.get().put(fileTF.getAbsolutePath(), endTimeMap);
        filesPath.add(fileTF.getAbsolutePath());
        processedNum++;
        if (logger.isInfoEnabled()) {
          logger.info(String
              .format("Get tsfile info has complete : %d/%d", processedNum, fileNum.get()));
        }
        fileNodeMap.get().put(storageGroupPB.getName(), filesPath);
      }
    }
  }


  /**
   * It is to merge data. If data in the tsfile is new, append the tsfile to the storage group
   * directly. If data in the tsfile is old, it has two strategy to merge.It depends on the
   * possibility of updating historical data.
   */
  public void loadData() {
    if(syncPath.length() > 0 && syncPath.charAt(syncPath.length()-1) != File.separatorChar){
      syncPath =syncPath + File.separatorChar;
    }
    int processedNum = 0;
    for (String storageGroup : fileNodeMap.get().keySet()) {
      List<String> filesPath = fileNodeMap.get().get(storageGroup);
      /**  before load external tsFile, it is necessary to order files in the same storage group **/
      for (int i = 0; i < filesPath.size(); i++) {
        for (int j = i + 1; j < filesPath.size(); j++) {
          boolean swapOrNot = false;
          Map<String, Long> startTimeI = fileNodeStartTime.get().get(filesPath.get(i));
          Map<String, Long> endTimeI = fileNodeStartTime.get().get(filesPath.get(i));
          Map<String, Long> startTimeJ = fileNodeStartTime.get().get(filesPath.get(j));
          Map<String, Long> endTimeJ = fileNodeStartTime.get().get(filesPath.get(j));
          for (String deviceId : endTimeI.keySet()) {
            if (startTimeJ.containsKey(deviceId) && startTimeI.get(deviceId) >
                endTimeJ.get(deviceId)) {
              swapOrNot = true;
              break;
            }
          }
          if (swapOrNot) {
            String temp = filesPath.get(i);
            filesPath.set(i, filesPath.get(j));
            filesPath.set(j, temp);
          }
        }
      }

      for (String path : filesPath) {
        // get startTimeMap and endTimeMap
        Map<String, Long> startTimeMap = fileNodeStartTime.get().get(path);
        Map<String, Long> endTimeMap = fileNodeEndTime.get().get(path);

        // create a new fileNode
        String header = syncPath + uuid.get() + File.separator;
        String relativePath = path.substring(header.length());
        TsFileResource fileNode = new TsFileResource(startTimeMap, endTimeMap,
            OverflowChangeType.NO_CHANGE,
            Directories.getInstance().getNextFolderIndexForTsFile(), relativePath);
        // call interface of load external file
        try {
          if (!fileNodeManager.appendFileToFileNode(storageGroup, fileNode, path)) {
            // it is a file with overflow data
            if (ioTDBConfig.isUpdate_historical_data_possibility()) {
              loadOldData(path);
            } else {
              List<String> overlapFiles = fileNodeManager.getOverlapFilesFromFileNode(
                  storageGroup,
                  fileNode, uuid.get());
              if (overlapFiles.isEmpty()) {
                loadOldData(path);
              } else {
                loadOldData(path, overlapFiles);
              }
            }
          }
        } catch (FileNodeManagerException e) {
          logger.error("Can not load external file ", e);
        }

        processedNum++;
        if (logger.isInfoEnabled()) {
          logger.info(String
              .format("Merging files has completed : %d/%d", processedNum, fileNum.get()));
        }
      }
    }
  }

  /**
   * Insert all data in the tsfile into IoTDB.
   */
  public void loadOldData(String filePath) {
    Set<String> timeseriesSet = new HashSet<>();
    TsFileSequenceReader reader = null;
    OverflowQPExecutor insertExecutor = new OverflowQPExecutor();
    try {
      /** use tsfile reader to get data **/
      reader = new TsFileSequenceReader(filePath);
      Map<String, TsDeviceMetadataIndex> deviceIdMap = reader.readFileMetadata().getDeviceMap();
      Iterator<Entry<String, TsDeviceMetadataIndex>> entryIterator = deviceIdMap.entrySet()
          .iterator();
      while (entryIterator.hasNext()) {
        Entry<String, TsDeviceMetadataIndex> deviceMIEntry = entryIterator.next();
        String deviceId = deviceMIEntry.getKey();
        TsDeviceMetadataIndex deviceMI = deviceMIEntry.getValue();
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(deviceMI);
        List<ChunkGroupMetaData> rowGroupMetadataList = deviceMetadata.getChunkGroupMetaDataList();
        timeseriesSet.clear();
        /** firstly, get all timeseries in the same device **/
        for (ChunkGroupMetaData chunkGroupMetaData : rowGroupMetadataList) {
          List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData
              .getChunkMetaDataList();
          for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
            String measurementUID = chunkMetaData.getMeasurementUid();
            measurementUID = deviceId + "." + measurementUID;
            timeseriesSet.add(measurementUID);
          }
        }
        /** Secondly, use tsFile Reader to form SQL **/
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        List<Path> paths = new ArrayList<>();
        paths.clear();
        for (String timeseries : timeseriesSet) {
          paths.add(new Path(timeseries));
        }
        QueryExpression queryExpression = QueryExpression.create(paths, null);
        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        InsertPlan insertPlan;
        while (queryDataSet.hasNext()) {
          RowRecord record = queryDataSet.next();
          List<Field> fields = record.getFields();
          List<String> measurementList = new ArrayList<>();
          List<String> insertValues = new ArrayList<>();
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (!field.isNull()) {
              measurementList.add(paths.get(i).getMeasurement());
              if (fields.get(i).getDataType() == TSDataType.TEXT) {
                insertValues.add(String.format("'%s'", field.toString()));
              } else {
                insertValues.add(String.format("%s", field.toString()));
              }
            }
          }
          insertPlan = new InsertPlan(deviceId, record.getTimestamp(), measurementList,
              insertValues);
          insertExecutor.processNonQuery(insertPlan);
        }
      }
    } catch (IOException e) {
      logger.error("Receiver can not parse tsfile into SQL", e);
    } catch (ProcessorException e) {
      logger.error("Meet error while processing non-query.", e);
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        logger.error("Cannot close file stream {}", filePath, e);
      }
    }
  }

  /**
   * Insert those valid data in the tsfile into IoTDB
   *
   * @param overlapFiles:files which are conflict with the sync file
   */
  public void loadOldData(String filePath, List<String> overlapFiles) {
    Set<String> timeseriesList = new HashSet<>();
    TsFileSequenceReader reader = null;
    OverflowQPExecutor insertExecutor = new OverflowQPExecutor();
    try {
      reader = new TsFileSequenceReader(filePath);
      Map<String, TsDeviceMetadataIndex> deviceIdMap = reader.readFileMetadata().getDeviceMap();
      Iterator<String> it = deviceIdMap.keySet().iterator();
      while (it.hasNext()) {
        String deviceID = it.next();
        TsDeviceMetadataIndex deviceMI = deviceIdMap.get(deviceID);
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(deviceMI);
        List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata
            .getChunkGroupMetaDataList();
        timeseriesList.clear();
        /** firstly, get all timeseries in the same device **/
        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
          List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData.getChunkMetaDataList();
          for (ChunkMetaData timeSeriesChunkMetaData : chunkMetaDataList) {
            String measurementUID = timeSeriesChunkMetaData.getMeasurementUid();
            measurementUID = deviceID + "." + measurementUID;
            timeseriesList.add(measurementUID);
          }
        }
        /** secondly, use tsFile Reader to form SQL **/
        ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        /** compare data with one timeseries in a round to get valid data **/
        for (String timeseries : timeseriesList) {
          paths.clear();
          paths.add(new Path(timeseries));
          Map<InsertPlan, String> originDataPoint = new HashMap<>();
          Map<InsertPlan, String> newDataPoint = new HashMap<>();
          QueryExpression queryExpression = QueryExpression.create(paths, null);
          QueryDataSet queryDataSet = readOnlyTsFile.query(queryExpression);
          while (queryDataSet.hasNext()) {
            RowRecord record = queryDataSet.next();
            List<Field> fields = record.getFields();
            /** get all data with the timeseries in the sync file **/
            for (int i = 0; i < fields.size(); i++) {
              Field field = fields.get(i);
              List<String> measurementList = new ArrayList<>();
              if (!field.isNull()) {
                measurementList.add(paths.get(i).getMeasurement());
                InsertPlan insertPlan = new InsertPlan(deviceID, record.getTimestamp(),
                    measurementList, new ArrayList<>());
                newDataPoint.put(insertPlan,
                    field.getDataType() == TSDataType.TEXT ? String.format("'%s'", field.toString())
                        : field.toString());
              }
            }
          }
          /** get all data with the timeseries in all overlap files. **/
          for (String overlapFile : overlapFiles) {
            TsFileSequenceReader inputOverlap = null;
            try {
              inputOverlap = new TsFileSequenceReader(overlapFile);
              ReadOnlyTsFile readTsFileOverlap = new ReadOnlyTsFile(inputOverlap);
              QueryDataSet queryDataSetOverlap = readTsFileOverlap.query(queryExpression);
              while (queryDataSetOverlap.hasNext()) {
                RowRecord recordOverlap = queryDataSetOverlap.next();
                List<Field> fields = recordOverlap.getFields();
                for (int i = 0; i < fields.size(); i++) {
                  Field field = fields.get(i);
                  List<String> measurementList = new ArrayList<>();
                  if (!field.isNull()) {
                    measurementList.add(paths.get(i).getMeasurement());
                    InsertPlan insertPlan = new InsertPlan(deviceID, recordOverlap.getTimestamp(),
                        measurementList, new ArrayList<>());
                    originDataPoint.put(insertPlan,
                        field.getDataType() == TSDataType.TEXT ? String
                            .format("'%s'", field.toString())
                            : field.toString());
                  }
                }
              }
            } finally {
              if (inputOverlap != null) {
                inputOverlap.close();
              }
            }
          }

          /** If there has no overlap data with the timeseries, inserting all data in the sync file **/
          if (originDataPoint.isEmpty()) {
            for (Map.Entry<InsertPlan, String> entry : newDataPoint.entrySet()) {
              InsertPlan insertPlan = entry.getKey();
              List<String> insertValues = new ArrayList<>();
              insertValues.add(entry.getValue());
              insertPlan.setValues(insertValues);
              insertExecutor.processNonQuery(insertPlan);
            }
          } else {
            /** Compare every data to get valid data **/
            for (Map.Entry<InsertPlan, String> entry : newDataPoint.entrySet()) {
              if (!originDataPoint.containsKey(entry.getKey())
                  || (originDataPoint.containsKey(entry.getKey())
                  && !originDataPoint.get(entry.getKey()).equals(entry.getValue()))) {
                InsertPlan insertPlan = entry.getKey();
                List<String> insertValues = new ArrayList<>();
                insertValues.add(entry.getValue());
                insertPlan.setValues(insertValues);
                insertExecutor.processNonQuery(insertPlan);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("Can not parse tsfile into SQL", e);
    } catch (ProcessorException e) {
      logger.error("Meet error while processing non-query.", e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        logger.error("Cannot close file stream {}", filePath, e);
      }
    }
  }

  /**
   * Release threadLocal variable resources
   */
  @Override
  public void cleanUp() {
    uuid.remove();
    fileNum.remove();
    fileNodeMap.remove();
    fileNodeStartTime.remove();
    fileNodeEndTime.remove();
    schemaFromSenderPath.remove();
    logger.info("Synchronization has finished!");
  }

  public Map<String, List<String>> getFileNodeMap() {
    return fileNodeMap.get();
  }

  public void setFileNodeMap(Map<String, List<String>> fileNodeMap) {
    this.fileNodeMap.set(fileNodeMap);
  }
}