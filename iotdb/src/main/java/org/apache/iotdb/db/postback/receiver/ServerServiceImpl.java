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
package org.apache.iotdb.db.postback.receiver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.engine.filenode.OverflowChangeType;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.utils.PostbackUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lta
 */
public class ServerServiceImpl implements ServerService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerServiceImpl.class);
  private static final FileNodeManager fileNodeManager = FileNodeManager.getInstance();
  private final String JDBC_DRIVER_NAME = "org.apache.iotdb.jdbc.IoTDBDriver";
  private ThreadLocal<String> uuid = new ThreadLocal<String>();
  // String means Storage Group,List means the set of new Files(AbsulutePath) in local IoTDB
  // String means AbsulutePath of new Files
  private ThreadLocal<Map<String, List<String>>> fileNodeMap = new ThreadLocal<>();
  // Map String1 means timeseries String2 means AbsulutePath of new Files, long means startTime
  private ThreadLocal<Map<String, Map<String, Long>>> fileNodeStartTime = new ThreadLocal<>();
  // Map String1 means timeseries String2 means AbsulutePath of new Files, long means endTime
  private ThreadLocal<Map<String, Map<String, Long>>> fileNodeEndTime = new ThreadLocal<>();
  private ThreadLocal<Integer> fileNum = new ThreadLocal<Integer>();
  private ThreadLocal<String> schemaFromSenderPath = new ThreadLocal<String>();
  private IoTDBConfig tsfileDBconfig = IoTDBDescriptor.getInstance().getConfig();
  private String postbackPath;
  // Absolute seriesPath of IoTDB data directory
  private String dataPath = new File(tsfileDBconfig.dataDir).getAbsolutePath() + File.separator;
  // Absolute paths of IoTDB bufferWrite directory
  private String[] bufferWritePaths = tsfileDBconfig.getBufferWriteDirs();
  private IoTDBConfig tsfileDBConfig = IoTDBDescriptor.getInstance().getConfig();

  /**
   * Init threadLocal variable
   */
  @Override
  public void init(String storageGroup) {
    LOGGER.info("IoTDB post back receiver: postback process starts to receive data of storage "
        + "group {}.", storageGroup);
    fileNum.set(0);
    fileNodeMap.set(new HashMap<>());
    fileNodeStartTime.set(new HashMap<>());
    fileNodeEndTime.set(new HashMap<>());
  }

  /**
   * Verify IP address of sender
   */
  @Override
  public boolean getUUID(String uuid, String IPaddress) throws TException {
    this.uuid.set(uuid);
    postbackPath = dataPath + "postback" + File.separator;
    schemaFromSenderPath.set(postbackPath + this.uuid.get() + File.separator + "mlog.txt");
    if (new File(postbackPath + this.uuid.get()).exists()
        && new File(postbackPath + this.uuid.get()).list().length != 0) {
      try {
        PostbackUtils.deleteFile(new File(postbackPath + this.uuid.get()));
      } catch (IOException e) {
        throw new TException(e);
      }
    }
    for (String bufferWritePath : bufferWritePaths) {
      String backupPath = bufferWritePath + "postback" + File.separator;
      if (new File(backupPath + this.uuid.get()).exists()
          && new File(backupPath + this.uuid.get()).list().length != 0) {
        // if does not exist, it means that the last time postback failed, clear uuid
        // data and receive the data again
        try {
          PostbackUtils.deleteFile(new File(backupPath + this.uuid.get()));
        } catch (IOException e) {
          throw new TException(e);
        }
      }
    }
    boolean legalOrNOt = PostbackUtils.verifyIPSegment(tsfileDBConfig.ipWhiteList, IPaddress);
    return legalOrNOt;
  }

  /**
   * Start receiving tsfile from sender
   *
   * @param status status = 0 : finish receiving one tsfile status = 1 : a tsfile has not received
   * completely.
   */
  @Override
  public String startReceiving(String md5OfSender, List<String> filePathSplit,
      ByteBuffer dataToReceive, int status) throws TException {
    String md5OfReceiver = "";
    String filePath = "";
    FileOutputStream fos = null;
    FileChannel channel = null;
    for (int i = 0; i < filePathSplit.size(); i++) {
      if (i == filePathSplit.size() - 1) {
        filePath = filePath + filePathSplit.get(i);
      } else {
        filePath = filePath + filePathSplit.get(i) + File.separator;
      }
    }
    filePath = postbackPath + uuid.get() + File.separator + filePath;
    if (status == 1) { // there are still data stream to add
      File file = new File(filePath);
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          LOGGER.error("IoTDB post back receiver: cannot make file because {}", e.getMessage());
        }
      }
      try {
        fos = new FileOutputStream(file, true); // append new data
        channel = fos.getChannel();
        channel.write(dataToReceive);
        channel.close();
        fos.close();
      } catch (Exception e) {
        LOGGER.error("IoTDB post back receiver: cannot write data to file because {}",
            e.getMessage());
      }
    } else { // all data in the same file has received successfully
      try {
        FileInputStream fis = new FileInputStream(filePath);
        MessageDigest md = MessageDigest.getInstance("MD5");
        int mBufferSize = 8 * 1024 * 1024;
        byte[] buffer = new byte[mBufferSize];
        int n;
        while ((n = fis.read(buffer)) != -1) {
          md.update(buffer, 0, n);
        }
        fis.close();
        md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
        if (md5OfSender.equals(md5OfReceiver)) {
          fileNum.set(fileNum.get() + 1);
          LOGGER.info("IoTDB post back receiver : Receiver has received " + fileNum.get()
              + " "
              + "files from sender!");
        } else {
          new File(filePath).delete();
        }
      } catch (Exception e) {
        LOGGER.error("IoTDB post back receiver: cannot generate md5 because {}", e.getMessage());
      }
    }
    return md5OfReceiver;
  }

  /**
   * Get schema from sender
   *
   * @param status: 0 or 1. status = 0 : finish receiving schema file, start to insert schema to
   * IoTDB through jdbc status = 1 : the schema file has not received completely.
   */
  @Override
  public void getSchema(ByteBuffer schema, int status) throws TException {
    FileOutputStream fos = null;
    FileChannel channel = null;
    if (status == 0) {
      Connection connection = null;
      Statement statement = null;
      try {
        Class.forName(JDBC_DRIVER_NAME);
        connection = DriverManager.getConnection("jdbc:iotdb://localhost:" +
            tsfileDBConfig.rpcPort + "/", "root", "root");
        statement = connection.createStatement();

        BufferedReader bf;
        try {
          bf = new BufferedReader(new java.io.FileReader(schemaFromSenderPath.get()));
          String data;
          statement.clearBatch();
          int count = 0;
          while ((data = bf.readLine()) != null) {
            String item[] = data.split(",");
            if (item[0].equals("2")) {
              String sql = "SET STORAGE GROUP TO " + item[1];
              statement.addBatch(sql);
            } else if (item[0].equals("0")) {
              String sql = "CREATE TIMESERIES " + item[1] + " WITH DATATYPE=" + item[2]
                  + "," + " ENCODING=" + item[3];
              statement.addBatch(sql);
            }
            count++;
            if (count > 10000) {
              statement.executeBatch();
              statement.clearBatch();
              count = 0;
            }
          }
          bf.close();
        } catch (IOException e) {
          LOGGER.error("IoTDB post back receiver: cannot insert schema to IoTDB "
              + "because {}", e.getMessage());
        }

        statement.executeBatch();
        statement.clearBatch();
      } catch (SQLException | ClassNotFoundException e) {
        LOGGER.error("IoTDB post back receiver: jdbc cannot connect to IoTDB because {}",
            e.getMessage());
      } finally {
        try {
          if (statement != null) {
            statement.close();
          }
          if (connection != null) {
            connection.close();
          }
        } catch (SQLException e) {
          LOGGER.error("IoTDB post back receiver : can not close JDBC connection "
              + "because {}", e.getMessage());
        }
      }
    } else {
      File file = new File(schemaFromSenderPath.get());
      if (!file.getParentFile().exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          LOGGER.error("IoTDB post back receiver: cannot make schema file because {}",
              e.getMessage());
        }
      }
      try {
        fos = new FileOutputStream(file, true);
        channel = fos.getChannel();
        channel.write(schema);
        channel.close();
        fos.close();
      } catch (Exception e) {
        LOGGER.error("IoTDB post back receiver: cannot write data to file because {}",
            e.getMessage());
      }
    }
  }

  @Override
  public boolean merge() throws TException {
    getFileNodeInfo();
    mergeData();
    try {
      PostbackUtils.deleteFile(new File(postbackPath + this.uuid.get()));
    } catch (IOException e) {
      throw new TException(e);
    }
    for (String bufferWritePath : bufferWritePaths) {
      String backupPath = bufferWritePath + "postback" + File.separator;
      if (new File(backupPath + this.uuid.get()).exists()
          && new File(backupPath + this.uuid.get()).list().length != 0) {
        // if does not exist, it means that the last time postback process failed, clear
        // uuid data and receive the data again
        try {
          PostbackUtils.deleteFile(new File(backupPath + this.uuid.get()));
        } catch (IOException e) {
          throw new TException(e);
        }
      }
    }
    return true;
  }

  /**
   * Release threadLocal variable resources
   */
  @Override
  public void afterReceiving() {
    uuid.remove();
    fileNum.remove();
    fileNodeMap.remove();
    fileNodeStartTime.remove();
    fileNodeEndTime.remove();
    schemaFromSenderPath.remove();
    LOGGER.info("IoTDB post back receiver: the postBack has finished!");
  }

  /**
   * Get all tsfiles' info which are sent from sender, it is prepare for merging these data
   */
  @Override
  public void getFileNodeInfo() throws TException {
    // String filePath = postbackPath + uuid.get() + File.separator + "data";
    // File root = new File(filePath);
    // File[] files = root.listFiles();
    // int num = 0;
    // for (File storageGroupPB : files) {
    // List<String> filesPath = new ArrayList<>();
    // File[] filesSG = storageGroupPB.listFiles();
    // for (File fileTF : filesSG) { // fileTF means TsFiles
    // Map<String, Long> startTimeMap = new HashMap<>();
    // Map<String, Long> endTimeMap = new HashMap<>();
    // TsRandomAccessLocalFileReader input = null;
    // try {
    // input = new TsRandomAccessLocalFileReader(fileTF.getAbsolutePath());
    // FileReader reader = new FileReader(input);
    // Map<String, TsDevice> deviceIdMap = reader.getFileMetaData().getDeviceMap();
    // Iterator<String> it = deviceIdMap.keySet().iterator();
    // while (it.hasNext()) {
    // String key = it.next().toString(); // key represent device
    // TsDevice deltaObj = deviceIdMap.get(key);
    // startTimeMap.put(key, deltaObj.startTime);
    // endTimeMap.put(key, deltaObj.endTime);
    // }
    // } catch (Exception e) {
    // LOGGER.error("IoTDB post back receiver: unable to read tsfile {} because {}",
    // fileTF.getAbsolutePath(), e.getMessage());
    // } finally {
    // try {
    // input.close();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}",
    // fileTF.getAbsolutePath(), e.getMessage());
    // }
    // }
    // fileNodeStartTime.get().put(fileTF.getAbsolutePath(), startTimeMap);
    // fileNodeEndTime.get().put(fileTF.getAbsolutePath(), endTimeMap);
    // filesPath.add(fileTF.getAbsolutePath());
    // num++;
    // LOGGER.info("IoTDB receiver : Getting FileNode Info has complete : " + num + "/" +
    // fileNum.get());
    // }
    // fileNodeMap.get().put(storageGroupPB.getName(), filesPath);
    // }
  }

  /**
   * Insert all data in the tsfile into IoTDB.
   */
  @Override
  public void mergeOldData(String filePath) throws TException {
    // Set<String> timeseries = new HashSet<>();
    // TsRandomAccessLocalFileReader input = null;
    // Connection connection = null;
    // Statement statement = null;
    // try {
    // Class.forName(JDBC_DRIVER_NAME);
    // connection = DriverManager.getConnection("jdbc:iotdb://localhost:" +
    // tsfileDBConfig.rpcPort + "/", "root",
    // "root");
    // statement = connection.createStatement();
    // int count = 0;
    //
    // input = new TsRandomAccessLocalFileReader(filePath);
    // FileReader reader = new FileReader(input);
    // Map<String, TsdeviceId> deviceIdMap = reader.getFileMetaData().getDeviceMap();
    // Iterator<String> it = deviceIdMap.keySet().iterator();
    // while (it.hasNext()) {
    // String key = it.next().toString(); // key represent devices
    // TsdeviceId deltaObj = deviceIdMap.get(key);
    // TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
    // blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
    // deltaObj.offset,
    // deltaObj.metadataBlockSize));
    // List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
    // timeseries.clear();
    // // firstly, get all timeseries in the same device
    // for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
    // List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
    // .getTimeSeriesChunkMetaDataList();
    // for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
    // TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
    // String measurementUID = properties.getMeasurementUID();
    // measurementUID = key + "." + measurementUID;
    // timeseries.add(measurementUID);
    // }
    // }
    // // secondly, use tsFile Reader to form SQL
    //
    // TsFile readTsFile = new TsFile(input);
    // ArrayList<Path> paths = new ArrayList<>();
    // paths.clear();
    // for (String timesery : timeseries) {
    // paths.add(new Path(timesery));
    // }
    // OnePassQueryDataSet queryDataSet = readTsFile.query(paths, null, null);
    // while (queryDataSet.hasNextRecord()) {
    // OldRowRecord record = queryDataSet.getNextRecord();
    // List<Field> fields = record.getFields();
    // String sql_front = null;
    // for (Field field : fields) {
    // if (field.toString() != "null") {
    // sql_front = "insert into " + field.deviceId + "(timestamp";
    // break;
    // }
    // }
    // String sql_rear = ") values(" + record.timestamp;
    // for (Field field : fields) {
    // if (field.toString() != "null") {
    // sql_front = sql_front + "," + field.measurementId.toString();
    // if (field.dataType == TSDataType.TEXT) {
    // sql_rear = sql_rear + "," + "'" + field.toString() + "'";
    // } else {
    // sql_rear = sql_rear + "," + field.toString();
    // }
    // }
    // }
    // String sql = sql_front + sql_rear + ")";
    //
    // statement.addBatch(sql);
    // count++;
    // if (count > 10000) {
    // statement.executeBatch();
    // statement.clearBatch();
    // count = 0;
    // }
    // }
    // }
    // statement.executeBatch();
    // statement.clearBatch();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver can not parse tsfile into SQL because{}", e.getMessage());
    // } catch (SQLException | ClassNotFoundException e) {
    // LOGGER.error("IoTDB post back receiver: jdbc cannot connect to IoTDB because {}",
    // e.getMessage());
    // } finally {
    // try {
    // input.close();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}", filePath);
    // }
    // try {
    // if (statement != null)
    // statement.close();
    // if (connection != null)
    // connection.close();
    // } catch (SQLException e) {
    // LOGGER.error("IoTDB receiver : Can not close JDBC connection because {}", e.getMessage());
    // }
    // }
  }

  /**
   * Insert those valid data in the tsfile into IoTDB
   *
   * @param overlapFiles:files which are conflict with the postback file
   */
  public void mergeOldData(String filePath, List<String> overlapFiles) throws TException {
    // Set<String> timeseries = new HashSet<>();
    // TsRandomAccessLocalFileReader input = null;
    // Connection connection = null;
    // Statement statement = null;
    // try {
    // Class.forName(JDBC_DRIVER_NAME);
    // connection = DriverManager.getConnection("jdbc:iotdb://localhost:" +
    // tsfileDBConfig.rpcPort + "/", "root",
    // "root");
    // statement = connection.createStatement();
    // int count = 0;
    //
    // input = new TsRandomAccessLocalFileReader(filePath);
    // FileReader reader = new FileReader(input);
    // Map<String, TsdeviceId> deviceIdMap = reader.getFileMetaData().getDeviceMap();
    // Iterator<String> it = deviceIdMap.keySet().iterator();
    // while (it.hasNext()) {
    // String key = it.next().toString(); // key represent devices
    // TsdeviceId deltaObj = deviceIdMap.get(key);
    // TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
    // blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
    // deltaObj.offset,
    // deltaObj.metadataBlockSize));
    // List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
    // timeseries.clear();
    // // firstly, get all timeseries in the same device
    // for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
    // List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
    // .getTimeSeriesChunkMetaDataList();
    // for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
    // TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
    // String measurementUID = properties.getMeasurementUID();
    // measurementUID = key + "." + measurementUID;
    // timeseries.add(measurementUID);
    // }
    // }
    // // secondly, use tsFile Reader to form SQL
    //
    // TsFile readTsFile = new TsFile(input);
    // ArrayList<Path> paths = new ArrayList<>();
    // for (String timesery : timeseries) { // compare data with one timesery in a round to get
    // valid data
    // paths.clear();
    // paths.add(new Path(timesery));
    // Map<String, String> originDataPoint = new HashMap<>();
    // Map<String, String> newDataPoint = new HashMap<>();
    // String sqlFormat = "insert into %s(timestamp,%s) values(%s,%s)";
    // OnePassQueryDataSet queryDataSet = readTsFile.query(paths, null, null);
    // while (queryDataSet.hasNextRecord()) {
    // OldRowRecord record = queryDataSet.getNextRecord();
    // List<Field> fields = record.getFields();
    // String sql;
    // for (Field field : fields) { // get all data with the timesery in the postback file
    // if (field.toString() != "null") {
    // sql = String.format(sqlFormat, field.deviceId, field.measurementId.toString(),
    // record.timestamp, "%s");
    // if (field.dataType == TSDataType.TEXT) {
    // newDataPoint.put(sql, "'" + field.toString() + "'");
    // } else {
    // newDataPoint.put(sql, field.toString());
    // }
    //
    // }
    // }
    // }
    // for (String overlapFile : overlapFiles) // get all data with the timesery in all overlap
    // files.
    // {
    // TsRandomAccessLocalFileReader inputOverlap = null;
    // try {
    // inputOverlap = new TsRandomAccessLocalFileReader(overlapFile);
    // TsFile readTsFileOverlap = new TsFile(inputOverlap);
    // OnePassQueryDataSet queryDataSetOverlap = readTsFileOverlap.query(paths, null, null);
    // while (queryDataSetOverlap.hasNextRecord()) {
    // OldRowRecord recordOverlap = queryDataSetOverlap.getNextRecord();
    // List<Field> fields = recordOverlap.getFields();
    // String sql;
    // for (Field field : fields) {
    // if (field.toString() != "null") {
    // sql = String.format(sqlFormat, field.deviceId,
    // field.measurementId.toString(), recordOverlap.timestamp, "%s");
    // if (field.dataType == TSDataType.TEXT) {
    // originDataPoint.put(sql, "'" + field.toString() + "'");
    // } else {
    // originDataPoint.put(sql, field.toString());
    // }
    // }
    // }
    // }
    // } catch (IOException e) {
    // LOGGER.error("IoTDB post back receiver: unable to read tsfile {} because {}",
    // overlapFile,
    // e.getMessage());
    // } finally {
    // try {
    // inputOverlap.close();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}", overlapFile,
    // e.getMessage());
    // }
    // }
    // }
    // if (originDataPoint.isEmpty()) { // If there has no overlap data with the timesery,
    // inserting all
    // // data in the postback file
    // for (Entry<String, String> entry : newDataPoint.entrySet()) {
    // String sql = String.format(entry.getKey(), entry.getValue());
    // statement.addBatch(sql);
    // count++;
    // if (count > 10000) {
    // statement.executeBatch();
    // statement.clearBatch();
    // count = 0;
    // }
    // }
    // } else { // Compare every data to get valid data
    // for (Entry<String, String> entry : newDataPoint.entrySet()) {
    // if (!originDataPoint.containsKey(entry.getKey())
    // || (originDataPoint.containsKey(entry.getKey())
    // && !originDataPoint.get(entry.getKey()).equals(entry.getValue()))) {
    // String sql = String.format(entry.getKey(), entry.getValue());
    // statement.addBatch(sql);
    // count++;
    // if (count > 10000) {
    // statement.executeBatch();
    // statement.clearBatch();
    // count = 0;
    // }
    // }
    // }
    // }
    // }
    // }
    // statement.executeBatch();
    // statement.clearBatch();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver can not parse tsfile into SQL because{}", e.getMessage());
    // } catch (SQLException | ClassNotFoundException e) {
    // LOGGER.error("IoTDB post back receiver: jdbc cannot connect to IoTDB because {}",
    // e.getMessage());
    // } finally {
    // try {
    // input.close();
    // } catch (IOException e) {
    // LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}", filePath,
    // e.getMessage());
    // }
    // try {
    // if (statement != null)
    // statement.close();
    // if (connection != null)
    // connection.close();
    // } catch (SQLException e) {
    // LOGGER.error("IoTDB receiver : Can not close JDBC connection because {}", e.getMessage());
    // }
    // }
  }

  /**
   * It is to merge data. If data in the tsfile is new, append the tsfile to the storage group
   * directly. If data in the tsfile is old, it has two strategy to merge.It depends on the
   * possibility of updating historical data.
   */
  @Override
  public void mergeData() throws TException {
    int num = 0;
    for (String storageGroup : fileNodeMap.get().keySet()) {
      List<String> filesPath = fileNodeMap.get().get(storageGroup);
      // before load extern tsFile, it is necessary to order files in the same SG
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
        String header = postbackPath + uuid.get() + File.separator + "data" + File.separator;
        String relativePath = path.substring(header.length());
        IntervalFileNode fileNode = new IntervalFileNode(startTimeMap, endTimeMap,
            OverflowChangeType.NO_CHANGE,
            Directories.getInstance().getNextFolderIndexForTsFile(), relativePath);
        // call interface of load external file
        try {
          if (!fileNodeManager.appendFileToFileNode(storageGroup, fileNode, path)) {
            // it is a file with overflow data
            if (tsfileDBConfig.update_historical_data_possibility) {
              mergeOldData(path);
            } else {
              List<String> overlapFiles = fileNodeManager.getOverlapFilesFromFileNode(
                  storageGroup,
                  fileNode, uuid.get());
              if (overlapFiles.size() == 0) {
                mergeOldData(path);
              } else {
                mergeOldData(path, overlapFiles);
              }
            }
          }
        } catch (FileNodeManagerException e) {
          LOGGER.error("IoTDB receiver : Can not load external file because {}",
              e.getMessage());
        }

        num++;
        LOGGER.info("IoTDB receiver : Merging files has completed : " + num + "/" + fileNum.get());
      }
    }
  }

  public Map<String, List<String>> getFileNodeMap() {
    return fileNodeMap.get();
  }

  public void setFileNodeMap(Map<String, List<String>> fileNodeMap) {
    this.fileNodeMap.set(fileNodeMap);
  }
}