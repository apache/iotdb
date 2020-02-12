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
package org.apache.iotdb.db.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.RootNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 */
public class MManager {

  private static final Logger logger = LoggerFactory.getLogger(MManager.class);
  private static final String PATH_SEPARATOR = "\\.";
  private static final String ROOT_NAME = MetadataConstant.ROOT;
  private static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // the log file seriesPath
  private String logFilePath;
  private MTree mtree;
  private BufferedWriter logWriter;
  private boolean writeToLog;
  private String schemaDir;
  private RandomDeleteCache<String, MNode> mNodeCache;

  private Map<String, Integer> seriesNumberInStorageGroups = new HashMap<>();
  private long maxSeriesNumberAmongStorageGroup;
  private boolean initialized;
  private IoTDBConfig dbconfig;

  private MManager() {
    dbconfig = IoTDBDescriptor.getInstance().getConfig();
    schemaDir = dbconfig.getSchemaDir();
    File systemFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!systemFolder.exists()) {
      if (systemFolder.mkdirs()) {
        logger.info("create system folder {}", systemFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", systemFolder.getAbsolutePath());
      }
    }
    logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    writeToLog = false;

    int cacheSize = dbconfig.getmManagerCacheSize();
    mNodeCache = new RandomDeleteCache<String, MNode>(cacheSize) {
      @Override
      public void beforeRemove(MNode object) {
        //allowed to do nothing
      }

      @Override
      public MNode loadObjectByKey(String key) throws CacheException {
        try {
          return getNodeByPathWithCheck(key);
        } catch (MetadataException e) {
          throw new CacheException(e);
        }
      }
    };
  }

  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
  }

  //Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  public void init() {
    if (initialized) {
      return;
    }
    lock.writeLock().lock();
    File logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);

    try {
      initFromLog(logFile);

      // storage group name -> the series number
      seriesNumberInStorageGroups = new HashMap<>();
      List<String> storageGroups = mtree.getAllStorageGroupNames();
      for (String sg : storageGroups) {
        MNode node = mtree.getNodeByPath(sg);
        seriesNumberInStorageGroups.put(sg, node.getLeafCount());
      }

      if (seriesNumberInStorageGroups.isEmpty()) {
        maxSeriesNumberAmongStorageGroup = 0;
      } else {
        maxSeriesNumberAmongStorageGroup = seriesNumberInStorageGroups.values().stream()
            .max(Integer::compareTo).get();
      }
      writeToLog = true;
    } catch (IOException | MetadataException e) {
      mtree = new MTree(ROOT_NAME);
      logger.error("Cannot read MTree from file, using an empty new one", e);
    } finally {
      lock.writeLock().unlock();
    }
    initialized = true;
  }

  private void initFromLog(File logFile) throws IOException, MetadataException {
    // init the metadata from the operation log
    mtree = new MTree(ROOT_NAME);
    if (logFile.exists()) {
      try (FileReader fr = new FileReader(logFile);
          BufferedReader br = new BufferedReader(fr)) {
        String cmd;
        while ((cmd = br.readLine()) != null) {
          operation(cmd);
        }
      }
    }
  }

  /**
   * function for clearing MTree
   */
  public void clear() {
    lock.writeLock().lock();
    try {
      this.mtree = new MTree(ROOT_NAME);
      this.mNodeCache.clear();
      this.seriesNumberInStorageGroups.clear();
      this.maxSeriesNumberAmongStorageGroup = 0;
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void operation(String cmd) throws IOException, MetadataException {
    //see addPathToMTree() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",");
    switch (args[0]) {
      case MetadataOperationType.ADD_PATH_TO_MTREE:
        Map<String, String> props = null;
        if (args.length > 5) {
          String[] kv;
          props = new HashMap<>(args.length - 5 + 1, 1);
          for (int k = 5; k < args.length; k++) {
            kv = args[k].split("=");
            props.put(kv[0], kv[1]);
          }
        }

        addPathToMTree(args[1], TSDataType.deserialize(Short.parseShort(args[2])),
            TSEncoding.deserialize(Short.parseShort(args[3])),
            CompressionType.deserialize(Short.parseShort(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        deletePaths(Collections.singletonList(args[1]), false);
        break;
      case MetadataOperationType.SET_STORAGE_GROUP_TO_MTREE:
        setStorageGroupToMTree(args[1]);
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP_FROM_MTREE:
        List<String> storageGroups = new ArrayList<>();
        storageGroups.addAll(Arrays.asList(args).subList(1, args.length));
        deleteStorageGroupsFromMTree(storageGroups);
        break;
      case MetadataOperationType.SET_TTL:
        setTTL(args[1], Long.parseLong(args[2]));
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  private BufferedWriter getLogWriter() throws IOException {
    if (logWriter == null) {
      File logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
      File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
      if (!metadataDir.exists()) {
        if (metadataDir.mkdirs()) {
          logger.info("create schema folder {}.", metadataDir);
        } else {
          logger.info("create schema folder {} failed.", metadataDir);
        }
      }
      FileWriter fileWriter;
      fileWriter = new FileWriter(logFile, true);
      logWriter = new BufferedWriter(fileWriter);
    }
    return logWriter;
  }

  /**
   * Add one timeseries to metadata tree
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   * @return whether the measurement occurs for the first time in this storage group (if true, the
   * measurement should be registered to the StorageEngine too)
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  public boolean addPathToMTree(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (isPathExist(path)) {
        throw new PathAlreadyExistException(path);
      }
      if (!checkStorageGroupByPath(path)) {
        if (!dbconfig.isAutoCreateSchemaEnabled()) {
          throw new MetadataException("Storage group should be created first");
        }
        String storageGroupName = getStorageGroupNameByAutoLevel(path,
            dbconfig.getDefaultStorageGroupLevel());
        setStorageGroupToMTree(storageGroupName);
      }
      // optimize the speed of adding timeseries
      String fileNodePath = getStorageGroupNameByPath(path);
      // the two map is stored in the storage group node
      Map<String, MeasurementSchema> schemaMap = getStorageGroupSchemaMap(fileNodePath);
      Map<String, Integer> numSchemaMap = getStorageGroupNumSchemaMap(fileNodePath);
      String lastNode = new Path(path).getMeasurement();
      boolean isNewMeasurement = true;
      // Thread safety: just one thread can access/modify the schemaMap
      synchronized (schemaMap) {
        // Need to check the path again to avoid duplicated inserting by multi concurrent threads
        if (isPathExist(path)) {
          throw new PathAlreadyExistException(path);
        }
        if (schemaMap.containsKey(lastNode)) {
          isNewMeasurement = false;
          MeasurementSchema columnSchema = schemaMap.get(lastNode);
          if (!columnSchema.getType().equals(dataType)
              || !columnSchema.getEncodingType().equals(encoding)
              || !columnSchema.getCompressor().equals(compressor)) {
            throw new MetadataException(String.format(
                "The resultDataType or encoding or compression of the last node %s is conflicting "
                    + "in the storage group %s", lastNode, fileNodePath));
          }
          try {
            addPathToMTreeInternal(path, dataType, encoding, compressor, props);
          } catch (IOException e) {
            throw new MetadataException(e.getMessage());
          }
          numSchemaMap.put(lastNode, numSchemaMap.get(lastNode) + 1);
        } else {
          try {
            addPathToMTreeInternal(path, dataType, encoding, compressor, props);
          } catch (IOException e) {
            throw new MetadataException(e.getMessage());
          }
          MeasurementSchema columnSchema;
          columnSchema = getSchemaForOnePath(path);
          schemaMap.put(lastNode, columnSchema);
          numSchemaMap.put(lastNode, 1);
        }
        try {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
        } catch (ConfigAdjusterException e) {
          // Undo create time series
          deletePaths(Collections.singletonList(path), true);
          throw new MetadataException(e);
        }
        return isNewMeasurement;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Add one timeseries to metadata tree. TEST ONLY
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   */
  @TestOnly
  public void addPathToMTree(String path, String dataType, String encoding)
      throws MetadataException, IOException {
    lock.writeLock().lock();
    try {
      TSDataType tsDataType = TSDataType.valueOf(dataType);
      TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
      CompressionType type = CompressionType
          .valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
      addPathToMTreeInternal(path, tsDataType, tsEncoding, type, Collections.emptyMap());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * path will be added to MTree with no check
   */
  private void addPathToMTreeInternal(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException, IOException {
    // Add a seriesPath to Metadata Tree
    // path Format: root.node.(node)*
    String[] nodes = MetaUtils.getNodeNames(path, PATH_SEPARATOR);
    if (nodes.length == 0) {
      throw new IllegalPathException(path);
    }
    mtree.addPath(path, dataType, encoding, compressor, props);

    // Get the file name for given seriesPath Notice: This method could be called if and only if the
    // seriesPath includes one node whose {@code isStorageGroup} is true.
    String storageGroupName = getStorageGroupNameByPath(path);
    int size = seriesNumberInStorageGroups.get(storageGroupName);
    seriesNumberInStorageGroups.put(storageGroupName, size + 1);
    if (size + 1 > maxSeriesNumberAmongStorageGroup) {
      maxSeriesNumberAmongStorageGroup = size + 1;
    }
    if (writeToLog) {
      BufferedWriter writer = getLogWriter();
      writer.write(String.format("%s,%s,%s,%s,%s", MetadataOperationType.ADD_PATH_TO_MTREE,
          path, dataType.serialize(), encoding.serialize(), compressor.serialize()));
      if (props != null) {
        for (Map.Entry entry : props.entrySet()) {
          writer.write(String.format(",%s=%s", entry.getKey(), entry.getValue()));
        }
      }
      writer.newLine();
      writer.flush();
    }
  }

  private List<String> collectPaths(List<String> paths) throws MetadataException {
    Set<String> pathSet = new HashSet<>();
    // Attention: Monitor storage group seriesPath is not allowed to be deleted
    for (String p : paths) {
      List<String> subPaths = getPaths(p);
      if (subPaths.isEmpty()) {
        throw new MetadataException(
            String.format("There are no timeseries in the prefix of %s seriesPath", p));
      }
      List<String> newSubPaths = new ArrayList<>();
      for (String eachSubPath : subPaths) {
        String storageGroupName;
        storageGroupName = getStorageGroupNameByPath(eachSubPath);

        if (MonitorConstants.STAT_STORAGE_GROUP_PREFIX.equals(storageGroupName)) {
          continue;
        }
        newSubPaths.add(eachSubPath);
      }
      pathSet.addAll(newSubPaths);
    }
    for (String p : pathSet) {
      if (!isPathExist(p)) {
        throw new MetadataException(String.format(
            "Timeseries %s does not exist and cannot be deleted", p));
      }
    }
    return new ArrayList<>(pathSet);
  }

  /**
   * delete given paths from metadata tree
   *
   * @param deletePathList list of paths to be deleted
   * @return a set contains StorageGroups that contain no more timeseries after this deletion and
   * files of such StorageGroups should be deleted to reclaim disk space.
   */
  public Set<String> deletePaths(List<String> deletePathList, boolean isUndo)
      throws MetadataException {
    if (deletePathList != null && !deletePathList.isEmpty()) {
      List<String> fullPath = collectPaths(deletePathList);

      Set<String> emptyStorageGroups = new HashSet<>();
      for (String p : fullPath) {
        if (!isUndo) {
          try {
            IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(-1);
          } catch (ConfigAdjusterException e) {
            throw new MetadataException(e);
          }
        }
        String emptiedStorageGroup = deletePath(p);
        if (emptiedStorageGroup != null) {
          emptyStorageGroups.add(emptiedStorageGroup);
        }
      }
      return emptyStorageGroups;
    }
    return Collections.emptySet();
  }

  private String deletePath(String pathStr) throws MetadataException {
    String storageGroupName;
    storageGroupName = getStorageGroupNameByPath(pathStr);

    String emptiedStorageGroup;
    // the two maps are stored in the storage group node
    Map<String, MeasurementSchema> schemaMap = getStorageGroupSchemaMap(storageGroupName);
    Map<String, Integer> numSchemaMap = getStorageGroupNumSchemaMap(storageGroupName);
    // Thread safety: just one thread can access/modify the schemaMap
    synchronized (schemaMap) {
      // TODO: don't delete the storage group seriesPath recursively
      String measurementId = new Path(pathStr).getMeasurement();
      if (numSchemaMap.get(measurementId) == 1) {
        numSchemaMap.remove(measurementId);
        schemaMap.remove(measurementId);
      } else {
        numSchemaMap.put(measurementId, numSchemaMap.get(measurementId) - 1);
      }
      try {
        emptiedStorageGroup = deletePathFromMTree(pathStr);
      } catch (MetadataException | IOException e) {
        throw new MetadataException(e.getMessage());
      }
    }
    return emptiedStorageGroup;
  }

  /**
   * function for deleting a given path from mTree.
   *
   * @return the related storage group name if there is no path in the storage group anymore;
   * otherwise null
   */
  private String deletePathFromMTree(String path) throws MetadataException, IOException {
    lock.writeLock().lock();
    try {
      mNodeCache.clear();
      String storageGroupName = deletePathInMTree(path);
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.DELETE_PATH_FROM_MTREE + "," + path);
        writer.newLine();
        writer.flush();
      }
      String storageGroup = getStorageGroupNameByPath(path);
      int size = seriesNumberInStorageGroups.get(storageGroup);
      seriesNumberInStorageGroups.put(storageGroup, size - 1);
      if (size == maxSeriesNumberAmongStorageGroup) {
        //recalculate
        if (seriesNumberInStorageGroups.isEmpty()) {
          maxSeriesNumberAmongStorageGroup = 0;
        } else {
          maxSeriesNumberAmongStorageGroup = seriesNumberInStorageGroups.values().stream()
              .max(Integer::compareTo).get();
        }
      } else {
        maxSeriesNumberAmongStorageGroup--;
      }
      return storageGroupName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for setting storage group of the given path to mTree.
   *
   * @param path Format: root.node.(node)*
   */
  public void setStorageGroupToMTree(String path) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (mtree.checkStorageGroup(path)) {
        return;
      }

      mtree.setStorageGroup(path);
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
      ActiveTimeSeriesCounter.getInstance().init(path);
      seriesNumberInStorageGroups.put(path, 0);
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.SET_STORAGE_GROUP_TO_MTREE + "," + path);
        writer.newLine();
        writer.flush();
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    } catch (ConfigAdjusterException e) {
      try {
        // path Format: root.node
        mtree.deleteStorageGroup(path);
        throw new MetadataException(e);
      } catch (MetadataException ex) {
        throw new MetadataException(ex);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for deleting storage groups of the given path from mTree. the log format is like
   * "delete_storage_group,sg1,sg2,sg3"
   */
  public void deleteStorageGroupsFromMTree(List<String> deletePathList) throws MetadataException {
    List<String> pathList = new ArrayList<>();
    StringBuilder jointPath = new StringBuilder();
    for (String storagePath : deletePathList) {
      pathList.add(storagePath);
      jointPath.append(",").append(storagePath);
    }
    lock.writeLock().lock();
    try {
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.DELETE_STORAGE_GROUP_FROM_MTREE + jointPath);
        writer.newLine();
        writer.flush();
      }
      for (String delStorageGroup : pathList) {
        try {
          mNodeCache.clear();
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(-1);

          // path Format: root.node
          mtree.deleteStorageGroup(delStorageGroup);
          IoTDBConfigDynamicAdapter.getInstance()
              .addOrDeleteTimeSeries(seriesNumberInStorageGroups.remove(delStorageGroup) * (-1));
          ActiveTimeSeriesCounter.getInstance().delete(delStorageGroup);
        } catch (MetadataException e) {
          try {
            IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
          } catch (ConfigAdjusterException ex) {
            throw new MetadataException(ex);
          }
          throw new MetadataException(e);
        }
      }
    } catch (ConfigAdjusterException e) {
      throw new MetadataException(e);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for checking if the given path is storage group of mTree or not.
   *
   * @apiNote :for cluster
   */
  boolean checkStorageGroupOfMTree(String path) {
    lock.readLock().lock();
    try {
      // param path Format: root.node.(node)*
      return mtree.checkStorageGroup(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Delete seriesPath in current MTree.
   *
   * @param path a seriesPath belongs to MTree
   */
  private String deletePathInMTree(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path, PATH_SEPARATOR);
    if (nodes.length == 0) {
      throw new IllegalPathException(path);
    }
    String rootName = nodes[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.deletePath(path);
    } else {
      throw new RootNotExistException(rootName);
    }
  }

  /**
   * Get series type for given seriesPath.
   *
   * @return TSDataType
   */
  public TSDataType getSeriesType(String fullPath) throws MetadataException {
    lock.readLock().lock();
    try {
      if (fullPath.equals(SQLConstant.RESERVED_TIME)) {
        return TSDataType.INT64;
      }
      return getSchemaForOnePath(fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting series type.
   */
  TSDataType getSeriesType(MNode node, String fullPath) throws MetadataException {
    lock.readLock().lock();
    try {
      return getSchemaForOnePath(node, fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the full Metadata info.
   *
   * @return A {@code Metadata} instance which stores all metadata info
   */
  public Metadata getMetadata() throws MetadataException {
    lock.readLock().lock();
    try {
      Map<String, List<String>> deviceIdMap = new HashMap<>();
      ArrayList<String> types = mtree.getAllType();
      for (String type : types) {
        deviceIdMap.put(type, mtree.getDeviceForOneType(type));
      }
      return new Metadata(deviceIdMap);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get devices info with given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   * @return A HashSet instance which stores devices names with given prefixPath.
   */
  public List<String> getDevices(String prefixPath) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getDevices(prefixPath);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all nodes from the given level
   *
   * @param prefixPath can be a prefix of a full path. Can not be a full path. can not have
   * wildcard. But, the level of the prefixPath can be smaller than the given level, e.g.,
   * prefixPath = root.a while the given level is 5
   * @param nodeLevel the level can not be smaller than the level of the prefixPath
   * @return A List instance which stores all node at given level
   */
  public List<String> getNodesList(String prefixPath, int nodeLevel) throws SQLException {
    lock.readLock().lock();
    try {
      return mtree.getNodesList(prefixPath, nodeLevel);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all ColumnSchemas for the storage group seriesPath.
   *
   * @param path the Path in a storage group
   * @return ArrayList<'   ColumnSchema   '> The list of the schema
   */
  public List<MeasurementSchema> getSchemaForStorageGroup(String path) {
    lock.readLock().lock();
    try {
      return mtree.getSchemaForOneStorageGroup(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting schema map for one file node.
   */
  private Map<String, MeasurementSchema> getStorageGroupSchemaMap(String path) {
    lock.readLock().lock();
    try {
      return mtree.getSchemaMapForOneStorageGroup(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting num schema map for one file node.
   */
  private Map<String, Integer> getStorageGroupNumSchemaMap(String path) {
    lock.readLock().lock();
    try {
      return mtree.getNumSchemaMapForOneFileNode(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the file name for given seriesPath Notice: This method could be called if and only if the
   * seriesPath includes one node whose {@code isStorageGroup} is true.
   *
   * @param path a prefix of a fullpath. The prefix should contains the name of a storage group. DO
   * NOT SUPPORT WILDCARD.
   * @return A String represented the file name
   */
  public String getStorageGroupNameByPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      // Get the file name for given seriesPath Notice: This method could be called if and only if the
      // seriesPath includes one node whose {@code isStorageGroup} is true.
      return mtree.getStorageGroupNameByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking storage group name by path.
   */
  boolean checkStorageGroupByPath(String path) {
    lock.readLock().lock();
    try {
      return mtree.checkFileNameByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all storage group names
   *
   * @return A list which stores all storage group names.
   */
  public List<String> getAllStorageGroupNames() {
    lock.readLock().lock();
    try {
      return mtree.getAllStorageGroupNames();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all storage group MNodes
   */
  public List<MNode> getAllStorageGroupNodes() {
    lock.readLock().lock();
    try {
      return mtree.getAllStorageGroupNodes();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all storage group names for given seriesPath
   *
   * @return List of String represented all storage group names
   */
  List<String> getAllStorageGroupNamesByPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getAllFileNamesByPath(path);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all paths for given seriesPath if the seriesPath is abstract. Or return the seriesPath
   * itself.
   *
   * @param path can be a prefix or a full path. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<String> getPaths(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      // Get all paths for given seriesPath regular expression if given seriesPath belongs to MTree, or
      // get all linked seriesPath for given seriesPath if given seriesPath belongs to PTree Notice:
      // Regular expression in this method is formed by the amalgamation of seriesPath and the character '*'.
      // return A HashMap whose Keys are separated by the storage file name.
      String rootName = MetaUtils.getNodeNames(path, PATH_SEPARATOR)[0];
      if (mtree.getRoot().getName().equals(rootName)) {
        List<String> res = new ArrayList<>();
        Map<String, List<String>> pathsGroupBySG = mtree.getAllPath(path);
        for (List<String> ps : pathsGroupBySG.values()) {
          res.addAll(ps);
        }
        return res;
      }
      throw new RootNotExistException(rootName);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   *
   * @param path can be root, root.*  root.*.*.a etc.. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   * @return for each storage group, return a List which size =5 (name, sg name, data type,
   * encoding, and compressor). TODO the structure needs to optimize
   */
  public List<List<String>> getShowTimeseriesPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      String rootName = MetaUtils.getNodeNames(path, PATH_SEPARATOR)[0];
      if (mtree.getRoot().getName().equals(rootName)) {
        return mtree.getShowTimeseriesPath(path);
      }
      throw new RootNotExistException(rootName);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get leaf node path in the next level of given seriesPath.
   *
   * @param path a prefix of a full path which has a storage group name. do not support wildcard.
   * can not be a full path.
   */
  List<String> getLeafNodePathInNextLevel(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getLeafNodePathInNextLevel(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get leaf node path in the next level of given seriesPath.
   *
   * @param path do not accept wildcard. can not be a full path.
   */
  public Set<String> getChildNodePathInNextLevel(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getChildNodePathInNextLevel(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check whether the seriesPath given exists.
   */
  public boolean isPathExist(String path) {
    lock.readLock().lock();
    try {
      return mtree.isPathExist(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking whether the path exists.
   */
  boolean isPathExist(MNode node, String path) {
    lock.readLock().lock();
    try {
      return mtree.isPathExist(node, path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * get node by path.
   */
  MNode getNodeByPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getNodeByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * get node by path with check.
   */
  public MNode getNodeByPathWithCheck(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getNodeByPathWithStorageGroupCheck(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting node by path from cache.
   */
  public MNode getNodeByPathFromCache(String path) throws MetadataException {
    return getNodeByPathFromCache(path, dbconfig.isAutoCreateSchemaEnabled(),
        dbconfig.getDefaultStorageGroupLevel());
  }

  /**
   * function for getting node by deviceId from cache.
   */
  public MNode getNodeByPathFromCache(String path, boolean autoCreateSchema, int sgLevel)
      throws MetadataException {
    lock.readLock().lock();
    MNode node = null;
    boolean createSchema = false;
    boolean setStorageGroup = false;
    try {
      node = mNodeCache.get(path);
    } catch (CacheException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path);
      } else {
        createSchema = true;
        setStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
      }
    } finally {
      lock.readLock().unlock();
      if (createSchema) {
        if (setStorageGroup) {
          String storageGroupName = getStorageGroupNameByAutoLevel(path, sgLevel);
          setStorageGroupToMTree(storageGroupName);
        }
        // Add deviceId to MTree. This is available IF and ONLY IF creating schema automatically is enabled
        node = mtree.addDeviceId(path);
      }
    }
    return node;
  }

  /**
   * Get schema for one path
   */
  private MeasurementSchema getSchemaForOnePath(MNode node, String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getSchemaForOnePath(node, path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get schema for one path
   */
  private MeasurementSchema getSchemaForOnePath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getSchemaForOnePath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check whether given seriesPath contains a MNode whose {@code MNode.isStorageGroup} is true
   */
  boolean checkFilesLevel(List<String> path) throws MetadataException {
    lock.readLock().lock();
    try {
      for (String p : path) {
        getStorageGroupNameByPath(p);
      }
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check file level
   */
  boolean checkFilesLevel(MNode node, List<String> path) throws MetadataException {
    lock.readLock().lock();
    try {
      for (String p : path) {
        mtree.getStorageGroupNameByPath(node, p);
      }
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get metadata in string
   */
  public String getMetadataInString() {
    lock.readLock().lock();
    try {
      return TIME_SERIES_TREE_HEADER + mtree.toString();
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getSeriesNumber(String storageGroup) {
    return seriesNumberInStorageGroups.getOrDefault(storageGroup, 0);
  }

  /**
   * function for getting storage group name when creating schema automatically is enable
   */
  String getStorageGroupNameByAutoLevel(String fullPath, int level) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(fullPath, PATH_SEPARATOR);
    StringBuilder storageGroupName = new StringBuilder(nodeNames[0]);
    if (nodeNames.length < level || !storageGroupName.toString().equals(ROOT_NAME)) {
      throw new IllegalPathException(fullPath);
    }
    for (int i = 1; i < level; i++) {
      storageGroupName.append(IoTDBConstant.PATH_SEPARATOR).append(nodeNames[i]);
    }
    return storageGroupName.toString();
  }

  @TestOnly
  public void setMaxSeriesNumberAmongStorageGroup(long maxSeriesNumberAmongStorageGroup) {
    this.maxSeriesNumberAmongStorageGroup = maxSeriesNumberAmongStorageGroup;
  }

  public long getMaximalSeriesNumberAmongStorageGroups() {
    return maxSeriesNumberAmongStorageGroup;
  }

  private static class MManagerHolder {

    private MManagerHolder() {
      //allowed to do nothing
    }

    private static final MManager INSTANCE = new MManager();
  }

  public void setTTL(String storageGroup, long dataTTL) throws MetadataException, IOException {
    lock.writeLock().lock();
    try {
      MNode sgNode = getNodeByPath(storageGroup);
      if (!sgNode.isStorageGroup()) {
        throw new StorageGroupNotSetException(storageGroup);
      }
      sgNode.setDataTTL(dataTTL);
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer
            .write(String.format("%s,%s,%s", MetadataOperationType.SET_TTL, storageGroup, dataTTL));
        writer.newLine();
        writer.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
