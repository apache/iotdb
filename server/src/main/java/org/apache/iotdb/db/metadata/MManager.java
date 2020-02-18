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
import org.apache.iotdb.db.exception.metadata.IncorrectRootException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
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
          return getNodeByPathWithStorageGroupCheck(key);
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
    //see addPath() to get the detailed format of the cmd
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

        addPath(args[1], TSDataType.deserialize(Short.parseShort(args[2])),
            TSEncoding.deserialize(Short.parseShort(args[3])),
            CompressionType.deserialize(Short.parseShort(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        deletePath(args[1], false);
        break;
      case MetadataOperationType.SET_STORAGE_GROUP_TO_MTREE:
        setStorageGroup(args[1]);
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP_FROM_MTREE:
        List<String> storageGroups = new ArrayList<>();
        storageGroups.addAll(Arrays.asList(args).subList(1, args.length));
        deleteStorageGroups(storageGroups);
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
  public boolean addPath(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (isPathExist(path)) {
        throw new PathAlreadyExistException(path);
      }
      if (!checkStorageGroup(path)) {
        if (!dbconfig.isAutoCreateSchemaEnabled()) {
          throw new MetadataException("Storage group should be created first");
        }
        String storageGroupName = getStorageGroupNameByAutoLevel(path,
            dbconfig.getDefaultStorageGroupLevel());
        setStorageGroup(storageGroupName);
      }
      // optimize the speed of adding timeseries
      String storageGroupName = getStorageGroupName(path);
      // the two map is stored in the storage group node
      Map<String, MeasurementSchema> schemaMap = getStorageGroupSchemaMap(storageGroupName);
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
                    + "in the storage group %s", lastNode, storageGroupName));
          }
          try {
            addPathToMTreeInternal(path, dataType, encoding, compressor, props);
          } catch (IOException e) {
            throw new MetadataException(e.getMessage());
          }
        } else {
          try {
            addPathToMTreeInternal(path, dataType, encoding, compressor, props);
          } catch (IOException e) {
            throw new MetadataException(e.getMessage());
          }
          schemaMap.put(lastNode, mtree.getSchema(path));
        }
        try {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
        } catch (ConfigAdjusterException e) {
          // Undo create time series
          deletePath(path, true);
          throw new MetadataException(e);
        }
        return isNewMeasurement;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean addPath(String path, TSDataType dataType, TSEncoding encoding)
      throws MetadataException {
    CompressionType compressionType =
        CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
    return addPath(path, dataType, encoding, compressionType, Collections.emptyMap());
  }

  @TestOnly
  public void addPath(String path, String dataType, String encoding)
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
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0) {
      throw new IllegalPathException(path);
    }
    mtree.addPath(path, dataType, encoding, compressor, props);

    // Get the storage group name for given seriesPath Notice: This method could be called if and only if the
    // seriesPath includes one node whose {@code isStorageGroup} is true.
    String storageGroupName = getStorageGroupName(path);
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

  private List<String> collectPath(String p) throws MetadataException {
    // Attention: Monitor storage group seriesPath is not allowed to be deleted
    List<String> subPaths = getPaths(p);
    if (subPaths.isEmpty()) {
      throw new PathNotExistException(p);
    }
    List<String> newSubPaths = new ArrayList<>();
    for (String subPath : subPaths) {
      String storageGroupName;
      storageGroupName = getStorageGroupName(subPath);

      if (MonitorConstants.STAT_STORAGE_GROUP_PREFIX.equals(storageGroupName)) {
        continue;
      }
      if (!isPathExist(subPath)) {
        throw new PathNotExistException(subPath);
      }
      newSubPaths.add(subPath);
    }
    return new ArrayList<>(newSubPaths);
  }

  /**
   * Delete given paths from MTree
   *
   * @param path path to be deleted
   * @return a set contains StorageGroups that contain no more timeseries after this deletion and
   * files of such StorageGroups should be deleted to reclaim disk space.
   */
  public Set<String> deletePath(String path, boolean isUndo) throws MetadataException {
    Set<String> emptyStorageGroups = new HashSet<>();
    for (String p : collectPath(path)) {
      if (!isUndo) {
        try {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(-1);
        } catch (ConfigAdjusterException e) {
          throw new MetadataException(e);
        }
      }
      String storageGroupName = getStorageGroupName(p);
      String emptyStorageGroup;
      // the two maps are stored in the storage group node
      Map<String, MeasurementSchema> schemaMap = getStorageGroupSchemaMap(storageGroupName);
      // Thread safety: just one thread can access/modify the schemaMap
      synchronized (schemaMap) {
        try {
          emptyStorageGroup = deletePathFromMTree(p);
        } catch (IOException e) {
          throw new MetadataException(e.getMessage());
        }
      }
      if (emptyStorageGroup != null) {
        emptyStorageGroups.add(emptyStorageGroup);
      }
    }
    return emptyStorageGroups;
  }

  /**
   * Delete a given path from MTree.
   *
   * @return storage group name if there is no path in the storage group anymore; otherwise null
   */
  private String deletePathFromMTree(String path) throws MetadataException, IOException {
    lock.writeLock().lock();
    try {
      mNodeCache.clear();
      String storageGroupName;
      String[] nodes = MetaUtils.getNodeNames(path);
      if (nodes.length == 0) {
        throw new IllegalPathException(path);
      }
      String rootName = nodes[0];
      if (mtree.getRoot().equals(rootName)) {
        storageGroupName = mtree.deletePath(path);
      } else {
        throw new IncorrectRootException(rootName);
      }
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.DELETE_PATH_FROM_MTREE + "," + path);
        writer.newLine();
        writer.flush();
      }
      String storageGroup = getStorageGroupName(path);
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
   * Set storage group of the given path to MTree.
   *
   * @param path root.node.(node)*
   */
  public void setStorageGroup(String path) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (mtree.isStorageGroup(path)) {
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
      mtree.deleteStorageGroup(path);
      throw new MetadataException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Delete storage groups of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3"
   *
   * @param paths list of paths to be deleted. Format: root.node
   */
  public void deleteStorageGroups(List<String> paths) throws MetadataException {
    List<String> pathList = new ArrayList<>();
    StringBuilder jointPath = new StringBuilder();
    for (String storagePath : paths) {
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
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  boolean isStorageGroup(String path) {
    lock.readLock().lock();
    try {
      return mtree.isStorageGroup(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath path
   */
  public TSDataType getSeriesType(String fullPath) throws MetadataException {
    lock.readLock().lock();
    try {
      if (fullPath.equals(SQLConstant.RESERVED_TIME)) {
        return TSDataType.INT64;
      }
      return mtree.getSchema(fullPath).getType();
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
      return new Metadata(mtree.getDeviceIdMap());
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
  public List<String> getNodesList(String prefixPath, int nodeLevel) throws MetadataException {
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
   */
  public List<MeasurementSchema> getStorageGroupSchema(String path) {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupSchema(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get schema map for storage group
   */
  private Map<String, MeasurementSchema> getStorageGroupSchemaMap(String path) {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupSchemaMap(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the storage group name for given path. Notice: This method could be called if and only if
   * the seriesPath includes one node whose {@code isStorageGroup} is true.
   *
   * @param path a prefix of a fullpath. The prefix should contains the name of a storage group. DO
   * NOT SUPPORT WILDCARD.
   * @return A String represented the storage group name
   */
  public String getStorageGroupName(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupName(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check storage group name by path
   */
  boolean checkStorageGroup(String path) {
    lock.readLock().lock();
    try {
      return mtree.checkStorageGroupByPath(path);
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
      return mtree.getStorageGroupByPath(path);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all paths for given path if the path is abstract. Or return the path itself. Regular
   * expression in this method is formed by the amalgamation of seriesPath and the character '*'.
   *
   * @param path can be a prefix or a full path. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<String> getPaths(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      String rootName = MetaUtils.getNodeNames(path)[0];
      if (mtree.getRoot().equals(rootName)) {
        return mtree.getAllPath(path);
      }
      throw new IncorrectRootException(rootName);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all timeseries paths under the given path.
   *
   * @param path can be root, root.*  root.*.*.a etc.. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   * @return for each storage group, return a List which size =5 (name, sg name, data type,
   * encoding, and compressor). TODO the structure needs to optimize
   */
  public List<List<String>> getShowTimeseriesPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      String rootName = MetaUtils.getNodeNames(path)[0];
      if (mtree.getRoot().equals(rootName)) {
        return mtree.getShowTimeseriesPath(path);
      }
      throw new IncorrectRootException(rootName);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get node path in the next level of given path
   *
   * @param path do not accept wildcard. cannot be a full path.
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
   * Check whether the path exists.
   *
   * @param path path
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
   * Get node by path
   *
   * @param path path
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
   * Get node by path with storage group check. If storage group is not set,
   * StorageGroupNotSetException will be thrown
   *
   * @param path path
   */
  public MNode getNodeByPathWithStorageGroupCheck(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getNodeByPathWithStorageGroupCheck(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get node by path from cache
   *
   * @param path path
   * @param autoCreateSchema whether auto create schema or not
   * @param sgLevel storage group level
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
          setStorageGroup(storageGroupName);
        }
        node = mtree.addPath(path);
      }
    }
    return node;
  }

  public MNode getNodeByPathFromCache(String path) throws MetadataException {
    return getNodeByPathFromCache(path, dbconfig.isAutoCreateSchemaEnabled(),
        dbconfig.getDefaultStorageGroupLevel());
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
   * Get storage group name when creating schema automatically is enable
   *
   * @param path path
   * @param level level
   */
  String getStorageGroupNameByAutoLevel(String path, int level) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    StringBuilder storageGroupName = new StringBuilder(nodeNames[0]);
    if (nodeNames.length < level || !storageGroupName.toString().equals(ROOT_NAME)) {
      throw new IllegalPathException(path);
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
      if (!sgNode.isNodeType(MNodeType.STORAGE_GROUP_MNODE)) {
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
