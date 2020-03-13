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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
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
  private static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // the log file seriesPath
  private String logFilePath;
  private MTree mtree;
  private BufferedWriter logWriter;
  private boolean writeToLog;
  private String schemaDir;
  // path -> MNode
  private RandomDeleteCache<String, MNode> mNodeCache;

  private Map<String, Integer> seriesNumberInStorageGroups = new HashMap<>();
  private long maxSeriesNumberAmongStorageGroup;
  private boolean initialized;
  private IoTDBConfig config;

  private MManager() {
    config = IoTDBDescriptor.getInstance().getConfig();
    schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
    logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    writeToLog = false;

    int cacheSize = config.getmManagerCacheSize();
    mNodeCache = new RandomDeleteCache<String, MNode>(cacheSize) {

      @Override
      public MNode loadObjectByKey(String key) throws CacheException {
        lock.readLock().lock();
        try {
          return mtree.getNodeByPathWithStorageGroupCheck(key);
        } catch (MetadataException e) {
          throw new CacheException(e);
        } finally {
          lock.readLock().unlock();
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
      mtree = new MTree();
      logger.error("Cannot read MTree from file, using an empty new one", e);
    } finally {
      lock.writeLock().unlock();
    }
    initialized = true;
  }

  private void initFromLog(File logFile) throws IOException, MetadataException {
    // init the metadata from the operation log
    mtree = new MTree();
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
      this.mtree = new MTree();
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
    //see createTimeseries() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",");
    switch (args[0]) {
      case MetadataOperationType.CREATE_TIMESERIES:
        Map<String, String> props = null;
        if (args.length > 5) {
          String[] kv;
          props = new HashMap<>(args.length - 5 + 1, 1);
          for (int k = 5; k < args.length; k++) {
            kv = args[k].split("=");
            props.put(kv[0], kv[1]);
          }
        }

        createTimeseries(args[1], TSDataType.deserialize(Short.parseShort(args[2])),
            TSEncoding.deserialize(Short.parseShort(args[3])),
            CompressionType.deserialize(Short.parseShort(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_TIMESERIES:
        deleteTimeseries(args[1]);
        break;
      case MetadataOperationType.SET_STORAGE_GROUP:
        setStorageGroup(args[1]);
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP:
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
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   * @return whether the measurement occurs for the first time in this storage group (if true, the
   * measurement should be registered to the StorageEngine too)
   */
  public boolean createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException {
    lock.writeLock().lock();
    try {
      /*
       * get the storage group with auto create schema
       */
      String storageGroupName;
      try {
        storageGroupName = mtree.getStorageGroupName(path);
      } catch (StorageGroupNotSetException e) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw e;
        }
        storageGroupName = MetaUtils.getStorageGroupNameByLevel(path,
            config.getDefaultStorageGroupLevel());
        setStorageGroup(storageGroupName);
      }

      /*
       * check if the measurement schema conflict in its storage group
       */
      Map<String, MeasurementSchema> schemaMap = mtree.getStorageGroupSchemaMap(storageGroupName);
      String measurement = new Path(path).getMeasurement();
      boolean isNewMeasurement = true;
      if (schemaMap.containsKey(measurement)) {
        isNewMeasurement = false;
        MeasurementSchema schema = schemaMap.get(measurement);
        if (!schema.getType().equals(dataType) || !schema.getEncodingType().equals(encoding)
            || !schema.getCompressor().equals(compressor)) {
          // conflict with existing
          throw new MetadataException(String.format(
              "The resultDataType or encoding or compression of the last node %s is conflicting "
                  + "in the storage group %s", measurement, storageGroupName));
        }
      }

      // create time series with memory check
      createTimeseriesWithMemoryCheckAndLog(path, dataType, encoding, compressor, props);
      // register schema in this storage group
      if (isNewMeasurement) {
        schemaMap.put(measurement,
            new MeasurementSchema(measurement, dataType, encoding, compressor, props));
      }
      // update statistics
      int size = seriesNumberInStorageGroups.get(storageGroupName);
      seriesNumberInStorageGroups.put(storageGroupName, size + 1);
      if (size + 1 > maxSeriesNumberAmongStorageGroup) {
        maxSeriesNumberAmongStorageGroup = size + 1;
      }
      return isNewMeasurement;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @TestOnly
  public void createTimeseries(String path, String dataType, String encoding)
      throws MetadataException {
    lock.writeLock().lock();
    try {
      TSDataType tsDataType = TSDataType.valueOf(dataType);
      TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
      CompressionType type = TSFileDescriptor.getInstance().getConfig().getCompressor();
      createTimeseriesWithMemoryCheckAndLog(path, tsDataType, tsEncoding, type,
          Collections.emptyMap());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * timeseries will be added to MTree with check memory, and log to file
   */
  private void createTimeseriesWithMemoryCheckAndLog(String timeseries, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props)
      throws MetadataException {
    mtree.createTimeseries(timeseries, dataType, encoding, compressor, props);
    try {
      // check memory
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
    } catch (ConfigAdjusterException e) {
      mtree.deleteTimeseriesAndReturnEmptyStorageGroup(timeseries);
      throw new MetadataException(e);
    }
    try {
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(String.format("%s,%s,%s,%s,%s", MetadataOperationType.CREATE_TIMESERIES,
            timeseries, dataType.serialize(), encoding.serialize(), compressor.serialize()));
        if (props != null) {
          for (Map.Entry entry : props.entrySet()) {
            writer.write(String.format(",%s=%s", entry.getKey(), entry.getValue()));
          }
        }
        writer.newLine();
        writer.flush();
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Delete all timeseries under the given path, may cross different storage group
   *
   * @param prefixPath path to be deleted, could be root or a prefix path or a full path
   * @return a set contains StorageGroups that contain no more timeseries after this deletion and
   * files of such StorageGroups should be deleted to reclaim disk space.
   */
  public Set<String> deleteTimeseries(String prefixPath) throws MetadataException {
    lock.writeLock().lock();
    if (isStorageGroup(prefixPath)) {
      int size = seriesNumberInStorageGroups.get(prefixPath);
      seriesNumberInStorageGroups.put(prefixPath, 0);
      if (size == maxSeriesNumberAmongStorageGroup) {
        seriesNumberInStorageGroups.values().stream().max(Integer::compareTo)
            .ifPresent(val -> maxSeriesNumberAmongStorageGroup = val);
      }
      try {
        IoTDBConfigDynamicAdapter.getInstance()
            .addOrDeleteTimeSeries(seriesNumberInStorageGroups.remove(prefixPath) * (-1));
      } catch (ConfigAdjusterException e) {
        throw new MetadataException(e.getMessage());
      }
      mNodeCache.clear();
    }
    try {
      Set<String> emptyStorageGroups = new HashSet<>();

      List<String> allTimeseries = mtree.getAllTimeseriesName(prefixPath);
      // Monitor storage group seriesPath is not allowed to be deleted
      allTimeseries.removeIf(p -> p.startsWith(MonitorConstants.STAT_STORAGE_GROUP_PREFIX));

      for (String p : allTimeseries) {
        String emptyStorageGroup = deleteOneTimeseriesAndUpdateStatisticsAndLog(p);
        if (emptyStorageGroup != null) {
          emptyStorageGroups.add(emptyStorageGroup);
        }
      }
      return emptyStorageGroups;
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return after delete if the storage group is empty, return its name, otherwise return null
   */
  private String deleteOneTimeseriesAndUpdateStatisticsAndLog(String path)
      throws MetadataException, IOException {
    lock.writeLock().lock();
    try {
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.DELETE_TIMESERIES + "," + path);
        writer.newLine();
        writer.flush();
      }
      String storageGroupName = mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);
      // TODO: delete the path node and all its ancestors
      mNodeCache.clear();
      try {
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(-1);
      } catch (ConfigAdjusterException e) {
        throw new MetadataException(e);
      }
      String storageGroup = getStorageGroupName(path);
      int size = seriesNumberInStorageGroups.get(storageGroup);
      seriesNumberInStorageGroups.put(storageGroup, size - 1);
      if (size == maxSeriesNumberAmongStorageGroup) {
        seriesNumberInStorageGroups.values().stream().max(Integer::compareTo)
            .ifPresent(val -> maxSeriesNumberAmongStorageGroup = val);
      }
      return storageGroupName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Set storage group of the given path to MTree. Check
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(String storageGroup) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (writeToLog) {
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.SET_STORAGE_GROUP + "," + storageGroup);
        writer.newLine();
        writer.flush();
      }
      mtree.setStorageGroup(storageGroup);
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
      ActiveTimeSeriesCounter.getInstance().init(storageGroup);
      seriesNumberInStorageGroups.put(storageGroup, 0);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    } catch (ConfigAdjusterException e) {
      mtree.deleteStorageGroup(storageGroup);
      throw new MetadataException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Delete storage groups of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3"
   *
   * @param storageGroups list of paths to be deleted. Format: root.node
   */
  public void deleteStorageGroups(List<String> storageGroups) throws MetadataException {
    lock.writeLock().lock();
    try {
      if (writeToLog) {
        StringBuilder jointPath = new StringBuilder();
        for (String storagePath : storageGroups) {
          jointPath.append(",").append(storagePath);
        }
        BufferedWriter writer = getLogWriter();
        writer.write(MetadataOperationType.DELETE_STORAGE_GROUP + jointPath);
        writer.newLine();
        writer.flush();
      }
      for (String storageGroup : storageGroups) {
        try {
          // try to delete storage group
          mtree.deleteStorageGroup(storageGroup);
        } catch (MetadataException e) {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
          throw new MetadataException(e);
        }
        mNodeCache.clear();
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(-1);
        int size = seriesNumberInStorageGroups.get(storageGroup);
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(size * -1);
        ActiveTimeSeriesCounter.getInstance().delete(storageGroup);
        seriesNumberInStorageGroups.remove(storageGroup);
        if (size == maxSeriesNumberAmongStorageGroup) {
          if (seriesNumberInStorageGroups.isEmpty()) {
            maxSeriesNumberAmongStorageGroup = 0;
          } else {
            maxSeriesNumberAmongStorageGroup = seriesNumberInStorageGroups.values().stream()
                .max(Integer::compareTo).get();
          }
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
   * @param path full path
   */
  public TSDataType getSeriesType(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      if (path.equals(SQLConstant.RESERVED_TIME)) {
        return TSDataType.INT64;
      }
      return mtree.getSchema(path).getType();
    } finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Get all devices under given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   * @return A HashSet instance which stores devices names with given prefixPath.
   */
  public Set<String> getDevices(String prefixPath) throws MetadataException {
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
   * @param storageGroup storage group name
   */
  public List<MeasurementSchema> getStorageGroupSchema(String storageGroup)
      throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupSchema(storageGroup);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get storage group name by path
   *
   * e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
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
   * Get all storage group names
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
  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    lock.readLock().lock();
    try {
      return mtree.getAllStorageGroupNodes();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all paths for given path if the path is abstract. Or return the path itself. Regular
   * expression in this method is formed by the amalgamation of seriesPath and the character '*'.
   *
   * @param prefixPath can be a prefix or a full path. if the wildcard is not at the tail, then each
   * wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<String> getAllTimeseriesName(String prefixPath) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getAllTimeseriesName(prefixPath);
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
   * @return for each storage group, return a List [name, sg name, data type, encoding, compressor]
   */
  public List<String[]> getAllTimeseriesSchema(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getAllTimeseriesSchema(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1, return
   * [root.sg1.d1, root.sg1.d2]
   *
   * @return All child nodes' seriesPath(s) of given seriesPath.
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
   * @param path a full path or a prefix path
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
   */
  public MNode getNodeByPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getNodeByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get storage group node by path. If storage group is not set, StorageGroupNotSetException will
   * be thrown
   */
  public StorageGroupMNode getStorageGroupNode(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupNode(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * @param path path
   */
  public MNode getDeviceNodeWithAutoCreateStorageGroup(String path, boolean autoCreateSchema,
      int sgLevel) throws MetadataException {
    lock.readLock().lock();
    MNode node = null;
    boolean shouldSetStorageGroup = false;
    try {
      node = mNodeCache.get(path);
    } catch (CacheException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path);
      } else {
        shouldSetStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
      }
    } finally {
      lock.readLock().unlock();
      if (autoCreateSchema) {
        if (shouldSetStorageGroup) {
          String storageGroupName = MetaUtils.getStorageGroupNameByLevel(path, sgLevel);
          setStorageGroup(storageGroupName);
        }
        node = mtree.getDeviceNodeWithAutoCreating(path);
      }
    }
    return node;
  }

  public MNode getDeviceNodeWithAutoCreateStorageGroup(String path) throws MetadataException {
    return getDeviceNodeWithAutoCreateStorageGroup(path, config.isAutoCreateSchemaEnabled(),
        config.getDefaultStorageGroupLevel());
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
      getStorageGroupNode(storageGroup).setDataTTL(dataTTL);
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

  /**
   * Check whether the given path contains a storage group
   */
  boolean checkStorageGroupByPath(String path) {
    lock.readLock().lock();
    try {
      return mtree.checkStorageGroupByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Get all storage groups under the given path
   *
   * @return List of String represented all storage group names
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      return mtree.getStorageGroupByPath(path);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void collectSeries(MNode startingNode, Collection<MeasurementSchema> timeseriesSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node instanceof LeafMNode) {
        MeasurementSchema nodeSchema = node.getSchema();
        timeseriesSchemas.add(new MeasurementSchema(node.getFullPath(),
            nodeSchema.getType(), nodeSchema.getEncodingType(), nodeSchema.getCompressor()));
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  public void collectSeries(String startingPath, List<MeasurementSchema> timeseriesSchemas) {
    MNode mNode;
    try {
      mNode = getNodeByPath(startingPath);
    } catch (MetadataException e) {
      return;
    }
    collectSeries(mNode, timeseriesSchemas);
  }

  /**
   * For a path, infer all storage groups it may belong to.
   * The path can have wildcards.
   *
   * Consider the path into two parts: (1) the sub path which can not contain a storage group name and
   * (2) the sub path which is substring that begin after the storage group name.
   *
   * (1) Suppose the part of the path can not contain a storage group name (e.g.,
   * "root".contains("root.sg") == false), then:
   * If the wildcard is not at the tail, then for each wildcard, only one level will be inferred
   * and the wildcard will be removed.
   * If the wildcard is at the tail, then the inference will go on until the storage groups are found
   * and the wildcard will be kept.
   * (2) Suppose the part of the path is a substring that begin after the storage group name. (e.g.,
   *  For "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part is "a.*.b.*").
   *  For this part, keep what it is.
   *
   * Assuming we have three SGs: root.group1, root.group2, root.area1.group3
   * Eg1:
   *  for input "root.*", returns ("root.group1", "root.group1.*"), ("root.group2", "root.group2.*")
   *  ("root.area1.group3", "root.area1.group3.*")
   * Eg2:
   *  for input "root.*.s1", returns ("root.group1", "root.group1.s1"), ("root.group2", "root.group2.s1")
   *
   * Eg3:
   *  for input "root.area1.*", returns ("root.area1.group3", "root.area1.group3.*")
   *
   * @param path can be a prefix or a full path.
   * @return StorageGroupName-FullPath pairs
   */
  public Map<String, String> determineStorageGroup(String path) throws IllegalPathException {
    lock.readLock().lock();
    try {
      return mtree.determineStorageGroup(path);
    } finally {
      lock.readLock().unlock();
    }
  }
}
