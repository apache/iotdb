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
package org.apache.iotdb.db.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 *
 * @author Jinrui Zhang
 */
public class MManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MManager.class);
  private static final String ROOT_NAME = MetadataConstant.ROOT;
  public static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // The file storing the serialize info for metadata
  private String datafilePath;
  // the log file seriesPath
  private String logFilePath;
  private MGraph mgraph;
  private BufferedWriter logWriter;
  private boolean writeToLog;
  private String metadataDirPath;

  private RandomDeleteCache<String, PathCheckRet> checkAndGetDataTypeCache;
  private RandomDeleteCache<String, MNode> mNodeCache;

  private Map<String, Integer> seriesNumberInStorageGroups = new HashMap<>();
  private int maxSeriesNumberAmongStorageGroup;

  private MManager() {
    metadataDirPath = IoTDBDescriptor.getInstance().getConfig().getMetadataDir();
    if (metadataDirPath.length() > 0
        && metadataDirPath.charAt(metadataDirPath.length() - 1) != File.separatorChar) {
      metadataDirPath = metadataDirPath + File.separatorChar;
    }
    File metadataDir = new File(metadataDirPath);
    if (!metadataDir.exists()) {
      metadataDir.mkdirs();
    }
    datafilePath = metadataDirPath + MetadataConstant.METADATA_OBJ;
    logFilePath = metadataDirPath + MetadataConstant.METADATA_LOG;
    writeToLog = false;

    int cacheSize = IoTDBDescriptor.getInstance().getConfig().getmManagerCacheSize();
    checkAndGetDataTypeCache = new RandomDeleteCache<String, PathCheckRet>(cacheSize) {
      @Override
      public void beforeRemove(PathCheckRet object) throws CacheException {
        //allowed to do nothing
      }

      @Override
      public PathCheckRet loadObjectByKey(String key) throws CacheException {
        return loadPathToCache(key);
      }
    };

    mNodeCache = new RandomDeleteCache<String, MNode>(cacheSize) {
      @Override
      public void beforeRemove(MNode object) throws CacheException {
        //allowed to do nothing
      }

      @Override
      public MNode loadObjectByKey(String key) throws CacheException {
        try {
          return getNodeByPathWithCheck(key);
        } catch (PathErrorException e) {
          throw new CacheException(e);
        }
      }
    };

    init();
  }

  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
  }

  //Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  private void init() {

    lock.writeLock().lock();
    File dataFile = new File(datafilePath);
    File logFile = new File(logFilePath);

    try {
      if (dataFile.exists()) {
        initFromDataFile(dataFile);
      } else {
        initFromLog(logFile);
      }
      seriesNumberInStorageGroups = mgraph.countSeriesNumberInEachStorageGroup();
      if (seriesNumberInStorageGroups.isEmpty()) {
        maxSeriesNumberAmongStorageGroup = 0;
      } else {
        maxSeriesNumberAmongStorageGroup = seriesNumberInStorageGroups.values().stream()
            .max(Integer::compareTo).get();
      }
      logWriter = new BufferedWriter(new FileWriter(logFile, true));
      writeToLog = true;

    } catch (PathErrorException | MetadataArgsErrorException
        | ClassNotFoundException | IOException e) {
      mgraph = new MGraph(ROOT_NAME);
      LOGGER.error("Cannot read MGraph from file, using an empty new one");
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void initFromDataFile(File dataFile) throws IOException, ClassNotFoundException {
    // init the metadata from the serialized file
    try(FileInputStream fis = new FileInputStream(dataFile);
    ObjectInputStream ois = new ObjectInputStream(fis)) {
      mgraph = (MGraph) ois.readObject();
      dataFile.delete();
    }
  }

  private void initFromLog(File logFile)
      throws IOException, PathErrorException, MetadataArgsErrorException {
    // init the metadata from the operation log
    mgraph = new MGraph(ROOT_NAME);
    if (logFile.exists()) {
      try( FileReader fr = new FileReader(logFile);
          BufferedReader br = new BufferedReader(fr)) {
        String cmd;
        while ((cmd = br.readLine()) != null) {
          operation(cmd);
        }
      }
    }
  }

  /**
   * function for clearing mgraph.
   */
  public void clear() {
    lock.writeLock().lock();
    try {
      this.mgraph = new MGraph(ROOT_NAME);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void operation(String cmd)
      throws PathErrorException, IOException, MetadataArgsErrorException {
    //see addPathToMTree() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",");
    switch (args[0]) {
      case MetadataOperationType.ADD_PATH_TO_MTREE:
        String[] leftArgs;
        Map<String, String> props = null;
        if (args.length > 5) {
          String[] kv = new String[2];
          props = new HashMap<>(args.length - 5 + 1, 1);
          leftArgs = new String[args.length - 5];
          for (int k = 5; k < args.length; k++) {
            kv = args[k].split("=");
            props.put(kv[0], kv[1]);
          }
        } else {
          //when ????
          leftArgs = new String[0];
        }
        addPathToMTree(args[1], TSDataType.deserialize(Short.valueOf(args[2])),
            TSEncoding.deserialize(Short.valueOf(args[3])),
            CompressionType.deserialize(Short.valueOf(args[4])),
            props);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_MTREE:
        deletePathFromMTree(args[1]);
        break;
      case MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE:
        setStorageLevelToMTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PTREE:
        addAPTree(args[1]);
        break;
      case MetadataOperationType.ADD_A_PATH_TO_PTREE:
        addPathToPTree(args[1]);
        break;
      case MetadataOperationType.DELETE_PATH_FROM_PTREE:
        deletePathFromPTree(args[1]);
        break;
      case MetadataOperationType.LINK_MNODE_TO_PTREE:
        linkMNodeToPTree(args[1], args[2]);
        break;
      case MetadataOperationType.UNLINK_MNODE_FROM_PTREE:
        unlinkMNodeFromPTree(args[1], args[2]);
        break;
      default:
        LOGGER.error("Unrecognizable command {}", cmd);
    }
  }

  private void initLogStream() throws IOException {
    if (logWriter == null) {
      File logFile = new File(logFilePath);
      File metadataDir = new File(metadataDirPath);
      if (!metadataDir.exists()) {
        metadataDir.mkdirs();
      }
      FileWriter fileWriter;
      fileWriter = new FileWriter(logFile, true);
      logWriter = new BufferedWriter(fileWriter);
    }
  }

  /**
   * <p> Add one timeseries to metadata. Must invoke the<code>pathExist</code> and
   * <code>getFileNameByPath</code> method first to check timeseries. </p>
   *
   * @param path the timeseries seriesPath
   * @param dataType the datetype {@code DataType} for the timeseries
   * @param encoding the encoding function {@code Encoding} for the timeseries
   * @param compressor the compressor function {@code Compressor} for the time series
   */
  public void addPathToMTree(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props)
      throws PathErrorException, IOException {

    lock.writeLock().lock();
    try {
      mgraph.addPathToMTree(path, dataType, encoding, compressor, props);
      String storageName = mgraph.getFileNameByPath(path);
      int size = seriesNumberInStorageGroups.get(mgraph.getFileNameByPath(path));
      seriesNumberInStorageGroups
          .put(storageName, size + 1);
      if (size + 1 > maxSeriesNumberAmongStorageGroup) {
        maxSeriesNumberAmongStorageGroup = size + 1;
      }
      if (writeToLog) {
        initLogStream();
        logWriter.write(String.format("%s,%s,%s,%s,%s", MetadataOperationType.ADD_PATH_TO_MTREE,
            path, dataType.serialize(), encoding.serialize(), compressor.serialize()));
        if (props != null) {
          for (Map.Entry entry : props.entrySet()) {
            logWriter.write(String.format(",%s=%s", entry.getKey(), entry.getValue()));
          }
        }
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * <p> Add one timeseries to metadata. Must invoke the<code>pathExist</code> and
   * <code>getFileNameByPath</code> method first to check timeseries. </p>
   *
   * this is just for compatibility
   *
   * @param path the timeseries seriesPath
   * @param dataType the datetype {@code DataType} for the timeseries
   * @param encoding the encoding function {@code Encoding} for the timeseries
   */
  public void addPathToMTree(String path, String dataType, String encoding)
      throws PathErrorException, IOException {
    TSDataType tsDataType = TSDataType.valueOf(dataType);
    TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
    CompressionType type = CompressionType.valueOf(TSFileConfig.compressor);
    addPathToMTree(path, tsDataType, tsEncoding, type, Collections.emptyMap());
  }

  /**
   * function for deleting a given path from mTree.
   */
  public String deletePathFromMTree(String path) throws PathErrorException, IOException {
    lock.writeLock().lock();
    try {
      checkAndGetDataTypeCache.clear();
      mNodeCache.clear();
      String dataFileName = mgraph.deletePath(path);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.DELETE_PATH_FROM_MTREE + "," + path);
        logWriter.newLine();
        logWriter.flush();
      }
      String storageGroup = getFileNameByPath(path);
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
      return dataFileName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for setting storage level of the given path to mTree.
   */
  public void setStorageLevelToMTree(String path) throws PathErrorException, IOException {

    lock.writeLock().lock();
    try {
      checkAndGetDataTypeCache.clear();
      mNodeCache.clear();
      mgraph.setStorageLevel(path);
      seriesNumberInStorageGroups.put(path, 0);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE + "," + path);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for checking if the given path is storage level of mTree or not.
   * @apiNote :for cluster
   */
  public boolean checkStorageLevelOfMTree(String path) {
    lock.readLock().lock();
    try {
      return mgraph.checkStorageLevel(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for adding a pTree.
   */
  public void addAPTree(String ptreeRootName) throws IOException, MetadataArgsErrorException {

    lock.writeLock().lock();
    try {
      mgraph.addAPTree(ptreeRootName);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.ADD_A_PTREE + "," + ptreeRootName);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for adding a given path to pTree.
   */
  public void addPathToPTree(String path)
      throws PathErrorException, IOException, MetadataArgsErrorException {

    lock.writeLock().lock();
    try {
      mgraph.addPathToPTree(path);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.ADD_A_PATH_TO_PTREE + "," + path);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for deleting a given path from pTree.
   */
  public void deletePathFromPTree(String path) throws PathErrorException, IOException {

    lock.writeLock().lock();
    try {
      mgraph.deletePath(path);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.DELETE_PATH_FROM_PTREE + "," + path);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for linking MNode to pTree.
   */
  public void linkMNodeToPTree(String path, String mpath) throws PathErrorException, IOException {

    lock.writeLock().lock();
    try {
      mgraph.linkMNodeToPTree(path, mpath);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.LINK_MNODE_TO_PTREE + "," + path + "," + mpath);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for unlinking MNode from pTree.
   */
  public void unlinkMNodeFromPTree(String path, String mpath)
      throws PathErrorException, IOException {

    lock.writeLock().lock();
    try {
      mgraph.unlinkMNodeFromPTree(path, mpath);
      if (writeToLog) {
        initLogStream();
        logWriter.write(MetadataOperationType.UNLINK_MNODE_FROM_PTREE + "," + path + "," + mpath);
        logWriter.newLine();
        logWriter.flush();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get series type for given seriesPath.
   *
   * @return TSDataType
   */
  public TSDataType getSeriesType(String fullPath) throws PathErrorException {

    lock.readLock().lock();
    try {
      return getSchemaForOnePath(fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting series type.
   */
  public TSDataType getSeriesType(MNode node, String fullPath) throws PathErrorException {

    lock.readLock().lock();
    try {
      return getSchemaForOnePath(node, fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting series type with check.
   */
  public TSDataType getSeriesTypeWithCheck(MNode node, String fullPath) throws PathErrorException {

    lock.readLock().lock();
    try {
      return getSchemaForOnePathWithCheck(node, fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * unction for getting series type with check.
   */
  public TSDataType getSeriesTypeWithCheck(String fullPath) throws PathErrorException {

    lock.readLock().lock();
    try {
      return getSchemaForOnePathWithCheck(fullPath).getType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all device type in current Metadata Tree.
   *
   * @return a HashMap contains all distinct device type separated by device Type
   */
  // future feature
  @SuppressWarnings("unused")
  public Map<String, List<MeasurementSchema>> getSchemaForAllType() throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaForAllType();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the full Metadata info.
   *
   * @return A {@code Metadata} instance which stores all metadata info
   */
  public Metadata getMetadata() throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getMetadata();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the full storage group info.
   *
   * @return A HashSet instance which stores all storage group info
   */
  public Set<String> getAllStorageGroup() throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getAllStorageGroup();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @deprecated Get all MeasurementSchemas for given delta object type.
   *
   * @param path A seriesPath represented one Delta object
   * @return a list contains all column schema
   */
  @Deprecated
  public List<MeasurementSchema> getSchemaForOneType(String path) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOneType(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all MeasurementSchemas for the filenode seriesPath.
   */
  public List<MeasurementSchema> getSchemaForFileName(String path) {
    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOneFileNode(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting schema map for one file node.
   */
  public Map<String, MeasurementSchema> getSchemaMapForOneFileNode(String path) {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaMapForOneFileNode(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting num schema map for one file node.
   */
  public Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {

    lock.readLock().lock();
    try {
      return mgraph.getNumSchemaMapForOneFileNode(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Calculate the count of storage-level nodes included in given seriesPath.
   *
   * @return The total count of storage-level nodes.
   */
  // future feature
  @SuppressWarnings("unused")
  public int getFileCountForOneType(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getFileCountForOneType(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the file name for given seriesPath Notice: This method could be called if and only if the
   * seriesPath includes one node whose {@code isStorageLevel} is true.
   *
   * @return A String represented the file name
   */
  public String getFileNameByPath(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getFileNameByPath(path);
    } catch (PathErrorException e) {
      throw new PathErrorException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting file name by path.
   */
  public String getFileNameByPath(MNode node, String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getFileNameByPath(node, path);
    } catch (PathErrorException e) {
      throw new PathErrorException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking file name by path.
   */
  public boolean checkFileNameByPath(String path) {

    lock.readLock().lock();
    try {
      return mgraph.checkFileNameByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting all file names.
   */
  public List<String> getAllFileNames() throws PathErrorException {

    lock.readLock().lock();
    try {
      Map<String, ArrayList<String>> res = getAllPathGroupByFileName(ROOT_NAME);
      return new ArrayList<>(res.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all file names for given seriesPath
   *
   * @return List of String represented all file names
   */
  public List<String> getAllFileNamesByPath(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getAllFileNamesByPath(path);
    } catch (PathErrorException e) {
      throw new PathErrorException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * return a HashMap contains all the paths separated by File Name.
   */
  public Map<String, ArrayList<String>> getAllPathGroupByFileName(String path)
      throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getAllPathGroupByFilename(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all paths for given seriesPath if the seriesPath is abstract. Or return the seriesPath
   * itself.
   */
  public List<String> getPaths(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      ArrayList<String> res = new ArrayList<>();
      Map<String, ArrayList<String>> pathsGroupByFilename = getAllPathGroupByFileName(path);
      for (ArrayList<String> ps : pathsGroupByFilename.values()) {
        res.addAll(ps);
      }
      return res;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   */
  public List<List<String>> getShowTimeseriesPath(String path) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getShowTimeseriesPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting leaf node path in the next level of given seriesPath.
   */
  public List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getLeafNodePathInNextLevel(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check whether the seriesPath given exists.
   */
  public boolean pathExist(String path) {

    lock.readLock().lock();
    try {
      return mgraph.pathExist(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking whether the path exists.
   */
  public boolean pathExist(MNode node, String path) {

    lock.readLock().lock();
    try {
      return mgraph.pathExist(node, path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting node by path.
   */
  public MNode getNodeByPath(String path) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getNodeByPath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting node by deviceId from cache.
   */
  public MNode getNodeByDeviceIdFromCache(String deviceId) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mNodeCache.get(deviceId);
    } catch (CacheException e) {
      throw new PathErrorException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting node by path with check.
   */
  public MNode getNodeByPathWithCheck(String path) throws PathErrorException {
    lock.readLock().lock();
    try {
      return mgraph.getNodeByPathWithCheck(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get MeasurementSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  public MeasurementSchema getSchemaForOnePath(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOnePath(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting schema for one path.
   */
  public MeasurementSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOnePath(node, path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting schema for one path with check.
   */
  public MeasurementSchema getSchemaForOnePathWithCheck(MNode node, String path)
      throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOnePathWithCheck(node, path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for getting schema for one path with check.
   */
  public MeasurementSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      return mgraph.getSchemaForOnePathWithCheck(path);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check whether given seriesPath contains a MNode whose {@code MNode.isStorageLevel} is true.
   */
  public boolean checkFileLevel(List<Path> path) throws PathErrorException {

    lock.readLock().lock();
    try {
      for (Path p : path) {
        getFileNameByPath(p.getFullPath());
      }
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking file level.
   */
  public boolean checkFileLevel(MNode node, List<Path> path) throws PathErrorException {

    lock.readLock().lock();
    try {
      for (Path p : path) {
        getFileNameByPath(node, p.getFullPath());
      }
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking file level.
   */
  public boolean checkFileLevel(String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      getFileNameByPath(path);
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for checking file level with check.
   */
  public boolean checkFileLevelWithCheck(MNode node, String path) throws PathErrorException {

    lock.readLock().lock();
    try {
      getFileNameByPath(node, path);
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * function for flushing object to file.
   */
  public void flushObjectToFile() throws IOException {

    lock.writeLock().lock();
    File dataFile = new File(datafilePath);
    // delete old metadata data file
    if (dataFile.exists()) {
      dataFile.delete();
    }
    File metadataDir = new File(metadataDirPath);
    if (!metadataDir.exists()) {
      metadataDir.mkdirs();
    }
    File tempFile = new File(datafilePath + MetadataConstant.METADATA_TEMP);
    try(FileOutputStream fos = new FileOutputStream(tempFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos)) {
      oos.writeObject(mgraph);
      // close the logFile stream
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      // rename temp file to data file
      tempFile.renameTo(dataFile);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * function for getting metadata in string.
   */
  public String getMetadataInString() {

    lock.readLock().lock();
    try {
      StringBuilder builder = new StringBuilder();
      builder.append(TIME_SERIES_TREE_HEADER).append(mgraph.toString());
      return builder.toString();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * combine multiple metadata in string format
   */
  public static String combineMetadataInStrings(String[] metadatas) {
    for (int i = 0; i < metadatas.length; i++) {
      metadatas[i] = metadatas[i].replace(TIME_SERIES_TREE_HEADER, "");
    }
    String res = MGraph.combineMetadataInStrings(metadatas);
    StringBuilder builder = new StringBuilder();
    builder.append(TIME_SERIES_TREE_HEADER).append(res);
    return builder.toString();
  }

  /**
   * Check whether {@code seriesPath} exists and whether {@code seriesPath} has been set storage
   * level.
   *
   * @return {@link PathCheckRet}
   */
  public PathCheckRet checkPathStorageLevelAndGetDataType(String path) throws PathErrorException {
    try {
      return checkAndGetDataTypeCache.get(path);
    } catch (CacheException e) {
      throw new PathErrorException(e);
    }
  }

  private PathCheckRet loadPathToCache(String path) throws CacheException {
    try {
      if (!pathExist(path)) {
        return new PathCheckRet(false, null);
      }
      List<Path> p = new ArrayList<>();
      p.add(new Path(path));
      if (!checkFileLevel(p)) {
        return new PathCheckRet(false, null);
      }
      return new PathCheckRet(true, getSeriesType(path));
    } catch (PathErrorException e) {
      throw new CacheException(e);
    }
  }

  public int getMaximalSeriesNumberAmongStorageGroups() {
    return maxSeriesNumberAmongStorageGroup;
  }

  private static class MManagerHolder {
    private MManagerHolder(){
      //allowed to do nothing
    }
    private static final MManager INSTANCE = new MManager();
  }

  public static class PathCheckRet {

    private boolean successfully;
    private TSDataType dataType;

    public PathCheckRet(boolean successfully, TSDataType dataType) {
      this.successfully = successfully;
      this.dataType = dataType;
    }

    public boolean isSuccessfully() {
      return successfully;
    }

    public TSDataType getDataType() {
      return dataType;
    }
  }
}
