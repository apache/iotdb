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

package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.google.common.util.concurrent.Striped;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Holder;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class MRocksDBManager {
  private static final String ROCKSDB_FOLDER = "rocksdb-schema";
  private static final String TABLE_NAME_STORAGE_GROUP = "storageGroupNodes".toLowerCase();
  private static final String TABLE_NAME_MEASUREMENT = "timeSeriesNodes".toLowerCase();
  private static final String TABLE_NAME_DEVICE = "deviceNodes".toLowerCase();
  private final Options options;
  private final RocksDB rocksDB;
  private static final char ZERO = '0';
  private static final String ROOT = "r";
  private static final String ESCAPE_PATH_SEPARATOR = "[.]";
  // TEMP
  private static final byte SG_NODE_FLAG = 0x31;
  private static final byte DEVICE_NODE_FLAG = 0x32;
  private static final byte MEASUREMENT_NODE_FLAG = 0x34;
  private static final byte INNER_NODE_FLAG = 0x38;
  ConcurrentMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();
  List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final String ROCKSDB_PATH = config.getSystemDir() + File.separator + ROCKSDB_FOLDER;
  private static final Logger logger = LoggerFactory.getLogger(MRocksDBManager.class);

  static {
    RocksDB.loadLibrary();
  }

  public MRocksDBManager() throws RocksDBException {
    options = new Options();
    options.setCreateIfMissing(true);
    options.setAllowMmapReads(true);
    options.setRowCache(new LRUCache(900000));
    options.setWriteBufferSize(1024 * 1024 * 16);
    org.rocksdb.Logger rocksDBLogger = new RockDBLogger(options, logger);
    rocksDBLogger.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
    options.setLogger(rocksDBLogger);
    List<byte[]> cfs = RocksDB.listColumnFamilies(options, ROCKSDB_PATH);
    if (cfs.size() > 0) {
      for (byte[] cf : cfs) {
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
      }
    } else {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
    }
    DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    rocksDB = RocksDB.open(dbOptions, ROCKSDB_PATH, columnFamilyDescriptors, columnFamilyHandles);
    createTable(TABLE_NAME_STORAGE_GROUP);
    createTable(TABLE_NAME_MEASUREMENT);
    createTable(TABLE_NAME_DEVICE);
    // TODO: make sure `root` existed
    if (rocksDB.get(ROOT.getBytes()) == null) {
      rocksDB.put(ROOT.getBytes(), "".getBytes());
    }
  }

  private void createTable(String tableName) {
    try {
      tableName = tableName.toLowerCase();
      if (columnFamilyHandleMap.get(tableName) == null) {
        ColumnFamilyHandle columnFamilyHandle =
            rocksDB.createColumnFamily(
                new ColumnFamilyDescriptor(tableName.getBytes(), new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(
            new ColumnFamilyDescriptor(tableName.getBytes(), new ColumnFamilyOptions()));
        columnFamilyHandles.add(columnFamilyHandle);
        columnFamilyHandleMap.put(tableName, columnFamilyHandle);
        System.out.println("创建rocksdb表名为:" + tableName);
      } else {
        System.out.println("该表已经存在：" + tableName);
      }
    } catch (RocksDBException e) {
      System.out.println("创建rocksdb的表异常,表名:" + tableName + e.getMessage());
    }
  }

  private void createNodeTypeByTableName(String tableName, String key, byte[] value) {
    Lock lock = locks.get(key);
    try {
      lock.lock();
      tableName = tableName.toLowerCase();
      if (columnFamilyHandleMap.containsKey(tableName)) {
        ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(tableName);
        rocksDB.put(columnFamilyHandle, key.getBytes(), new byte[] {});
      } else {
        System.out.println(tableName + "表不存在,无法写入：" + key);
      }
    } catch (RocksDBException e) {
      System.out.println("添加rocksDB:key值数据异常" + e.getMessage());
    } finally {
      lock.unlock();
    }
  }

  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    String[] nodes = storageGroup.getNodes();
    try {
      long session = sessionId.incrementAndGet();
      boolean sgExisted = false;
      for (int i = 0; i < nodes.length; i++) {
        String normalKey = constructKey(nodes, i);
        Holder<byte[]> holder = new Holder();
        if (!keyExist(normalKey, holder, session)) {
          if (!sgExisted) {
            if (i < nodes.length - 1) {
              createKey(normalKey, new byte[] {INNER_NODE_FLAG}, session);
            } else {
              createKey(normalKey, new byte[] {SG_NODE_FLAG}, session);
              createNodeTypeByTableName(
                  TABLE_NAME_STORAGE_GROUP, normalKey, new byte[] {SG_NODE_FLAG});
            }
          } else {
            throw new StorageGroupAlreadySetException(storageGroup.getFullPath());
          }
        } else {
          byte[] value = holder.getValue();
          sgExisted = value.length > 0 && holder.getValue()[0] == SG_NODE_FLAG;
        }
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  private void createKey(String key, byte[] value, long sessionId) throws RocksDBException {
    Lock lock = locks.get(key);
    try {
      lock.lock();
      if (!keyExist(key, sessionId)) {
        rocksDB.put(key.getBytes(), value);
      }
    } finally {
      lock.unlock();
    }
  }
  // TODO: check how Stripped Lock consume memory
  Striped<Lock> locks = Striped.lazyWeakLock(10000);

  public void scanAllKeys() throws IOException {
    RocksIterator iterator = rocksDB.newIterator();
    System.out.println("\n-----------------scan rocksdb start----------------------");
    iterator.seekToFirst();
    File outputFile = new File(config.getSystemDir() + "/" + "rocksdb.key");
    if (!outputFile.exists()) {
      outputFile.createNewFile();
    }
    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile));
    while (iterator.isValid()) {
      outputStream.write(iterator.key());
      outputStream.write(" -> ".getBytes());
      outputStream.write(iterator.value());
      outputStream.write("\n".getBytes());
      iterator.next();
    }
    outputStream.close();
    System.out.println("\n-----------------scan rocksdb end----------------------");
  }

  public static AtomicLong sessionId = new AtomicLong(0);
  private static Map<Long, String> costMap = new HashMap();

  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodes = path.getNodes();
    try {
      if (!checkStorageGroupByPath(path)) {
        throw new StorageGroupNotSetException(path.getFullPath());
      }
      createTimeSeriesRecursive(nodes, nodes.length);
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  private void createTimeSeriesRecursive(String nodes[], int start) throws RocksDBException {
    if (start < 1) {
      // "root" must exist
      return;
    }
    String key = constructKey(nodes, start - 1);
    if (!keyExist(key, -1)) {
      createTimeSeriesRecursive(nodes, start - 1);
      byte[] value;
      if (start == nodes.length) {
        value = new byte[] {MEASUREMENT_NODE_FLAG};
        createNodeTypeByTableName(TABLE_NAME_MEASUREMENT, key, value);
      } else if (start == nodes.length - 1) {
        value = new byte[] {DEVICE_NODE_FLAG};
        createNodeTypeByTableName(TABLE_NAME_DEVICE, key, value);
      } else {
        value = new byte[] {INNER_NODE_FLAG};
      }
      createKey(key, value, -1);
    }
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = constructKey(nodes, nodes.length - 1);
    try {
      byte[] value = rocksDB.get(key.getBytes());
      if (value == null) {
        logger.warn("path not exist: {}", key);
        throw new MetadataException("key not exist");
      }
      IMeasurementMNode node = new MeasurementMNode(null, fullPath.getFullPath(), null, null);
      return node;
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  public long countStorageGroupNodes() {
    return countNodesNum(TABLE_NAME_STORAGE_GROUP);
  }

  public long countMeasurementNodes() {
    return countNodesNum(TABLE_NAME_MEASUREMENT);
  }

  public long countDeviceNodes() {
    return countNodesNum(TABLE_NAME_DEVICE);
  }

  private long countNodesNum(String tableName) {
    ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(tableName);
    RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
    long count = 0;
    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
      count++;
    }
    return count;
  }

  private boolean keyExist(String key, Holder<byte[]> holder, long sessionId)
      throws RocksDBException {
    boolean exist = false;
    if (!rocksDB.keyMayExist(key.getBytes(), holder)) {
      exist = false;
    } else {
      byte[] value = rocksDB.get(key.getBytes());
      if (value != null) {
        exist = true;
        holder.setValue(value);
      }
    }
    return exist;
  }

  private boolean keyExist(String key, long sessionId) throws RocksDBException {
    return keyExist(key, new Holder<>(), sessionId);
  }

  private String constructKey(String[] nodes, int end) {
    return constructKey(nodes, end, end);
  }

  private String constructKey(String[] nodes, int end, int level) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT);
    char depth = (char) (ZERO + level);
    for (int i = 1; i <= end; i++) {
      builder.append(PATH_SEPARATOR).append(depth).append(nodes[i]);
    }
    return builder.toString();
  }
  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) throws RocksDBException {
    String[] nodes = path.getNodes();
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String key = constructKey(nodes, i);
      Holder<byte[]> holder = new Holder();
      if (keyExist(key, holder, -1)) {
        byte[] value = holder.getValue();
        return value.length > 0 && value[0] == SG_NODE_FLAG;
      } else {
        return false;
      }
    }
    return false;
  }
  /**
   * Get child node path in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) {
    String[] nodes = pathPattern.getNodes();
    String startKey =
        constructKey(nodes, nodes.length - 1, nodes.length)
            + PATH_SEPARATOR
            + (char) (ZERO + nodes.length);
    return getAllByPrefix(startKey);
  }
  /**
   * Get all nodes matching the given path pattern in the given level. The level of the path should
   * match the nodeLevel. 1. The given level equals the path level with out **, e.g. give path
   * root.*.d.* and the level should be 4. 2. The given level is greater than path level with **,
   * e.g. give path root.** and the level could be 2 or 3.
   *
   * @param pathPattern can be a pattern of a full path.
   * @param nodeLevel the level should match the level of the path
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    // TODO: ignore pathPattern with *, all nodeLevel are start from "root.*"
    List<PartialPath> results = new ArrayList<>();
    if (nodeLevel == 0) {
      results.add(new PartialPath(PATH_ROOT));
      return results;
    }
    // TODO: level one usually only contains small numbers, query in serialize
    Set<String> paths;
    StringBuilder builder = new StringBuilder();
    if (nodeLevel <= 5) {
      char level = (char) (ZERO + nodeLevel);
      String prefix = builder.append(ROOT).append(PATH_SEPARATOR).append(level).toString();
      paths = getAllByPrefix(prefix);
    } else {
      paths = ConcurrentHashMap.newKeySet();
      char upperLevel = (char) (ZERO + nodeLevel - 1);
      String prefix = builder.append(ROOT).append(PATH_SEPARATOR).append(upperLevel).toString();
      Set<String> parentPaths = getAllByPrefix(prefix);
      parentPaths
          .parallelStream()
          .forEach(
              x -> {
                String targetPrefix = getNextLevelOfPath(x, upperLevel);
                paths.addAll(getAllByPrefix(targetPrefix));
              });
    }
    return convertToPartialPath(paths, nodeLevel);
  }
  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    AtomicInteger counter = new AtomicInteger(0);
    Set<String> seeds = new HashSet<>();
    seeds.add(ROOT + PATH_SEPARATOR + ZERO);
    scanAllKeysRecursively(
        seeds,
        0,
        s -> {
          try {
            byte[] value = rocksDB.get(s.getBytes());
            if (value != null && value.length > 0 && value[0] == MEASUREMENT_NODE_FLAG) {
              counter.incrementAndGet();
              return false;
            }
          } catch (RocksDBException e) {
            return false;
          }
          return true;
        });
    return counter.get();
  }
  /** To calculate the count of devices for given path pattern. */
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    AtomicInteger counter = new AtomicInteger(0);
    Set<String> seeds = new HashSet<>();
    seeds.add(ROOT + PATH_SEPARATOR + ZERO);
    scanAllKeysRecursively(
        seeds,
        0,
        s -> {
          try {
            byte[] value = rocksDB.get(s.getBytes());
            if (value != null && value.length > 0 && value[0] == DEVICE_NODE_FLAG) {
              counter.incrementAndGet();
              return false;
            }
          } catch (RocksDBException e) {
            return false;
          }
          return true;
        });
    return counter.get();
  }

  private BlockingQueue<String> blockingQueue = new LinkedBlockingQueue();

  private void scanAllKeysRecursively(Set<String> seeds, int level, Function<String, Boolean> op) {
    if (seeds == null || seeds.isEmpty()) {
      return;
    }
    Set<String> children = ConcurrentHashMap.newKeySet();
    seeds
        .parallelStream()
        .forEach(
            x -> {
              if (op.apply(x)) {
                // x is not leaf node
                String childrenPrefix = getNextLevelOfPath(x, level);
                children.addAll(getAllByPrefix(childrenPrefix));
              }
            });
    if (!children.isEmpty()) {
      scanAllKeysRecursively(children, level + 1, op);
    }
  }

  private String getNextLevelOfPath(String innerPath, int currentLevel) {
    char levelChar = (char) (ZERO + currentLevel);
    String old = PATH_SEPARATOR + levelChar;
    String target = PATH_SEPARATOR + (char) (levelChar + 1);
    return innerPath.replace(old, target);
  }

  private String getNextLevelOfPath(String innerPath, char currentLevel) {
    String old = PATH_SEPARATOR + currentLevel;
    String target = PATH_SEPARATOR + (char) (currentLevel + 1);
    return innerPath.replace(old, target);
  }

  private PartialPath getPartialPathFromInnerPath(String path, int level) {
    char charLevel = (char) (ZERO + level);
    return getPartialPathFromInnerPath(path, charLevel);
  }

  private PartialPath getPartialPathFromInnerPath(String path, char level) {
    String pathWithoutLevel = path.replace(PATH_SEPARATOR + level, PATH_SEPARATOR);
    String[] nodes = pathWithoutLevel.split(ESCAPE_PATH_SEPARATOR);
    nodes[0] = PATH_ROOT;
    return new PartialPath(nodes);
  }

  private List<PartialPath> convertToPartialPath(Collection<String> paths, int level) {
    return paths
        .parallelStream()
        .map(x -> getPartialPathFromInnerPath(x, level))
        .collect(Collectors.toList());
  }

  private Set<String> getAllByPrefix(String prefix) {
    Set<String> result = new HashSet<>();
    byte[] prefixKey = prefix.getBytes();
    RocksIterator iterator = rocksDB.newIterator();
    for (iterator.seek(prefixKey); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      if (!key.startsWith(prefix)) {
        break;
      }
      result.add(key);
    }
    //    System.out.println(prefix + " " + result.size());
    return result;
  }

  public static void main(String[] args) throws RocksDBException, MetadataException, IOException {
    MRocksDBManager rocksDBManager = new MRocksDBManager();
    long start = System.currentTimeMillis();
    int count1 = rocksDBManager.getAllTimeseriesCount(null);
    System.out.println(
        "timeseries count: " + count1 + ", cost: " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    int count2 = rocksDBManager.getDevicesNum(null);
    System.out.println(
        "devices count: " + count2 + ", cost: " + (System.currentTimeMillis() - start));
    List<PartialPath> path = rocksDBManager.getNodesListInGivenLevel(null, 4);
    //    System.out.println("----------- nodes in level 2 --------");
    //    path.forEach(
    //        p -> {
    //          System.out.println(p.getFullPath());
    //        });
  }
}
