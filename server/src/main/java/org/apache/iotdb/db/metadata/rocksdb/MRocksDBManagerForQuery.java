package org.apache.iotdb.db.metadata.rocksdb;

import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

import com.google.common.util.concurrent.Striped;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
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

public class MRocksDBManagerForQuery {

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

  public MRocksDBManagerForQuery() throws RocksDBException {
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
        rocksDB.put(columnFamilyHandle, key.getBytes(), new byte[]{});
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
              createKey(normalKey, new byte[]{INNER_NODE_FLAG}, session);
            } else {
              createKey(normalKey, new byte[]{SG_NODE_FLAG}, session);
              createNodeTypeByTableName(
                  TABLE_NAME_STORAGE_GROUP, normalKey, new byte[]{SG_NODE_FLAG});
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
        value = new byte[]{MEASUREMENT_NODE_FLAG};
        createNodeTypeByTableName(TABLE_NAME_MEASUREMENT, key, value);
      } else if (start == nodes.length - 1) {
        value = new byte[]{DEVICE_NODE_FLAG};
        createNodeTypeByTableName(TABLE_NAME_DEVICE, key, value);
      } else {
        value = new byte[]{INNER_NODE_FLAG};
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

  private long countNodesNum(String tableName, String prefix) {
    ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(tableName);
    RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
    long count = 0;
    for (iter.seek(prefix.getBytes()); iter.isValid(); iter.next()) {
      String key = new String(iter.key());
      if (!key.startsWith(prefix)) {
        break;
      }
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

  /**
   * Check whether the given path contains a storage group
   */
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
   * @param nodeLevel   the level should match the level of the path
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

  /**
   * To calculate the count of devices for given path pattern.
   */
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

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard.
   */
  public int getAllStorageGroupCount(PartialPath pathPattern) throws MetadataException {
    AtomicInteger counter = new AtomicInteger(0);
    Set<String> seeds = new HashSet<>();
    seeds.add(ROOT + PATH_SEPARATOR + ZERO);
    scanAllKeysRecursively(
        seeds,
        0,
        s -> {
          try {
            byte[] value = rocksDB.get(s.getBytes());
            if (value != null && value.length > 0 && value[0] == SG_NODE_FLAG) {
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

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    String innerPathName = constructKey(path.getNodes(), path.getNodeLength());
    return rocksDB.keyMayExist(innerPathName.getBytes(), new Holder<>());
  }

  /**
   * Get metadata in string
   */
  public String getMetadataInString() {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  // todo mem count
  public long getTotalSeriesNumber() {
    return countNodesNum(TABLE_NAME_MEASUREMENT);
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  public long getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getTimeseriesCountByPrefix(pathPattern);
  }

  private int getTimeseriesCountByPrefix(PartialPath pathPattern) throws MetadataException {
    AtomicInteger counter = new AtomicInteger(0);
    Set<String> seeds = new HashSet<>();

    String seedPath;

    int nonWildcardAvailablePosition =
        pathPattern.getFullPath().indexOf(ONE_LEVEL_PATH_WILDCARD) - 1;
    if (nonWildcardAvailablePosition < 0) {
      seedPath = constructKey(pathPattern.getNodes(), pathPattern.getNodeLength());
    } else {
      seedPath = pathPattern.getFullPath().substring(0, nonWildcardAvailablePosition);
    }

    seeds.add(seedPath);
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

  /**
   * To calculate the count of devices for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public long getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return countNodesNum(TABLE_NAME_DEVICE, pathPattern.getFullPath());
  }

  /**
   * To calculate the count of storage group for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public long getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return countNodesNum(TABLE_NAME_STORAGE_GROUP, pathPattern.getFullPath());
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern. If using prefix
   * match, the path pattern is used to match prefix path. All timeseries start with the matched
   * prefix path will be counted.
   *
   * @param pathPattern   a path pattern or a full path
   * @param level         the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
//  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
//      throws MetadataException {
//    return mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
//  }

  /**
   * To calculate the count of nodes in the given level for given path pattern.
   *
   * @param pathPattern a path pattern or a full path
   * @param level       the level should match the level of the path
   */
//  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
//      throws MetadataException {
//    return getNodesCountInGivenLevel(pathPattern, level, false);
//  }

//  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
//      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
//    return mtree.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
//  }
//
//  public List<PartialPath> getNodesListInGivenLevel(
//      PartialPath pathPattern, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
//    return getNodesListInGivenLevel(pathPattern, nodeLevel);
//  }

  /**
   * Get child node in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2] given path = root.sg.d1 return [s1,s2]
   *
   * @return All child nodes of given seriesPath.
   */
//  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
//    return mtree.getChildNodeNameInNextLevel(pathPattern);
//  }
  public boolean isStorageGroup(PartialPath path) {
    int level = MRocksDBQueryUtils.getLevelByPartialPath(path.getFullPath());
    String innerPathName = MRocksDBQueryUtils
        .convertPartialPathToInner(path.getFullPath(), level, RockDBConstants.NODE_TYPE_SG);
    return rocksDB.keyMayExist(innerPathName.getBytes(), new Holder<>());
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path)
      throws StorageGroupNotSetException, IllegalPathException {
    String innerPathName = MRocksDBQueryUtils
        .findBelongToSpecifiedNodeType(path.getNodes(), rocksDB, RockDBConstants.NODE_TYPE_SG);
    if (innerPathName == null) {
      throw new StorageGroupNotSetException(
          String.format("Cannot find [%s] belong to which storage group.", path.getFullPath()));
    }
    return new PartialPath(MRocksDBQueryUtils.convertPartialPathToInner(innerPathName,
        MRocksDBQueryUtils.getLevelByPartialPath(innerPathName), RockDBConstants.NODE_TYPE_SG));
  }

  /**
   * Get the storage group that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all storage groups related to given path pattern
   */
//  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
//      throws MetadataException {
//    return mtree.getBelongedStorageGroups(pathPattern);
//  }

  /**
   * Get all storage group matching given path pattern. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be
   * collected.
   *
   * @param pathPattern   a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
//  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
//      throws MetadataException {
//    return mtree.getMatchedStorageGroups(pathPattern, isPrefixMatch);
//  }

  /**
   * Get all storage group paths
   */
  public List<PartialPath> getAllStorageGroupPaths() throws IllegalPathException {
    List<PartialPath> allStorageGroupPath = new ArrayList<>();
    RocksIterator iterator = rocksDB
        .newIterator(columnFamilyHandleMap.get(TABLE_NAME_STORAGE_GROUP));
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      allStorageGroupPath.add(convertInnerPathToPartialPath(new String(iterator.key())));
    }
    return allStorageGroupPath;
  }

  private PartialPath convertInnerPathToPartialPath(String innerPath) throws IllegalPathException {
    StringBuilder stringBuilder = new StringBuilder();
    char lastChar = '\u0019';
    for (char c : innerPath.toCharArray()) {
      if ('.' == lastChar) {
        lastChar = c;
        continue;
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    return new PartialPath(stringBuilder.toString());
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL()
      throws RocksDBException, IllegalPathException {
    List<String> allStorageGroupPath = new ArrayList<>();
    RocksIterator iterator = rocksDB
        .newIterator(columnFamilyHandleMap.get(TABLE_NAME_STORAGE_GROUP));
    // get all storage group path
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      allStorageGroupPath.add(new String(iterator.key()));
    }

    Map<PartialPath, Long> allStorageGroupAndTTL = new HashMap<>();
    // get ttl in default table by specifying path.
    for (String path : allStorageGroupPath) {
      byte[] value = rocksDB.get(path.getBytes());
      Object ttl = getValueByParse(ByteBuffer.wrap(value), RockDBConstants.FLAG_SET_TTL, true);
      // initialize a value
      if (ttl == null) {
        ttl = 0L;
      }
      allStorageGroupAndTTL.put(convertInnerPathToPartialPath(path), (Long) ttl);
    }
    return allStorageGroupAndTTL;
  }

  /**
   * parse value and return a specified type
   *
   * @param byteBuffer value written in default table
   * @param type       the type of value to obtain
   */
  private Object getValueByParse(ByteBuffer byteBuffer, byte type, boolean needSkipFlag) {
    if (needSkipFlag) {
      // skip the version flag and node type flag
      ReadWriteIOUtils.readByte(byteBuffer);
    }
    // get block type
    byte flag = ReadWriteIOUtils.readByte(byteBuffer);
    // this means that the following data contains the information we need
    if ((flag & type) > 0) {
      while (byteBuffer.hasRemaining()) {
        byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
        Object obj = new Object();
        switch (blockType) {
          case RockDBConstants.FLAG_SET_TTL:
            obj = ReadWriteIOUtils.readLong(byteBuffer);
            break;
          case RockDBConstants.FLAG_HAS_ALIAS:
            obj = ReadWriteIOUtils.readString(byteBuffer);
            break;
          case RockDBConstants.FLAG_HAS_TAGS:
          case RockDBConstants.FLAG_HAS_ATTRIBUTES:
            obj = ReadWriteIOUtils.readMap(byteBuffer);
            break;
          default:
            break;
        }
        // got the data we need,don't need to read any more
        if (type == blockType) {
          return obj;
        }
      }
    }
    return null;
  }

  /**
   * Get all devices that one of the timeseries, matching the given timeseries path pattern, belongs
   * to.
   *
   * @param timeseries a path pattern of the target timeseries
   * @return A HashSet instance which stores devices paths.
   */
//  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
//    return mtree.getDevicesByTimeseries(timeseries);
//  }

  /**
   * Get all device paths matching the path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern   the pattern of the target devices.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
//  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
//      throws MetadataException {
//    return mtree.getDevices(pathPattern, isPrefixMatch);
//  }

  /**
   * Get all device paths and according storage group paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult.
   */
//  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
//    return mtree.getDevices(plan);
//  }

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'. If using prefix match, the path pattern is used to match prefix path. All timeseries start
   * with the matched prefix path will be collected.
   *
   * @param pathPattern   can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
//  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
//      throws MetadataException {
//    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch).left;
//  }

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   */
//  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern)
//      throws MetadataException {
//    return getMeasurementPaths(pathPattern, false);
//  }

  /**
   * Similar to method getMeasurementPaths(), but return Path with alias and filter the result by
   * limit and offset. If using prefix match, the path pattern is used to match prefix path. All
   * timeseries start with the matched prefix path will be collected.
   *
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
//  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
//      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
//      throws MetadataException {
//    return mtree.getMeasurementPathsWithAlias(pathPattern, limit, offset, isPrefixMatch);
//  }
//
//  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
//      throws MetadataException {
//    // show timeseries with index
//    if (plan.getKey() != null && plan.getValue() != null) {
//      return showTimeseriesWithIndex(plan, context);
//    } else {
//      return showTimeseriesWithoutIndex(plan, context);
//    }
//  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
//  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
//    if (fullPath.equals(SQLConstant.TIME_PATH)) {
//      return TSDataType.INT64;
//    }
//    return getSeriesSchema(fullPath).getType();
//  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
//  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
//    if (fullPath.equals(SQLConstant.TIME_PATH)) {
//      return TSDataType.INT64;
//    }
//    return getSeriesSchema(fullPath).getType();
//  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
//  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
//    if (fullPath.equals(SQLConstant.TIME_PATH)) {
//      return TSDataType.INT64;
//    }
//    return getSeriesSchema(fullPath).getType();
//  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node by path. If storage group is not
   * set, StorageGroupNotSetException will be thrown
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException, RocksDBException {
    String innerPath = constructKey(path.getNodes(), path.getNodeLength());
    byte[] value = rocksDB.get(innerPath.getBytes());
    if (value == null) {
      throw new StorageGroupNotSetException(
          String.format("Can not find storage group by path : %s", path.getFullPath()));
    }
    if ((value[0] & RockDBConstants.NODE_TYPE_ENTITY_SG) != 0) {
      Object ttl = getValueByParse(ByteBuffer.wrap(value), RockDBConstants.FLAG_SET_TTL, true);
      if (ttl == null) {
        ttl = 0L;
      }
      return new StorageGroupEntityMNode(null,
          convertInnerPathToPartialPath(innerPath).getFullPath(), (Long) ttl);
    } else if ((value[0] & RockDBConstants.NODE_TYPE_SG) != 0) {
      Object ttl = getValueByParse(ByteBuffer.wrap(value), RockDBConstants.FLAG_SET_TTL, true);
      if (ttl == null) {
        ttl = 0L;
      }
      return new StorageGroupMNode(null, convertInnerPathToPartialPath(innerPath).getFullPath(),
          (Long) ttl);
    } else {
      throw new StorageGroupNotSetException(
          String.format("Cannot find the storage group by %s.", path.getFullPath()));
    }
  }

  /**
   * Get storage group node by path. the give path don't need to be storage group path.
   */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path)
      throws MetadataException, RocksDBException {
    String[] nodes = path.getNodes();
    String innerPathName = "";
    int i;
    for (i = nodes.length; i > 0; i--) {
      String[] copy = Arrays.copyOf(nodes, i);
      innerPathName = constructKey(copy, copy.length);
      boolean isStorageGroup = rocksDB
          .keyMayExist(columnFamilyHandleMap.get(TABLE_NAME_STORAGE_GROUP),
              innerPathName.getBytes(),
              new Holder<>());
      if (isStorageGroup) {
        break;
      }
    }
    byte[] value = rocksDB.get(innerPathName.getBytes());

    if ((value[0] & RockDBConstants.NODE_TYPE_ENTITY_SG) != 0) {
      Object ttl = getValueByParse(ByteBuffer.wrap(value), RockDBConstants.FLAG_SET_TTL, true);
      if (ttl == null) {
        ttl = 0L;
      }
      return new StorageGroupEntityMNode(null,
          convertInnerPathToPartialPath(innerPathName).getFullPath(), (Long) ttl);
    } else if ((value[0] & RockDBConstants.NODE_TYPE_SG) != 0) {
      Object ttl = getValueByParse(ByteBuffer.wrap(value), RockDBConstants.FLAG_SET_TTL, true);
      if (ttl == null) {
        ttl = 0L;
      }
      return new StorageGroupMNode(null, convertInnerPathToPartialPath(innerPathName).getFullPath(),
          (Long) ttl);
    } else {
      throw new StorageGroupNotSetException(
          String.format("Cannot find the storage group by %s.", path.getFullPath()));
    }
  }

  /**
   * Get all storage group MNodes
   */
  public List<IStorageGroupMNode> getAllStorageGroupNodes()
      throws MetadataException, RocksDBException {
    List<String> allStorageGroupPath = new ArrayList<>();
    RocksIterator iterator = rocksDB
        .newIterator(columnFamilyHandleMap.get(TABLE_NAME_STORAGE_GROUP));
    // get all storage group path
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      allStorageGroupPath.add(new String(iterator.key()));
    }
    List<IStorageGroupMNode> result = new ArrayList<>();
    for (String path : allStorageGroupPath) {
      result.add(getStorageGroupNodeByPath(convertInnerPathToPartialPath(path)));
    }
    return result;
  }

  public IMNode getDeviceNode(PartialPath path) throws MetadataException, RocksDBException {
    String[] nodes = path.getNodes();
    String innerPathName = "";
    int i;
    for (i = nodes.length; i > 0; i--) {
      String[] copy = Arrays.copyOf(nodes, i);
      innerPathName = constructKey(copy, copy.length);
      boolean isDevice = rocksDB
          .keyMayExist(columnFamilyHandleMap.get(TABLE_NAME_DEVICE),
              innerPathName.getBytes(),
              new Holder<>());
      if (isDevice) {
        break;
      }
    }
    byte[] value = rocksDB.get(innerPathName.getBytes());
    if ((value[0] & RockDBConstants.NODE_TYPE_ENTITY) != 0) {
      return new EntityMNode(null,
          convertInnerPathToPartialPath(innerPathName).getFullPath());
    } else {
      throw new StorageGroupNotSetException(
          String.format("Cannot find the storage group by %s.", path.getFullPath()));
    }
  }

  public IMeasurementMNode[] getMeasurementMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    IMeasurementMNode[] mNodes = new IMeasurementMNode[measurements.length];
    for (int i = 0; i < mNodes.length; i++) {
      try {
        mNodes[i] = getMeasurementMNode(deviceId.concatNode(measurements[i]));
      } catch (PathNotExistException | MNodeTypeMismatchException ignored) {
        logger.warn("MeasurementMNode {} does not exist in {}", measurements[i], deviceId);
      }
      if (mNodes[i] == null && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
        throw new MetadataException(measurements[i] + " does not exist in " + deviceId);
      }
    }
    return mNodes;
  }


  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws PathAlreadyExistException {
    IMNode result = deviceMNode.getChild(measurementName);
    if (result == null) {
      return null;
    }
    if (result.isMeasurement()) {
      return result.getAsMeasurementMNode();
    } else {
      throw new PathAlreadyExistException(
          deviceMNode.getFullPath() + PATH_SEPARATOR + measurementName);
    }
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
