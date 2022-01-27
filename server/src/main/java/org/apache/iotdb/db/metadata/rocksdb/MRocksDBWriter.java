package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.Striped;
import org.apache.commons.lang3.StringUtils;
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
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;

public class MRocksDBWriter {
  private static final Logger logger = LoggerFactory.getLogger(MRocksDBManager.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final long MAX_LOCK_WAIT_TIME = 300;

  private static final String ROCKSDB_FOLDER = "rocksdb-schema";
  private static final String TABLE_NAME_STORAGE_GROUP = "storageGroupNodes".toLowerCase();
  private static final String TABLE_NAME_MEASUREMENT = "timeSeriesNodes".toLowerCase();
  private static final String TABLE_NAME_DEVICE = "deviceNodes".toLowerCase();
  private static final String[] INNER_TABLES =
      new String[] {
        new String(RocksDB.DEFAULT_COLUMN_FAMILY),
        TABLE_NAME_STORAGE_GROUP,
        TABLE_NAME_MEASUREMENT,
        TABLE_NAME_DEVICE
      };

  private static final String ROCKSDB_PATH =
      config.getSystemDir() + File.separator + ROCKSDB_FOLDER;

  private RocksDB rocksDB;

  ConcurrentMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();
  List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

  // TODO: check how Stripped Lock consume memory
  private Striped<Lock> locks = Striped.lazyWeakLock(10000);

  static {
    RocksDB.loadLibrary();
  }

  public MRocksDBWriter() throws MetadataException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setAllowMmapReads(true);
    options.setRowCache(new LRUCache(900000));
    options.setDbWriteBufferSize(16 * 1024 * 1024);

    org.rocksdb.Logger rocksDBLogger = new RockDBLogger(options, logger);
    rocksDBLogger.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
    options.setLogger(rocksDBLogger);

    DBOptions dbOptions = new DBOptions(options);

    try {
      initColumnFamilyDescriptors(options);

      rocksDB = RocksDB.open(dbOptions, ROCKSDB_PATH, columnFamilyDescriptors, columnFamilyHandles);

      initInnerColumnFamilies();

      initRootKey();
    } catch (RocksDBException e) {
      logger.error("init rocksdb fail", e);
      throw new MetadataException(e);
    }
  }

  public void init() throws MetadataException {
    // TODO: scan to init tag manager
    // TODO: warn up cache if needed
  }

  private void initColumnFamilyDescriptors(Options options) throws RocksDBException {
    List<byte[]> cfs = RocksDB.listColumnFamilies(options, ROCKSDB_PATH);
    for (byte[] tableBytes : cfs) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(tableBytes, new ColumnFamilyOptions()));
    }
  }

  private void initInnerColumnFamilies() throws RocksDBException {
    for (String tableNames : INNER_TABLES) {
      boolean tableCreated = false;
      for (ColumnFamilyHandle cfh : columnFamilyHandles) {
        if (tableNames.equals(new String(cfh.getName()))) {
          tableCreated = true;
          break;
        }
      }
      if (!tableCreated) {
        createTable(tableNames);
      }
    }
    for (ColumnFamilyHandle handle : columnFamilyHandles) {
      columnFamilyHandleMap.put(new String(handle.getName()), handle);
    }
  }

  private void initRootKey() throws RocksDBException {
    if (!keyExist(ROOT)) {
      rocksDB.put(ROOT.getBytes(), new byte[] {0x00});
    }
  }

  private void createTable(String tableName) throws RocksDBException {
    ColumnFamilyHandle columnFamilyHandle =
        rocksDB.createColumnFamily(
            new ColumnFamilyDescriptor(tableName.getBytes(), new ColumnFamilyOptions()));
    columnFamilyDescriptors.add(
        new ColumnFamilyDescriptor(tableName.getBytes(), new ColumnFamilyOptions()));
    columnFamilyHandles.add(columnFamilyHandle);
  }

  private ColumnFamilyHandle getCFHByName(String columnFamilyName) {
    return columnFamilyHandleMap.get(columnFamilyName);
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

  private void createNode(String key, byte[] value)
      throws RocksDBException, InterruptedException, MetadataException {
    Lock lock = locks.get(key);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (!keyExist(key)) {
          rocksDB.put(key.getBytes(), value);
        }
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + key);
    }
  }

  private void batchCreateNode(String lockKey, WriteBatch batch)
      throws RocksDBException, InterruptedException, MetadataException {
    Lock lock = locks.get(lockKey);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (!keyExist(lockKey)) {
          rocksDB.write(new WriteOptions(), batch);
        }
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + lockKey);
    }
  }

  private void batchCreateNode(String primaryKey, String aliasKey, WriteBatch batch)
      throws RocksDBException, MetadataException, InterruptedException {
    Lock primaryLock = locks.get(primaryKey);
    Lock aliasLock = locks.get(aliasKey);
    if (primaryLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (keyExist(primaryKey)) {
          throw new PathAlreadyExistException(primaryKey);
        }
        if (aliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            if (keyExist(aliasKey)) {
              throw new AliasAlreadyExistException(aliasKey, aliasKey);
            }
            rocksDB.write(new WriteOptions(), batch);
          } finally {
            aliasLock.unlock();
          }
        } else {
          throw new MetadataException("acquire lock timeout: " + aliasKey);
        }
      } finally {
        primaryLock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + primaryKey);
    }
  }

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    String[] nodes = storageGroup.getNodes();
    try {
      boolean sgExisted = false;
      for (int i = 0; i < nodes.length; i++) {
        String pathKey = constructKey(nodes, i);
        Holder<byte[]> holder = new Holder();
        if (!keyExist(pathKey, holder)) {
          if (!sgExisted) {
            if (i < nodes.length - 1) {
              createNode(pathKey, new byte[] {NODE_TYPE_INNER});
            } else {
              WriteBatch batch = new WriteBatch();
              byte[] key = pathKey.getBytes();
              batch.put(key, DEFAULT_SG_NODE_VALUE);
              batch.put(getCFHByName(TABLE_NAME_STORAGE_GROUP), key, EMPTY_NODE_VALUE);
              batchCreateNode(pathKey, batch);
            }
          } else {
            throw new StorageGroupAlreadySetException(storageGroup.getFullPath());
          }
        } else {
          byte[] value = holder.getValue();
          sgExisted = value.length > 0 && holder.getValue()[0] == NODE_TYPE_SG;
        }
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Add one timeseries to metadata, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
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
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        PartialPath storageGroupPath =
            MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
        setStorageGroup(storageGroupPath);
      }
      MeasurementSchema schema =
          new MeasurementSchema(nodes[nodes.length - 1], dataType, encoding, compressor, props);
      createTimeSeriesRecursive(nodes, nodes.length, alias, schema);
    } catch (RocksDBException | IOException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(
        plan.getPath(),
        plan.getDataType(),
        plan.getEncoding(),
        plan.getCompressor(),
        plan.getProps(),
        plan.getAlias());

    // TODO: persist tags and update tag index
  }

  private void createTimeSeriesRecursive(
      String nodes[], int start, String alias, MeasurementSchema schema)
      throws RocksDBException, IOException, MetadataException, InterruptedException {
    if (start < 1) {
      // "root" must exist
      return;
    }
    String key = constructKey(nodes, start - 1);
    if (!keyExist(key)) {
      createTimeSeriesRecursive(nodes, start - 1, alias, schema);
      byte[] value;
      if (start == nodes.length) {
        WriteBatch batch = new WriteBatch();
        value = buildMeasurementNodeValue(alias, schema);
        byte[] pathKey = key.getBytes();
        batch.put(pathKey, value);
        batch.put(getCFHByName(TABLE_NAME_MEASUREMENT), pathKey, EMPTY_NODE_VALUE);

        if (StringUtils.isNotEmpty(alias)) {
          String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
          aliasNodes[nodes.length - 1] = alias;
          String aliasKey = constructKey(aliasNodes, aliasNodes.length - 1);
          if (!keyExist(aliasKey)) {
            batch.put(
                aliasKey.getBytes(),
                Bytes.concat(new byte[] {DATA_VERSION, NODE_TYPE_ALIAS}, key.getBytes()));
            batchCreateNode(key, aliasKey, batch);
          } else {
            throw new AliasAlreadyExistException(key, alias);
          }
        } else {
          batchCreateNode(key, batch);
        }
      } else if (start == nodes.length - 1) {
        value = new byte[] {NODE_TYPE_ENTITY};
        createNodeTypeByTableName(TABLE_NAME_DEVICE, key, value);
      } else {
        value = new byte[] {NODE_TYPE_INNER};
      }
      createNode(key, value);
    }
  }

  private byte[] buildMeasurementNodeValue(String alias, MeasurementSchema schema)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(DATA_VERSION);
    outputStream.write(NODE_TYPE_MEASUREMENT);

    if (StringUtils.isNotEmpty(alias)) {
      outputStream.write(FLAG_HAS_ALIAS);
      byte[] data = RocksDBUtils.constructDataBlock(DATA_BLOCK_TYPE_ALIAS, alias);
      outputStream.write(data, 0, data.length);
    } else {
      outputStream.write(DEFAULT_FLAG);
    }

    schema.serializeTo(outputStream);

    return outputStream.toByteArray();
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

  private boolean keyExist(String key, Holder<byte[]> holder) throws RocksDBException {
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

  private boolean keyExist(String key) throws RocksDBException {
    return keyExist(key, new Holder<>());
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
      if (keyExist(key, holder)) {
        byte[] value = holder.getValue();
        return value.length > 0 && value[0] == NODE_TYPE_SG;
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
            if (value != null && value.length > 0 && value[0] == NODE_TYPE_MEASUREMENT) {
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
            if (value != null && value.length > 0 && value[0] == NODE_TYPE_ENTITY) {
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
    MRocksDBWriter rocksDBWriter = new MRocksDBWriter();
    long start = System.currentTimeMillis();
    int count1 = rocksDBWriter.getAllTimeseriesCount(null);
    System.out.println(
        "timeseries count: " + count1 + ", cost: " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    int count2 = rocksDBWriter.getDevicesNum(null);
    System.out.println(
        "devices count: " + count2 + ", cost: " + (System.currentTimeMillis() - start));
    List<PartialPath> path = rocksDBWriter.getNodesListInGivenLevel(null, 4);
    //    System.out.println("----------- nodes in level 2 --------");
    //    path.forEach(
    //        p -> {
    //          System.out.println(p.getFullPath());
    //        });
  }

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
}
