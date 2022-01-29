package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;

public class RocksDBReadWriteHandler {
  private static final Logger logger = LoggerFactory.getLogger(RocksDBReadWriteHandler.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final long MAX_LOCK_WAIT_TIME = 300;

  private static final String ROCKSDB_FOLDER = "rocksdb-schema";

  private static final String[] INNER_TABLES =
      new String[] {new String(RocksDB.DEFAULT_COLUMN_FAMILY), TABLE_NAME_TAGS};

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

  public RocksDBReadWriteHandler() throws RocksDBException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setAllowMmapReads(true);
    options.setRowCache(new LRUCache(900000));
    options.setDbWriteBufferSize(16 * 1024 * 1024);

    org.rocksdb.Logger rocksDBLogger = new RockDBLogger(options, logger);
    rocksDBLogger.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
    options.setLogger(rocksDBLogger);

    DBOptions dbOptions = new DBOptions(options);

    initColumnFamilyDescriptors(options);

    rocksDB = RocksDB.open(dbOptions, ROCKSDB_PATH, columnFamilyDescriptors, columnFamilyHandles);

    initInnerColumnFamilies();

    initRootKey();
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

  public ColumnFamilyHandle getCFHByName(String columnFamilyName) {
    return columnFamilyHandleMap.get(columnFamilyName);
  }

  public void updateNode(String key, byte[] value)
      throws MetadataException, InterruptedException, RocksDBException {
    Lock lock = locks.get(key);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        rocksDB.put(key.getBytes(), value);
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + key);
    }
  }

  public void createNode(String key, RocksDBMNodeType type, byte[] value)
      throws RocksDBException, InterruptedException, MetadataException {
    Lock lock = locks.get(key);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        byte[] nodeKey = RocksDBUtils.toRocksDBKey(key, type.value);
        if (keyExist(nodeKey)) {
          throw new PathAlreadyExistException(key);
        }
        rocksDB.put(nodeKey, value);
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + key);
    }
  }

  public void createNode(String key, byte[] nodeKey, byte[] value)
      throws RocksDBException, InterruptedException, MetadataException {
    Lock lock = locks.get(key);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (keyExist(nodeKey)) {
          throw new PathAlreadyExistException(key);
        }
        rocksDB.put(nodeKey, value);
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + key);
    }
  }

  public void convertToEntityNode(String lockKey, byte[] entityNodeKey, WriteBatch batch)
      throws InterruptedException, MetadataException {
    Lock lock = locks.get(lockKey);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (keyExist(entityNodeKey)) {
          throw new PathAlreadyExistException(lockKey);
        }
        // check exist of some key to make sure execute could success
        rocksDB.write(new WriteOptions(), batch);
      } catch (RocksDBException e) {
        e.printStackTrace();
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + lockKey);
    }
  }

  public void batchCreateTwoKeys(
      String primaryKey,
      String aliasKey,
      byte[] primaryNodeKey,
      byte[] aliasNodeKey,
      WriteBatch batch)
      throws RocksDBException, MetadataException, InterruptedException {
    Lock primaryLock = locks.get(primaryKey);
    Lock aliasLock = locks.get(aliasKey);
    if (primaryLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (keyExist(primaryNodeKey)) {
          throw new PathAlreadyExistException(primaryKey);
        }
        if (aliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            if (keyExist(aliasNodeKey)) {
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

  public void batchCreateOneKey(String key, byte[] nodeKey, WriteBatch batch)
      throws RocksDBException, MetadataException, InterruptedException {
    Lock lock = locks.get(key);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        if (keyExist(nodeKey)) {
          throw new PathAlreadyExistException(key);
        }
        rocksDB.write(new WriteOptions(), batch);
      } finally {
        lock.unlock();
      }
    } else {
      throw new MetadataException("acquire lock timeout: " + key);
    }
  }

  public byte[] buildMeasurementNodeValue(
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(DATA_VERSION, outputStream);

    byte flag = DEFAULT_FLAG;
    if (alias != null) {
      flag = (byte) (flag | FLAG_HAS_ALIAS);
    }

    if (tags != null && tags.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_TAGS);
    }

    if (attributes != null && attributes.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_ATTRIBUTES);
    }

    ReadWriteIOUtils.write(flag, outputStream);
    schema.serializeTo(outputStream);

    if (alias != null) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_ALIAS, outputStream);
      ReadWriteIOUtils.write(alias, outputStream);
    }

    if (tags != null && tags.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }

    if (attributes != null && attributes.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }
    return outputStream.toByteArray();
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = RocksDBUtils.constructKey(nodes, nodes.length - 1);
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

  public long countNodesNum(String tableName) {
    ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(tableName);
    RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
    long count = 0;
    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
      count++;
    }
    return count;
  }

  public boolean typeKyeExist(String levelKey, RocksDBMNodeType type) throws RocksDBException {
    byte[] key = Bytes.concat(new byte[] {type.value}, levelKey.getBytes());
    return keyExist(key, new Holder<>());
  }

  public boolean keyExist(String levelKey, RocksDBMNodeType type, Holder<byte[]> holder)
      throws RocksDBException {
    byte[] key = Bytes.concat(new byte[] {type.value}, levelKey.getBytes());
    return keyExist(key, holder);
  }

  public CheckKeyResult keyExist(String levelKey, RocksDBMNodeType... types)
      throws RocksDBException {
    return keyExist(levelKey, new Holder<>(), types);
  }

  public CheckKeyResult keyExist(String levelKey, Holder<byte[]> holder, RocksDBMNodeType... types)
      throws RocksDBException {
    CheckKeyResult result = new CheckKeyResult();
    try {
      Arrays.stream(types)
          .parallel()
          .forEach(
              x -> {
                byte[] key = Bytes.concat(new byte[] {x.value}, levelKey.getBytes());
                try {
                  result.setSingleCheckValue(x.value, keyExist(key, holder));
                } catch (RocksDBException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      if (e.getCause() instanceof RocksDBException) {
        throw (RocksDBException) e.getCause();
      }
      throw e;
    }
    return result;
  }

  public boolean keyExist(byte[] key, Holder<byte[]> holder) throws RocksDBException {
    boolean exist = false;
    if (!rocksDB.keyMayExist(key, holder)) {
      exist = false;
    } else {
      byte[] value = rocksDB.get(key);
      if (value != null) {
        exist = true;
        holder.setValue(value);
      }
    }
    return exist;
  }

  public boolean keyExist(byte[] key) throws RocksDBException {
    return keyExist(key, new Holder<>());
  }

  public boolean keyExist(String key, Holder<byte[]> holder) throws RocksDBException {
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

  public boolean keyExist(String key) throws RocksDBException {
    return keyExist(key, new Holder<>());
  }

  public void scanAllKeysRecursively(Set<String> seeds, int level, Function<String, Boolean> op) {
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
                String childrenPrefix = RocksDBUtils.getNextLevelOfPath(x, level);
                children.addAll(getAllByPrefix(childrenPrefix));
              }
            });
    if (!children.isEmpty()) {
      scanAllKeysRecursively(children, level + 1, op);
    }
  }

  public Set<String> getAllByPrefix(String prefix) {
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

  public static void main(String[] args) throws RocksDBException, MetadataException, IOException {
    MRocksDBWriter rocksDBWriter = new MRocksDBWriter();
    long start = System.currentTimeMillis();
    //    int count1 = getAllTimeseriesCount(null);
    //    System.out.println(
    //        "timeseries count: " + count1 + ", cost: " + (System.currentTimeMillis() - start));
    //    start = System.currentTimeMillis();
    //    int count2 = getDevicesNum(null);
    //    System.out.println(
    //        "devices count: " + count2 + ", cost: " + (System.currentTimeMillis() - start));
    //    List<PartialPath> path = rocksDBWriter.getNodesListInGivenLevel(null, 4);
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
