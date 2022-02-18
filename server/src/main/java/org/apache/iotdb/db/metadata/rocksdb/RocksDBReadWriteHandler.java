package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_ORIGIN_KEY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_SCHEMA;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_VERSION;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_FLAG;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.PATH_SEPARATOR;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.TABLE_NAME_TAGS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ZERO;

public class RocksDBReadWriteHandler {

  private static final Logger logger = LoggerFactory.getLogger(RocksDBReadWriteHandler.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String ROCKSDB_FOLDER = "rocksdb-schema";

  private static final String[] INNER_TABLES =
      new String[] {new String(RocksDB.DEFAULT_COLUMN_FAMILY), TABLE_NAME_TAGS};

  public static final String ROCKSDB_PATH = config.getSystemDir() + File.separator + ROCKSDB_FOLDER;

  private RocksDB rocksDB;

  private static RocksDBReadWriteHandler readWriteHandler;

  ConcurrentMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();
  List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

  static {
    RocksDB.loadLibrary();
  }

  private RocksDBReadWriteHandler() throws RocksDBException {
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
    if (cfs == null || cfs.size() <= 0) {
      cfs = new ArrayList<>();
      cfs.add(RocksDB.DEFAULT_COLUMN_FAMILY);
    }

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
    byte[] rootKey = RocksDBUtils.toRocksDBKey(ROOT, NODE_TYPE_ROOT);
    if (!keyExist(rootKey)) {
      rocksDB.put(rootKey, new byte[] {DATA_VERSION, DEFAULT_FLAG});
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

  public void updateNode(byte[] key, byte[] value) throws RocksDBException {
    rocksDB.put(key, value);
  }

  public void createNode(String key, RocksDBMNodeType type, byte[] value) throws RocksDBException {
    byte[] nodeKey = RocksDBUtils.toRocksDBKey(key, type.value);
    rocksDB.put(nodeKey, value);
  }

  public void createNode(byte[] nodeKey, byte[] value) throws RocksDBException {
    rocksDB.put(nodeKey, value);
  }

  public void convertToEntityNode(String levelPath, byte[] value) throws RocksDBException {
    WriteBatch batch = new WriteBatch();
    byte[] internalKey = RocksDBUtils.toInternalNodeKey(levelPath);
    byte[] entityKey = RocksDBUtils.toEntityNodeKey(levelPath);
    batch.delete(internalKey);
    batch.put(entityKey, value);
    executeBatch(batch);
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = RocksDBUtils.getLevelPath(nodes, nodes.length - 1);
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

  public boolean keyExistByType(String levelKey, RocksDBMNodeType type) throws RocksDBException {
    return keyExistByType(levelKey, type, new Holder<>());
  }

  public boolean keyExistByType(String levelKey, RocksDBMNodeType type, Holder<byte[]> holder)
      throws RocksDBException {
    byte[] key = RocksDBUtils.toRocksDBKey(levelKey, type.value);
    return keyExist(key, holder);
  }

  public CheckKeyResult keyExistByAllTypes(String levelKey) throws RocksDBException {
    return keyExistByAllTypes(levelKey, new Holder<>());
  }

  public CheckKeyResult keyExistByAllTypes(String levelKey, Holder<byte[]> holder)
      throws RocksDBException {
    RocksDBMNodeType[] types =
        new RocksDBMNodeType[] {
          RocksDBMNodeType.ALISA,
          RocksDBMNodeType.ENTITY,
          RocksDBMNodeType.INTERNAL,
          RocksDBMNodeType.MEASUREMENT,
          RocksDBMNodeType.STORAGE_GROUP
        };
    return keyExistByTypes(levelKey, holder, types);
  }

  public CheckKeyResult keyExistByTypes(String levelKey, RocksDBMNodeType... types)
      throws RocksDBException {
    return keyExistByTypes(levelKey, new Holder<>(), types);
  }

  public CheckKeyResult keyExistByTypes(
      String levelKey, Holder<byte[]> holder, RocksDBMNodeType... types) throws RocksDBException {
    // TODO: compare the performance between two methods
    CheckKeyResult result = new CheckKeyResult();
    for (RocksDBMNodeType type : types) {
      byte[] key = RocksDBUtils.toRocksDBKey(levelKey, type.value);
      if (keyExist(key, holder)) {
        result.setSingleCheckValue(type.value, keyExist(key, holder));
        break;
      }
    }
    return result;

    //    try {
    //      Arrays.stream(types)
    //          .parallel()
    //          .forEach(
    //              x -> {
    //                byte[] key = Bytes.concat(new byte[] {x.value}, levelKey.getBytes());
    //                try {
    //                  boolean keyExisted = keyExist(key, holder);
    //                  if (keyExisted) {
    //                    holder.getValue();
    //                    result.setSingleCheckValue(x.value, true);
    //                  }
    //                } catch (RocksDBException e) {
    //                  throw new RuntimeException(e);
    //                }
    //              });
    //    } catch (Exception e) {
    //      if (e.getCause() instanceof RocksDBException) {
    //        throw (RocksDBException) e.getCause();
    //      }
    //      throw e;
    //    }
    //    return result;
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
    return keyExist(key.getBytes(), holder);
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

  /**
   * Count the number of keys with the specified prefix in the specified column family
   *
   * @param columnFamilyHandle specified column family handle
   * @param nodeType specified prefix
   * @return total number in this column family
   */
  public long countNodesNumByType(ColumnFamilyHandle columnFamilyHandle, char nodeType) {
    RocksIterator iter;
    if (columnFamilyHandle == null) {
      iter = rocksDB.newIterator();
    } else {
      iter = rocksDB.newIterator(columnFamilyHandle);
    }
    long count = 0;
    for (iter.seek(String.valueOf(nodeType).getBytes()); iter.isValid(); iter.next()) {
      if (iter.key()[0] == nodeType) {
        count++;
      }
    }
    return count;
  }

  public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
    if (columnFamilyHandle == null) {
      return rocksDB.get(key);
    }
    return rocksDB.get(columnFamilyHandle, key);
  }

  public RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) {
    if (columnFamilyHandle == null) {
      return rocksDB.newIterator();
    }
    return rocksDB.newIterator(columnFamilyHandle);
  }

  public Set<String> getKeyByPrefix(String innerName) {
    RocksIterator iterator = rocksDB.newIterator();
    Set<String> result = new HashSet<>();
    for (iterator.seek(innerName.getBytes()); iterator.isValid(); iterator.next()) {
      String keyStr = new String(iterator.key());
      if (!keyStr.startsWith(innerName)) {
        break;
      }
      result.add(keyStr);
    }
    return result;
  }

  public Map<byte[], byte[]> getKeyValueByPrefix(String innerName) {
    RocksIterator iterator = rocksDB.newIterator();
    Map<byte[], byte[]> result = new HashMap<>();
    for (iterator.seek(innerName.getBytes()); iterator.isValid(); iterator.next()) {
      String keyStr = new String(iterator.key());
      if (!keyStr.startsWith(innerName)) {
        break;
      }
      result.put(iterator.key(), iterator.value());
    }
    return result;
  }

  public String findBelongToSpecifiedNodeType(String[] nodes, char nodeType) {
    String innerPathName;
    for (int level = nodes.length; level > 0; level--) {
      String[] copy = Arrays.copyOf(nodes, level);
      innerPathName = RocksDBUtils.convertPartialPathToInnerByNodes(copy, level, nodeType);
      boolean isBelongToType = rocksDB.keyMayExist(innerPathName.getBytes(), new Holder<>());
      if (isBelongToType) {
        return innerPathName;
      }
    }
    return null;
  }

  public void executeBatch(WriteBatch batch) throws RocksDBException {
    rocksDB.write(new WriteOptions(), batch);
  }

  public void deleteNode(String[] nodes, RocksDBMNodeType type) throws RocksDBException {
    byte[] key =
        RocksDBUtils.toRocksDBKey(
            RocksDBUtils.getLevelPath(nodes, nodes.length - 1), type.getValue());
    rocksDB.delete(key);
  }

  public void deleteNodeByPrefix(byte[] startKey, byte[] endKey) throws RocksDBException {
    rocksDB.deleteRange(startKey, endKey);
  }

  public void deleteNodeByPrefix(ColumnFamilyHandle handle, byte[] startKey, byte[] endKey)
      throws RocksDBException {
    rocksDB.deleteRange(handle, startKey, endKey);
  }

  @TestOnly
  public void scanAllKeys(String filePath) throws IOException {
    RocksIterator iterator = rocksDB.newIterator();
    System.out.println("\n-----------------scan rocksdb start----------------------");
    iterator.seekToFirst();
    File outputFile = new File(filePath);
    if (outputFile.exists()) {
      boolean deleted = outputFile.delete();
      System.out.println("delete output file: " + deleted);
    }
    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile));
    while (iterator.isValid()) {
      byte[] key = iterator.key();
      key[0] = (byte) (Integer.valueOf(key[0]) + '0');
      outputStream.write(key);
      outputStream.write(" -> ".getBytes());

      byte[] value = iterator.value();
      ByteBuffer byteBuffer = ByteBuffer.wrap(value);
      // skip the version flag and node type flag
      ReadWriteIOUtils.readBytes(byteBuffer, 2);
      while (byteBuffer.hasRemaining()) {
        byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
        switch (blockType) {
          case RockDBConstants.DATA_BLOCK_TYPE_TTL:
            long l = ReadWriteIOUtils.readLong(byteBuffer);
            outputStream.write(String.valueOf(l).getBytes());
            outputStream.write(" ".getBytes());
            break;
          case DATA_BLOCK_TYPE_SCHEMA:
            MeasurementSchema schema = MeasurementSchema.deserializeFrom(byteBuffer);
            outputStream.write(schema.toString().getBytes());
            outputStream.write(" ".getBytes());
            break;
          case RockDBConstants.DATA_BLOCK_TYPE_ALIAS:
            String str = ReadWriteIOUtils.readString(byteBuffer);
            outputStream.write(str.getBytes());
            outputStream.write(" ".getBytes());
            break;
          case DATA_BLOCK_TYPE_ORIGIN_KEY:
            byte[] originKey = RocksDBUtils.readOriginKey(byteBuffer);
            outputStream.write(originKey);
            outputStream.write(" ".getBytes());
            break;
          case RockDBConstants.DATA_BLOCK_TYPE_TAGS:
          case RockDBConstants.DATA_BLOCK_TYPE_ATTRIBUTES:
            Map<String, String> map = ReadWriteIOUtils.readMap(byteBuffer);
            for (Map.Entry<String, String> entry : map.entrySet()) {
              outputStream.write(("<" + entry.getKey() + "," + entry.getValue() + ">").getBytes());
            }
            outputStream.write(" ".getBytes());
            break;
          default:
            break;
        }
      }
      outputStream.write("\n".getBytes());
      iterator.next();
    }
    outputStream.close();
    System.out.println("\n-----------------scan rocksdb end----------------------");
  }

  @TestOnly
  public void close() {
    rocksDB.close();
  }

  public static RocksDBReadWriteHandler getInstance() throws RocksDBException {
    if (readWriteHandler != null) {
      return readWriteHandler;
    }
    return new RocksDBReadWriteHandler();
  }
}
