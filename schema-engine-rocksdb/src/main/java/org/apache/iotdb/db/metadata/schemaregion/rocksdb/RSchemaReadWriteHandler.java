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

package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RMNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.Holder;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.ALL_NODE_TYPE_ARRAY;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DATA_BLOCK_TYPE_ORIGIN_KEY;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DATA_BLOCK_TYPE_SCHEMA;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DATA_VERSION;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DEFAULT_FLAG;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_ROOT;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.ROOT;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.TABLE_NAME_TAGS;

public class RSchemaReadWriteHandler {

  private static final Logger logger = LoggerFactory.getLogger(RSchemaReadWriteHandler.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String ROCKSDB_FOLDER = "rocksdb-schema";

  private static final String[] INNER_TABLES =
      new String[] {new String(RocksDB.DEFAULT_COLUMN_FAMILY), TABLE_NAME_TAGS};

  public static final String ROCKSDB_PATH = config.getSystemDir() + File.separator + ROCKSDB_FOLDER;

  private RocksDB rocksDB;

  private RSchemaConfLoader rSchemaConfLoader;

  ConcurrentMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();
  List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

  static {
    RocksDB.loadLibrary();
  }

  public RSchemaReadWriteHandler(String path, RSchemaConfLoader schemaConfLoader)
      throws RocksDBException {
    this.rSchemaConfLoader = schemaConfLoader;
    initReadWriteHandler(path);
  }

  public RSchemaReadWriteHandler() throws RocksDBException {
    initReadWriteHandler(ROCKSDB_PATH);
  }

  private void initReadWriteHandler(String path) throws RocksDBException {
    try (Options options = new Options()) {
      org.rocksdb.Logger rocksDBLogger = new RSchemaLogger(options, logger);
      rocksDBLogger.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);

      options
          .setCreateIfMissing(true)
          .setAllowMmapReads(true)
          .setWriteBufferSize(rSchemaConfLoader.getWriteBufferSize())
          .setMaxWriteBufferNumber(rSchemaConfLoader.getMaxWriteBufferNumber())
          .setMaxBackgroundJobs(rSchemaConfLoader.getMaxBackgroundJobs())
          .setStatistics(new Statistics())
          .setLogger(rocksDBLogger);

      final Filter bloomFilter = new BloomFilter(rSchemaConfLoader.getBloomFilterPolicy());

      final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
      Cache cache = new LRUCache(rSchemaConfLoader.getBlockCache(), 6);
      tableOptions
          .setBlockCache(cache)
          .setFilterPolicy(bloomFilter)
          .setBlockSizeDeviation(rSchemaConfLoader.getBlockSizeDeviation())
          .setBlockSize(rSchemaConfLoader.getBlockSize())
          .setBlockRestartInterval(rSchemaConfLoader.getBlockRestartInterval())
          .setCacheIndexAndFilterBlocks(true)
          .setBlockCacheCompressed(new LRUCache(rSchemaConfLoader.getBlockCacheCompressed(), 6));

      options.setTableFormatConfig(tableOptions);

      try (DBOptions dbOptions = new DBOptions(options)) {

        initColumnFamilyDescriptors(options, path);

        rocksDB = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);

        initInnerColumnFamilies();

        initRootKey();
      }
    }
  }

  private void initColumnFamilyDescriptors(Options options, String path) throws RocksDBException {
    List<byte[]> cfs = RocksDB.listColumnFamilies(options, path);
    if (cfs.isEmpty()) {
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
    byte[] rootKey = RSchemaUtils.toRocksDBKey(ROOT, NODE_TYPE_ROOT);
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

  public ColumnFamilyHandle getColumnFamilyHandleByName(String columnFamilyName) {
    return columnFamilyHandleMap.get(columnFamilyName);
  }

  public void updateNode(byte[] key, byte[] value) throws RocksDBException {
    rocksDB.put(key, value);
  }

  public void createNode(String levelKey, RMNodeType type, byte[] value) throws RocksDBException {
    byte[] nodeKey = RSchemaUtils.toRocksDBKey(levelKey, type.getValue());
    rocksDB.put(nodeKey, value);
  }

  public void createNode(byte[] nodeKey, byte[] value) throws RocksDBException {
    rocksDB.put(nodeKey, value);
  }

  public void convertToEntityNode(String levelPath, byte[] value) throws RocksDBException {
    try (WriteBatch batch = new WriteBatch()) {
      byte[] internalKey = RSchemaUtils.toInternalNodeKey(levelPath);
      byte[] entityKey = RSchemaUtils.toEntityNodeKey(levelPath);
      batch.delete(internalKey);
      batch.put(entityKey, value);
      executeBatch(batch);
    }
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = RSchemaUtils.getLevelPath(nodes, nodes.length - 1);
    try {
      byte[] value = rocksDB.get(key.getBytes());
      if (value == null) {
        logger.warn("path not exist: {}", key);
        throw new MetadataException("key not exist");
      }
      return new MeasurementMNode(null, fullPath.getFullPath(), null, null);
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  public boolean keyExistByType(String levelKey, RMNodeType type) throws RocksDBException {
    return keyExistByType(levelKey, type, new Holder<>());
  }

  public boolean keyExistByType(String levelKey, RMNodeType type, Holder<byte[]> holder)
      throws RocksDBException {
    byte[] key = RSchemaUtils.toRocksDBKey(levelKey, type.getValue());
    return keyExist(key, holder);
  }

  public CheckKeyResult keyExistByAllTypes(String levelKey) throws RocksDBException {
    RMNodeType[] types =
        new RMNodeType[] {
          RMNodeType.ALISA,
          RMNodeType.ENTITY,
          RMNodeType.INTERNAL,
          RMNodeType.MEASUREMENT,
          RMNodeType.STORAGE_GROUP
        };
    return keyExistByTypes(levelKey, types);
  }

  public CheckKeyResult keyExistByTypes(String levelKey, RMNodeType... types)
      throws RocksDBException {
    CheckKeyResult result = new CheckKeyResult();
    try {
      Arrays.stream(types)
          .forEach(
              x -> {
                byte[] key = Bytes.concat(new byte[] {(byte) x.getValue()}, levelKey.getBytes());
                try {
                  Holder<byte[]> holder = new Holder<>();
                  boolean keyExisted = keyExist(key, holder);
                  if (keyExisted) {
                    result.setExistType(x.getValue());
                    result.setValue(holder.getValue());
                  }
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
    if (rocksDB.keyMayExist(key, holder)) {
      if (holder.getValue() == null) {
        byte[] value = rocksDB.get(key);
        if (value != null) {
          exist = true;
          holder.setValue(value);
        }
      } else {
        exist = true;
      }
    }
    return exist;
  }

  public boolean keyExist(byte[] key) throws RocksDBException {
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
              if (Boolean.TRUE.equals(op.apply(x))) {
                // x is not leaf node
                String childrenPrefix = RSchemaUtils.getNextLevelOfPath(x, level);
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
    try (RocksIterator iterator = rocksDB.newIterator()) {
      for (iterator.seek(prefixKey); iterator.isValid(); iterator.next()) {
        String key = new String(iterator.key());
        if (!key.startsWith(prefix)) {
          break;
        }
        result.add(key);
      }
      return result;
    }
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

  public boolean existAnySiblings(String siblingPrefix) {
    for (char type : ALL_NODE_TYPE_ARRAY) {
      byte[] key = RSchemaUtils.toRocksDBKey(siblingPrefix, type);
      try (RocksIterator iterator = rocksDB.newIterator()) {
        for (iterator.seek(key); iterator.isValid(); iterator.next()) {
          if (!RSchemaUtils.prefixMatch(iterator.key(), key)) {
            break;
          } else {
            return true;
          }
        }
      }
    }
    return false;
  }

  public void getKeyByPrefix(String innerName, Function<String, Boolean> function) {
    try (RocksIterator iterator = rocksDB.newIterator()) {
      for (iterator.seek(innerName.getBytes()); iterator.isValid(); iterator.next()) {
        String keyStr = new String(iterator.key());
        if (!keyStr.startsWith(innerName)) {
          break;
        }
        function.apply(keyStr);
      }
    }
  }

  public Map<byte[], byte[]> getKeyValueByPrefix(String innerName) {
    try (RocksIterator iterator = rocksDB.newIterator()) {
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
  }

  public String findBelongToSpecifiedNodeType(String[] nodes, char nodeType) {
    String innerPathName;
    for (int level = nodes.length; level > 0; level--) {
      String[] copy = Arrays.copyOf(nodes, level);
      innerPathName = RSchemaUtils.convertPartialPathToInnerByNodes(copy, level, nodeType);
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

  public void deleteNode(String[] nodes, RMNodeType type) throws RocksDBException {
    byte[] key =
        RSchemaUtils.toRocksDBKey(
            RSchemaUtils.getLevelPath(nodes, nodes.length - 1), type.getValue());
    rocksDB.delete(key);
  }

  public void deleteByKey(byte[] key) throws RocksDBException {
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
    try (RocksIterator iterator = rocksDB.newIterator()) {
      logger.info("\n-----------------scan rocksdb start----------------------");
      iterator.seekToFirst();
      File outputFile = new File(filePath);
      if (outputFile.exists()) {
        boolean deleted = outputFile.delete();
        logger.info("delete output file: " + deleted);
      }
      try (BufferedOutputStream outputStream =
          new BufferedOutputStream(new FileOutputStream(outputFile))) {
        while (iterator.isValid()) {
          byte[] key = iterator.key();
          key[0] = (byte) (key[0] + '0');
          outputStream.write(key);
          outputStream.write(" -> ".getBytes());

          byte[] value = iterator.value();
          ByteBuffer byteBuffer = ByteBuffer.wrap(value);
          // skip the version flag and node type flag
          ReadWriteIOUtils.readBytes(byteBuffer, 2);
          while (byteBuffer.hasRemaining()) {
            byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
            switch (blockType) {
              case RSchemaConstants.DATA_BLOCK_TYPE_TTL:
                long l = ReadWriteIOUtils.readLong(byteBuffer);
                outputStream.write(String.valueOf(l).getBytes());
                outputStream.write(" ".getBytes());
                break;
              case DATA_BLOCK_TYPE_SCHEMA:
                MeasurementSchema schema = MeasurementSchema.deserializeFrom(byteBuffer);
                outputStream.write(schema.toString().getBytes());
                outputStream.write(" ".getBytes());
                break;
              case RSchemaConstants.DATA_BLOCK_TYPE_ALIAS:
                String str = ReadWriteIOUtils.readString(byteBuffer);
                outputStream.write(Objects.requireNonNull(str).getBytes());
                outputStream.write(" ".getBytes());
                break;
              case DATA_BLOCK_TYPE_ORIGIN_KEY:
                byte[] originKey = RSchemaUtils.readOriginKey(byteBuffer);
                outputStream.write(originKey);
                outputStream.write(" ".getBytes());
                break;
              case RSchemaConstants.DATA_BLOCK_TYPE_TAGS:
              case RSchemaConstants.DATA_BLOCK_TYPE_ATTRIBUTES:
                Map<String, String> map = ReadWriteIOUtils.readMap(byteBuffer);
                for (Map.Entry<String, String> entry : Objects.requireNonNull(map).entrySet()) {
                  outputStream.write(
                      ("<" + entry.getKey() + "," + entry.getValue() + ">").getBytes());
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
      }
      logger.info("\n-----------------scan rocksdb end----------------------");
    }
  }

  public void close() throws RocksDBException {
    rocksDB.syncWal();
    rocksDB.closeE();
  }
}
