/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AcquireLockTimeoutException;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.metric.ISchemaRegionMetric;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowTimeSeriesResult;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegionParams;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegionUtils;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RMNodeType;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RMNodeValueType;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RMeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.collect.MapMaker;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.ALL_NODE_TYPE_ARRAY;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DEFAULT_ALIGNED_ENTITY_VALUE;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.DEFAULT_NODE_VALUE;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.FLAG_IS_ALIGNED;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_ALIAS;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.ROOT_STRING;
import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConstants.TABLE_NAME_TAGS;

@SchemaRegion(mode = "Rocksdb_based")
public class RSchemaRegion implements ISchemaRegion {

  private static final Logger logger = LoggerFactory.getLogger(RSchemaRegion.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // TODO: make it configurable
  public static final int MAX_PATH_DEPTH = 10;

  private static final long MAX_LOCK_WAIT_TIME = 50;

  private final RSchemaReadWriteHandler readWriteHandler;

  private final ReadWriteLock deleteUpdateLock = new ReentrantReadWriteLock();

  private final Map<String, ReentrantLock> locksPool =
      new MapMaker().weakValues().initialCapacity(10000).makeMap();

  private String schemaRegionDirPath;
  private String storageGroupFullPath;
  private SchemaRegionId schemaRegionId;
  private int storageGroupPathLevel;

  public RSchemaRegion() throws MetadataException {
    try {
      readWriteHandler = new RSchemaReadWriteHandler();
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  public RSchemaRegion(ISchemaRegionParams schemaRegionParams) throws MetadataException {
    RSchemaConfLoader rSchemaConfLoader = new RSchemaConfLoader();
    this.schemaRegionId = schemaRegionParams.getSchemaRegionId();
    storageGroupFullPath = schemaRegionParams.getDatabase().getFullPath();
    init();
    try {
      readWriteHandler = new RSchemaReadWriteHandler(schemaRegionDirPath, rSchemaConfLoader);
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  @Override
  public void init() throws MetadataException {
    schemaRegionDirPath =
        config.getSchemaDir()
            + File.separator
            + storageGroupFullPath
            + File.separator
            + schemaRegionId.getId();
    File schemaRegionFolder = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    if (!schemaRegionFolder.exists()) {
      if (schemaRegionFolder.mkdirs()) {
        logger.info("create schema region folder {}", schemaRegionDirPath);
      } else {
        if (!schemaRegionFolder.exists()) {
          logger.error("create schema region folder {} failed.", schemaRegionDirPath);
          throw new SchemaDirCreationFailureException(schemaRegionDirPath);
        }
      }
    }
    storageGroupPathLevel = RSchemaUtils.getLevelByPartialPath(storageGroupFullPath);
  }

  @Override
  public void forceMlog() {
    // do nothing
  }

  @Override
  public MemSchemaRegionStatistics getSchemaRegionStatistics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISchemaRegionMetric createSchemaRegionMetric() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  @Override
  public String getStorageGroupFullPath() {
    return storageGroupFullPath;
  }

  @Override
  public void deleteSchemaRegion() throws MetadataException {
    clear();
    SchemaRegionUtils.deleteSchemaRegionFolder(schemaRegionDirPath, logger);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    // todo implement this
    throw new UnsupportedOperationException(
        "Rocksdb mode currently doesn't support snapshot feature.");
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    // todo implement this
    throw new UnsupportedOperationException(
        "Rocksdb mode currently doesn't support snapshot feature.");
  }

  @Override
  public void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        createTimeseries(
            plan.getPath(),
            new MeasurementSchema(
                plan.getPath().getMeasurement(),
                plan.getDataType(),
                plan.getEncoding(),
                plan.getCompressor(),
                plan.getProps()),
            plan.getAlias(),
            plan.getTags(),
            plan.getAttributes());
        // update id table if id table log file is disabled
        if (config.isEnableIDTable() && !config.isEnableIDTableLogFile()) {
          IDTable idTable = IDTableManager.getInstance().getIDTable(plan.getPath().getDevicePath());
          idTable.createTimeseries(plan);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "Acquire lock timeout when creating timeseries: " + plan.getPath().getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @TestOnly
  protected void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    createTimeseries(path, dataType, encoding, compressor, props, null);
  }

  @TestOnly
  protected void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    createTimeseries(
        path,
        new MeasurementSchema(path.getMeasurement(), dataType, encoding, compressor, props),
        alias,
        null,
        null);
  }

  protected void createTimeseries(
      PartialPath path,
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws MetadataException {
    // regular check
    if (path.getNodes().length > RSchemaRegion.MAX_PATH_DEPTH) {
      throw new IllegalPathException(
          String.format(
              "path is too long, provide: %d, max: %d",
              path.getNodeLength(), RSchemaRegion.MAX_PATH_DEPTH));
    }
    MetaFormatUtils.checkTimeseries(path);
    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), schema.getProps());

    // sg check and create
    String[] nodes = path.getNodes();
    SchemaUtils.checkDataTypeWithEncoding(schema.getType(), schema.getEncodingType());

    try {
      createTimeSeriesRecursively(
          nodes, nodes.length, storageGroupPathLevel, schema, alias, tags, attributes);
      // TODO: load tags to memory
    } catch (RocksDBException | IOException e) {
      throw new MetadataException(e);
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private void createTimeSeriesRecursively(
      String[] nodes,
      int start,
      int end,
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws InterruptedException, MetadataException, RocksDBException, IOException {
    if (start <= end) {
      // "ROOT" node must exist and don't need to check
      return;
    }
    String levelPath = RSchemaUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath);
        if (!checkResult.existAnyKey()) {
          createTimeSeriesRecursively(nodes, start - 1, end, schema, alias, tags, attributes);
          if (start == nodes.length) {
            createTimeSeriesNode(nodes, levelPath, schema, alias, tags, attributes);
          } else if (start == nodes.length - 1) {
            readWriteHandler.createNode(levelPath, RMNodeType.ENTITY, DEFAULT_NODE_VALUE);
          } else {
            readWriteHandler.createNode(levelPath, RMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
          }
        } else {
          if (start == nodes.length) {
            throw new PathAlreadyExistException(RSchemaUtils.getPathByLevelPath(levelPath));
          }

          if (start == nodes.length - 1) {
            if (checkResult.getResult(RMNodeType.INTERNAL)) {
              // convert the parent node to entity if it is internal node
              readWriteHandler.convertToEntityNode(levelPath, DEFAULT_NODE_VALUE);
            } else if (checkResult.getResult(RMNodeType.ENTITY)) {
              if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
                throw new AlignedTimeseriesException(
                    "timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
                    RSchemaUtils.getPathByLevelPath(levelPath));
              }
            } else {
              throw new MNodeTypeMismatchException(
                  RSchemaUtils.getPathByLevelPath(levelPath), MetadataConstant.ENTITY_MNODE_TYPE);
            }
          }

          if (checkResult.getResult(RMNodeType.MEASUREMENT)
              || checkResult.getResult(RMNodeType.ALISA)) {
            throw new MNodeTypeMismatchException(
                RSchemaUtils.getPathByLevelPath(levelPath), MetadataConstant.INTERNAL_MNODE_TYPE);
          }
        }
      } finally {
        lock.unlock();
      }
    } else {
      lock.unlock();
      throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
    }
  }

  private void createTimeSeriesNode(
      String[] nodes,
      String levelPath,
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException, RocksDBException, MetadataException, InterruptedException {
    // create time-series node
    try (WriteBatch batch = new WriteBatch()) {
      byte[] value = RSchemaUtils.buildMeasurementNodeValue(schema, alias, tags, attributes);
      byte[] measurementKey = RSchemaUtils.toMeasurementNodeKey(levelPath);
      batch.put(measurementKey, value);

      // measurement with tags will save in a separate table at the same time
      if (tags != null && !tags.isEmpty()) {
        batch.put(
            readWriteHandler.getColumnFamilyHandleByName(TABLE_NAME_TAGS),
            measurementKey,
            DEFAULT_NODE_VALUE);
      }

      if (StringUtils.isNotEmpty(alias)) {
        String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
        aliasNodes[nodes.length - 1] = alias;
        String aliasLevelPath = RSchemaUtils.getLevelPath(aliasNodes, aliasNodes.length - 1);
        byte[] aliasNodeKey = RSchemaUtils.toAliasNodeKey(aliasLevelPath);
        Lock lock = locksPool.computeIfAbsent(aliasLevelPath, x -> new ReentrantLock());
        if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            if (!readWriteHandler.keyExistByAllTypes(aliasLevelPath).existAnyKey()) {
              batch.put(aliasNodeKey, RSchemaUtils.buildAliasNodeValue(measurementKey));
              readWriteHandler.executeBatch(batch);
            } else {
              throw new AliasAlreadyExistException(
                  RSchemaUtils.getPathByLevelPath(levelPath), alias);
            }
          } finally {
            lock.unlock();
          }
        } else {
          lock.unlock();
          throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
        }
      } else {
        readWriteHandler.executeBatch(batch);
      }
    }
  }

  private void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings)
      throws MetadataException {
    if (prefixPath.getNodeLength() > MAX_PATH_DEPTH - 1) {
      throw new IllegalPathException(
          String.format(
              "Prefix path is too long, provide: %d, max: %d",
              prefixPath.getNodeLength(), RSchemaRegion.MAX_PATH_DEPTH - 1));
    }

    MetaFormatUtils.checkTimeseries(prefixPath);

    for (int i = 0; i < measurements.size(); i++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      MetaFormatUtils.checkNodeName(measurements.get(i));
    }

    try (WriteBatch batch = new WriteBatch()) {
      createEntityRecursively(
          prefixPath.getNodes(), prefixPath.getNodeLength(), storageGroupPathLevel + 1, true);
      String[] locks = new String[measurements.size()];
      for (int i = 0; i < measurements.size(); i++) {
        String measurement = measurements.get(i);
        String levelPath = RSchemaUtils.getMeasurementLevelPath(prefixPath.getNodes(), measurement);
        locks[i] = levelPath;
        MeasurementSchema schema =
            new MeasurementSchema(measurement, dataTypes.get(i), encodings.get(i));
        byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
        byte[] value = RSchemaUtils.buildMeasurementNodeValue(schema, null, null, null);
        batch.put(key, value);
      }

      for (String lockKey : locks) {
        Lock lock = locksPool.computeIfAbsent(lockKey, x -> new ReentrantLock());
        if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            if (readWriteHandler.keyExistByAllTypes(lockKey).existAnyKey()) {
              throw new PathAlreadyExistException(lockKey);
            }
          } finally {
            lock.unlock();
          }
        } else {
          lock.unlock();
          throw new AcquireLockTimeoutException("acquire lock timeout: " + lockKey);
        }
      }
      readWriteHandler.executeBatch(batch);

      // TODO: update cache if necessary
    } catch (RocksDBException | IOException e) {
      throw new MetadataException(e);
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void createAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException {
    PartialPath prefixPath = plan.getDevicePath();
    List<String> measurements = plan.getMeasurements();
    List<TSDataType> dataTypes = plan.getDataTypes();
    List<TSEncoding> encodings = plan.getEncodings();

    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        createAlignedTimeSeries(prefixPath, measurements, dataTypes, encodings);
        // update id table if not in recovering or disable id table log file
        if (config.isEnableIDTable() && !config.isEnableIDTableLogFile()) {
          IDTable idTable = IDTableManager.getInstance().getIDTable(plan.getDevicePath());
          idTable.createAlignedTimeseries(plan);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "Acquire lock timeout when do createAlignedTimeSeries: " + prefixPath.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    throw new UnsupportedOperationException();
  }

  private void createEntityRecursively(String[] nodes, int start, int end, boolean aligned)
      throws RocksDBException, MetadataException, InterruptedException {
    if (start <= end) {
      // "ROOT" must exist
      return;
    }
    String levelPath = RSchemaUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath);
        if (!checkResult.existAnyKey()) {
          createEntityRecursively(nodes, start - 1, end, aligned);
          if (start == nodes.length) {
            byte[] nodeKey = RSchemaUtils.toEntityNodeKey(levelPath);
            byte[] value = aligned ? DEFAULT_ALIGNED_ENTITY_VALUE : DEFAULT_NODE_VALUE;
            readWriteHandler.createNode(nodeKey, value);
          } else {
            readWriteHandler.createNode(levelPath, RMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
          }
        } else {
          if (start == nodes.length) {
            // make sure sg node and entity node are different
            // eg.,'root.a' is a database path, 'root.a.b' can not be a timeseries
            if (checkResult.getResult(RMNodeType.STORAGE_GROUP)) {
              throw new MetadataException("Database Node and Entity Node could not be same!");
            }

            if (!checkResult.getResult(RMNodeType.ENTITY)) {
              throw new MNodeTypeMismatchException(
                  RSchemaUtils.getPathByLevelPath(levelPath), MetadataConstant.ENTITY_MNODE_TYPE);
            }

            if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) == 0) {
              throw new MetadataException(
                  "Timeseries under this entity is not aligned, please use createTimeseries or change entity. (Path: "
                      + RSchemaUtils.getPathByLevelPath(levelPath)
                      + ")");
            }
          } else if (checkResult.getResult(RMNodeType.MEASUREMENT)
              || checkResult.getResult(RMNodeType.ALISA)) {
            throw new MNodeTypeMismatchException(
                RSchemaUtils.getPathByLevelPath(levelPath), MetadataConstant.ENTITY_MNODE_TYPE);
          }
        }
      } finally {
        lock.unlock();
      }
    } else {
      lock.unlock();
      throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
    }
  }

  public Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    try {
      if (deleteUpdateLock.writeLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        Set<String> failedNames = ConcurrentHashMap.newKeySet();
        Set<IMNode> parentNeedsToCheck = ConcurrentHashMap.newKeySet();

        AtomicInteger atomicInteger = new AtomicInteger(0);
        traverseOutcomeBasins(
            pathPattern.getNodes(),
            MAX_PATH_DEPTH,
            (key, value) -> {
              String path = null;
              RMeasurementMNode deletedNode;
              try {
                path = RSchemaUtils.getPathByInnerName(new String(key));
                String[] nodes = PathUtils.splitPathToDetachedNodes(path);
                deletedNode = new RMeasurementMNode(path, value, readWriteHandler);
                atomicInteger.incrementAndGet();
                try (WriteBatch batch = new WriteBatch()) {
                  // delete the last node of path
                  batch.delete(key);
                  if (deletedNode.getAlias() != null) {
                    String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
                    aliasNodes[nodes.length - 1] = deletedNode.getAlias();
                    String aliasLevelPath =
                        RSchemaUtils.getLevelPath(aliasNodes, aliasNodes.length - 1);
                    batch.delete(RSchemaUtils.toAliasNodeKey(aliasLevelPath));
                  }
                  if (deletedNode.getTags() != null && !deletedNode.getTags().isEmpty()) {
                    batch.delete(
                        readWriteHandler.getColumnFamilyHandleByName(TABLE_NAME_TAGS), key);
                    // TODO: tags invert index update
                  }
                  readWriteHandler.executeBatch(batch);
                  if (!deletedNode.getParent().isStorageGroup()) {
                    parentNeedsToCheck.add(deletedNode.getParent());
                  }
                }
              } catch (Exception e) {
                logger.error("delete timeseries [{}] fail", path, e);
                failedNames.add(path);
                return false;
              }
              return true;
            },
            new Character[] {NODE_TYPE_MEASUREMENT});

        // delete parent without child after timeseries deleted
        while (true) {
          if (parentNeedsToCheck.isEmpty()) {
            break;
          }
          Set<IMNode> tempSet = ConcurrentHashMap.newKeySet();

          Stream<IMNode> parentStream = parentNeedsToCheck.parallelStream();
          parentStream.forEach(
              currentNode -> {
                if (!currentNode.isStorageGroup()) {
                  PartialPath parentPath = currentNode.getPartialPath();
                  int level = parentPath.getNodeLength();
                  int end = parentPath.getNodeLength() - 1;
                  if (!readWriteHandler.existAnySiblings(
                      RSchemaUtils.getLevelPathPrefix(parentPath.getNodes(), end, level))) {
                    try {
                      readWriteHandler.deleteNode(
                          parentPath.getNodes(), RSchemaUtils.typeOfMNode(currentNode));
                      IMNode parentNode = currentNode.getParent();
                      if (!parentNode.isStorageGroup()) {
                        tempSet.add(currentNode.getParent());
                      }
                    } catch (Exception e) {
                      logger.warn("delete {} fail.", parentPath.getFullPath(), e);
                    }
                  }
                }
              });
          parentNeedsToCheck.clear();
          parentNeedsToCheck.addAll(tempSet);
        }
        return new Pair<>(atomicInteger.get(), failedNames);
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when delete timeseries: " + pathPattern);
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
      return new Pair<>(0, null);
    } finally {
      deleteUpdateLock.writeLock().unlock();
    }
  }

  @Override
  public long constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  private void traverseOutcomeBasins(
      String[] nodes,
      int maxLevel,
      BiFunction<byte[], byte[], Boolean> function,
      Character[] nodeTypeArray)
      throws IllegalPathException {
    List<String[]> allNodesArray = RSchemaUtils.replaceMultiWildcardToSingle(nodes, maxLevel);
    allNodesArray.parallelStream().forEach(x -> traverseByPatternPath(x, function, nodeTypeArray));
  }

  private void traverseByPatternPath(
      String[] nodes, BiFunction<byte[], byte[], Boolean> function, Character[] nodeTypeArray) {

    int startIndex = 0;
    List<String[]> scanKeys = new ArrayList<>();

    int indexOfPrefix = indexOfFirstWildcard(nodes, startIndex);
    if (indexOfPrefix >= nodes.length) {
      Arrays.stream(nodeTypeArray)
          .parallel()
          .forEach(
              x -> {
                String levelPrefix =
                    RSchemaUtils.convertPartialPathToInnerByNodes(nodes, nodes.length - 1, x);
                try {
                  Holder<byte[]> holder = new Holder<>();
                  readWriteHandler.keyExist(levelPrefix.getBytes(), holder);
                  if (holder.getValue() != null) {
                    function.apply(levelPrefix.getBytes(), holder.getValue());
                  }
                } catch (RocksDBException e) {
                  logger.error(e.getMessage());
                }
              });
      return;
    }

    startIndex = indexOfPrefix;
    String[] seedPath = ArrayUtils.subarray(nodes, 0, indexOfPrefix);
    scanKeys.add(seedPath);

    while (!scanKeys.isEmpty()) {
      int firstNonWildcardIndex = indexOfFirstNonWildcard(nodes, startIndex);
      int nextFirstWildcardIndex = indexOfFirstWildcard(nodes, firstNonWildcardIndex);
      startIndex = nextFirstWildcardIndex;
      int level = nextFirstWildcardIndex - 1;

      boolean lastIteration = nextFirstWildcardIndex >= nodes.length;
      Character[] nodeType;
      if (!lastIteration) {
        nodeType = ALL_NODE_TYPE_ARRAY;
      } else {
        nodeType = nodeTypeArray;
      }

      Queue<String[]> tempNodes = new ConcurrentLinkedQueue<>();
      byte[] suffixToMatch =
          RSchemaUtils.getSuffixOfLevelPath(
              ArrayUtils.subarray(nodes, firstNonWildcardIndex, nextFirstWildcardIndex), level);
      Stream<String[]> scanKeysStream = scanKeys.parallelStream();
      scanKeysStream.forEach(
          prefixNodes -> {
            String levelPrefix =
                RSchemaUtils.getLevelPathPrefix(prefixNodes, prefixNodes.length - 1, level);
            Arrays.stream(nodeType)
                .parallel()
                .forEach(
                    x -> {
                      byte[] startKey = RSchemaUtils.toRocksDBKey(levelPrefix, x);
                      RocksIterator iterator = readWriteHandler.iterator(null);
                      iterator.seek(startKey);
                      while (iterator.isValid()) {
                        if (!RSchemaUtils.prefixMatch(iterator.key(), startKey)) {
                          break;
                        }
                        if (RSchemaUtils.suffixMatch(iterator.key(), suffixToMatch)) {
                          if (lastIteration) {
                            function.apply(iterator.key(), iterator.value());
                          } else {
                            tempNodes.add(RSchemaUtils.toMetaNodes(iterator.key()));
                          }
                        }
                        iterator.next();
                      }
                    });
          });
      scanKeys.clear();
      scanKeys.addAll(tempNodes);
      tempNodes.clear();
    }
  }

  private int indexOfFirstWildcard(String[] nodes, int start) {
    int index = start;
    for (; index < nodes.length; index++) {
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(nodes[index])
          || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(nodes[index])) {
        break;
      }
    }
    return index;
  }

  private int indexOfFirstNonWildcard(String[] nodes, int start) {
    int index = start;
    for (; index < nodes.length; index++) {
      if (!IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(nodes[index])
          && !IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(nodes[index])) {
        break;
      }
    }
    return index;
  }

  public long getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  public long getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getCountByNodeType(new Character[] {NODE_TYPE_ENTITY}, pathPattern.getNodes());
  }

  private int getCountByNodeType(Character[] nodetype, String[] nodes) throws IllegalPathException {
    AtomicInteger atomicInteger = new AtomicInteger(0);
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          atomicInteger.incrementAndGet();
          return true;
        };
    traverseOutcomeBasins(nodes, MAX_PATH_DEPTH, function, nodetype);
    return atomicInteger.get();
  }

  public Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    Map<PartialPath, Long> result = new ConcurrentHashMap<>();
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          String key = new String(a);
          String partialName = splitToPartialNameByLevel(key, level);
          if (partialName != null) {
            PartialPath path = null;
            try {
              path = new PartialPath(partialName);
            } catch (IllegalPathException e) {
              logger.warn(e.getMessage());
            }
            result.putIfAbsent(path, 0L);
            result.put(path, result.get(path) + 1);
          }
          return true;
        };
    traverseOutcomeBasins(
        pathPattern.getNodes(), MAX_PATH_DEPTH, function, new Character[] {NODE_TYPE_MEASUREMENT});

    return result;
  }

  private String splitToPartialNameByLevel(String innerName, int level) {
    StringBuilder stringBuilder = new StringBuilder(ROOT_STRING);
    boolean currentIsFlag;
    boolean lastIsFlag = false;
    int j = 0;
    for (int i = 0; i < innerName.length() && j <= level; i++) {
      currentIsFlag = innerName.charAt(i) == '.';
      if (currentIsFlag) {
        j++;
        currentIsFlag = true;
      }
      if (j <= 0 || lastIsFlag || (currentIsFlag && j > level)) {
        lastIsFlag = false;
        continue;
      }
      stringBuilder.append(innerName.charAt(i));
      lastIsFlag = currentIsFlag;
    }
    if (j < level) {
      return null;
    }
    return stringBuilder.toString();
  }

  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) {
    if (pathPattern.getFullPath().contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      throw new UnsupportedOperationException(
          formatNotSupportInfo(Thread.currentThread().getStackTrace()[1].getMethodName()));
    }
    return getNodesListInGivenLevel(pathPattern, nodeLevel);
  }

  private String formatNotSupportInfo(String methodName) {
    return String.format("[%s] is not currently supported!", methodName);
  }

  private List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel) {
    List<PartialPath> result = Collections.synchronizedList(new ArrayList<>());
    Arrays.stream(ALL_NODE_TYPE_ARRAY)
        .forEach(
            x -> {
              if (x == NODE_TYPE_ALIAS) {
                return;
              }
              String innerName =
                  RSchemaUtils.convertPartialPathToInnerByNodes(
                      pathPattern.getNodes(), nodeLevel, x);
              readWriteHandler
                  .getAllByPrefix(innerName)
                  .forEach(
                      resultByPrefix -> {
                        try {
                          result.add(
                              new PartialPath(RSchemaUtils.getPathByInnerName(resultByPrefix)));
                        } catch (IllegalPathException e) {
                          logger.warn(e.getMessage());
                        }
                      });
            });
    return result;
  }

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  private List<ShowTimeSeriesResult> showTimeseriesWithIndex(IShowTimeSeriesPlan plan) {
    // temporarily unsupported
    throw new UnsupportedOperationException(
        formatNotSupportInfo(Thread.currentThread().getStackTrace()[1].getMethodName()));
  }

  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(IShowTimeSeriesPlan plan)
      throws MetadataException {

    List<ShowTimeSeriesResult> res = new LinkedList<>();
    Map<MeasurementPath, Pair<Map<String, String>, Map<String, String>>> measurementPathsAndTags =
        getMatchedMeasurementPathWithTags(plan.getPath().getNodes());
    for (Entry<MeasurementPath, Pair<Map<String, String>, Map<String, String>>> entry :
        measurementPathsAndTags.entrySet()) {
      MeasurementPath measurementPath = entry.getKey();
      res.add(
          new ShowTimeSeriesResult(
              measurementPath.getFullPath(),
              measurementPath.getMeasurementAlias(),
              (MeasurementSchema) measurementPath.getMeasurementSchema(),
              entry.getValue().left,
              entry.getValue().right,
              measurementPath.isUnderAlignedEntity()));
    }

    return res;
  }

  @SuppressWarnings("unchecked")
  private Map<MeasurementPath, Pair<Map<String, String>, Map<String, String>>>
      getMatchedMeasurementPathWithTags(String[] nodes) throws IllegalPathException {
    Map<MeasurementPath, Pair<Map<String, String>, Map<String, String>>> allResult =
        new ConcurrentHashMap<>();
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          MeasurementPath measurementPath =
              new RMeasurementMNode(
                      RSchemaUtils.getPathByInnerName(new String(a)), b, readWriteHandler)
                  .getMeasurementPath();
          Object tag = RSchemaUtils.parseNodeValue(b, RMNodeValueType.TAGS);
          if (!(tag instanceof Map)) {
            tag = Collections.emptyMap();
          }
          Object attributes = RSchemaUtils.parseNodeValue(b, RMNodeValueType.ATTRIBUTES);
          if (!(attributes instanceof Map)) {
            attributes = Collections.emptyMap();
          }
          Map<String, String> tagMap = (Map<String, String>) tag;
          Map<String, String> attributesMap = (Map<String, String>) attributes;
          Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
              new Pair<>(tagMap, attributesMap);
          allResult.put(measurementPath, tagsAndAttributes);
          measurementPath.setTagMap(tagMap);
          return true;
        };
    traverseOutcomeBasins(nodes, MAX_PATH_DEPTH, function, new Character[] {NODE_TYPE_MEASUREMENT});

    return allResult;
  }

  IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = RSchemaUtils.getLevelPath(nodes, nodes.length - 1);
    IMeasurementMNode node = null;
    try {
      Holder<byte[]> holder = new Holder<>();
      if (readWriteHandler.keyExistByType(key, RMNodeType.MEASUREMENT, holder)) {
        node = new RMeasurementMNode(fullPath.getFullPath(), holder.getValue(), readWriteHandler);
      } else if (readWriteHandler.keyExistByType(key, RMNodeType.ALISA, holder)) {
        byte[] aliasValue = holder.getValue();
        if (aliasValue != null) {
          ByteBuffer byteBuffer = ByteBuffer.wrap(aliasValue);
          ReadWriteIOUtils.readBytes(byteBuffer, 3);
          byte[] oriKey = RSchemaUtils.readOriginKey(byteBuffer);
          node =
              new RMeasurementMNode(
                  fullPath.getFullPath(), readWriteHandler.get(null, oriKey), readWriteHandler);
        }
      }
      return node;
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void upsertAliasAndTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath path)
      throws MetadataException, IOException {
    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] originKey = RSchemaUtils.toMeasurementNodeKey(levelPath);
        try {
          Lock rawKeyLock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (rawKeyLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              boolean hasUpdate = false;
              String[] nodes = path.getNodes();
              RMeasurementMNode mNode = (RMeasurementMNode) getMeasurementMNode(path);
              // upsert alias
              if (StringUtils.isNotEmpty(alias)
                  && (StringUtils.isEmpty(mNode.getAlias()) || !mNode.getAlias().equals(alias))) {
                String oldAliasStr = mNode.getAlias();
                mNode.setAlias(alias);
                hasUpdate = true;
                String[] newAlias = Arrays.copyOf(nodes, nodes.length);
                newAlias[nodes.length - 1] = alias;
                String newAliasLevel = RSchemaUtils.getLevelPath(newAlias, newAlias.length - 1);
                byte[] newAliasKey = RSchemaUtils.toAliasNodeKey(newAliasLevel);
                Lock newAliasLock =
                    locksPool.computeIfAbsent(newAliasLevel, x -> new ReentrantLock());
                Lock oldAliasLock = null;
                try (WriteBatch batch = new WriteBatch()) {
                  if (newAliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                    if (readWriteHandler.keyExistByAllTypes(newAliasLevel).existAnyKey()) {
                      throw new PathAlreadyExistException("Alias node has exist: " + newAliasLevel);
                    }
                    batch.put(newAliasKey, RSchemaUtils.buildAliasNodeValue(originKey));
                    if (StringUtils.isNotEmpty(oldAliasStr)) {
                      String[] oldAliasNodes = Arrays.copyOf(nodes, nodes.length);
                      oldAliasNodes[nodes.length - 1] = oldAliasStr;
                      String oldAliasLevel =
                          RSchemaUtils.getLevelPath(oldAliasNodes, oldAliasNodes.length - 1);
                      byte[] oldAliasKey = RSchemaUtils.toAliasNodeKey(oldAliasLevel);
                      oldAliasLock =
                          locksPool.computeIfAbsent(oldAliasLevel, x -> new ReentrantLock());
                      if (oldAliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                        if (!readWriteHandler.keyExist(oldAliasKey)) {
                          logger.error(
                              "origin node [{}] has alias but alias node [{}] doesn't exist ",
                              levelPath,
                              oldAliasLevel);
                        }
                        batch.delete(oldAliasKey);
                      } else {
                        throw new AcquireLockTimeoutException(
                            "acquire lock timeout: " + oldAliasLevel);
                      }
                    }
                  } else {
                    throw new AcquireLockTimeoutException("acquire lock timeout: " + newAliasLevel);
                  }
                  // TODO: need application lock
                  readWriteHandler.executeBatch(batch);
                } finally {
                  newAliasLock.unlock();
                  if (oldAliasLock != null) {
                    oldAliasLock.unlock();
                  }
                }
              }

              try (WriteBatch batch = new WriteBatch()) {
                if (tagsMap != null && !tagsMap.isEmpty()) {
                  if (mNode.getTags() == null) {
                    mNode.setTags(tagsMap);
                  } else {
                    mNode.getTags().putAll(tagsMap);
                  }
                  batch.put(
                      readWriteHandler.getColumnFamilyHandleByName(TABLE_NAME_TAGS),
                      originKey,
                      DEFAULT_NODE_VALUE);
                  hasUpdate = true;
                }
                if (attributesMap != null && !attributesMap.isEmpty()) {
                  if (mNode.getAttributes() == null) {
                    mNode.setAttributes(attributesMap);
                  } else {
                    mNode.getAttributes().putAll(attributesMap);
                  }
                  hasUpdate = true;
                }
                if (hasUpdate) {
                  batch.put(originKey, mNode.getRocksDBValue());
                  readWriteHandler.executeBatch(batch);
                }
              }

            } finally {
              rawKeyLock.unlock();
            }
          } else {
            rawKeyLock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do upsertTagsAndAttributes: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath path)
      throws MetadataException, IOException {
    if (attributesMap == null || attributesMap.isEmpty()) {
      return;
    }

    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
        Holder<byte[]> holder = new Holder<>();
        try {
          Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              if (!readWriteHandler.keyExist(key, holder)) {
                throw new PathNotExistException(path.getFullPath());
              }

              byte[] originValue = holder.getValue();
              RMeasurementMNode mNode =
                  new RMeasurementMNode(path.getFullPath(), originValue, readWriteHandler);
              if (mNode.getAttributes() != null) {
                for (Map.Entry<String, String> entry : attributesMap.entrySet()) {
                  if (mNode.getAttributes().containsKey(entry.getKey())) {
                    throw new MetadataException(
                        String.format(
                            "TimeSeries [%s] already has the attribute [%s].",
                            path, new String(key)));
                  }
                }
                attributesMap.putAll(mNode.getAttributes());
              }
              mNode.setAttributes(attributesMap);
              readWriteHandler.updateNode(key, mNode.getRocksDBValue());
            } finally {
              lock.unlock();
            }
          } else {
            lock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do addAttributes: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath path)
      throws MetadataException, IOException {
    if (tagsMap == null || tagsMap.isEmpty()) {
      return;
    }

    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
        Holder<byte[]> holder = new Holder<>();
        try {
          Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              if (!readWriteHandler.keyExist(key, holder)) {
                throw new PathNotExistException(path.getFullPath());
              }
              byte[] originValue = holder.getValue();
              RMeasurementMNode mNode =
                  new RMeasurementMNode(path.getFullPath(), originValue, readWriteHandler);
              boolean hasTags = false;
              if (mNode.getTags() != null && mNode.getTags().size() > 0) {
                hasTags = true;
                for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
                  if (mNode.getTags().containsKey(entry.getKey())) {
                    throw new MetadataException(
                        String.format(
                            "TimeSeries [%s] already has the tag [%s].", path, new String(key)));
                  }
                  tagsMap.putAll(mNode.getTags());
                }
              }
              mNode.setTags(tagsMap);
              try (WriteBatch batch = new WriteBatch()) {
                if (!hasTags) {
                  batch.put(
                      readWriteHandler.getColumnFamilyHandleByName(TABLE_NAME_TAGS),
                      key,
                      DEFAULT_NODE_VALUE);
                }
                batch.put(key, mNode.getRocksDBValue());
                // TODO: need application lock
                readWriteHandler.executeBatch(batch);
              }
            } finally {
              lock.unlock();
            }
          } else {
            lock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do addTags: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath path)
      throws MetadataException, IOException {
    if (keySet == null || keySet.isEmpty()) {
      return;
    }
    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
        Holder<byte[]> holder = new Holder<>();
        try {
          Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              if (!readWriteHandler.keyExist(key, holder)) {
                throw new PathNotExistException(path.getFullPath());
              }

              byte[] originValue = holder.getValue();
              RMeasurementMNode mNode =
                  new RMeasurementMNode(path.getFullPath(), originValue, readWriteHandler);
              int tagLen = mNode.getTags() == null ? 0 : mNode.getTags().size();

              boolean didAnyUpdate = false;
              for (String toDelete : keySet) {
                didAnyUpdate = false;
                if (mNode.getTags() != null && mNode.getTags().containsKey(toDelete)) {
                  mNode.getTags().remove(toDelete);
                  didAnyUpdate = true;
                }
                if (mNode.getAttributes() != null && mNode.getAttributes().containsKey(toDelete)) {
                  mNode.getAttributes().remove(toDelete);
                  didAnyUpdate = true;
                }
                if (!didAnyUpdate) {
                  logger.warn("TimeSeries [{}] does not have tag/attribute [{}]", path, toDelete);
                }
              }
              if (didAnyUpdate) {
                try (WriteBatch batch = new WriteBatch()) {
                  if (tagLen > 0 && mNode.getTags().size() <= 0) {
                    batch.delete(
                        readWriteHandler.getColumnFamilyHandleByName(TABLE_NAME_TAGS), key);
                  }
                  batch.put(key, mNode.getRocksDBValue());
                  readWriteHandler.executeBatch(batch);
                }
              }
            } finally {
              lock.unlock();
            }
          } else {
            lock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do dropTagsOrAttributes: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath path)
      throws MetadataException, IOException {
    if (alterMap == null || alterMap.isEmpty()) {
      return;
    }

    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
        Holder<byte[]> holder = new Holder<>();
        try {
          Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              if (!readWriteHandler.keyExist(key, holder)) {
                throw new PathNotExistException(path.getFullPath());
              }
              byte[] originValue = holder.getValue();
              RMeasurementMNode mNode =
                  new RMeasurementMNode(path.getFullPath(), originValue, readWriteHandler);
              boolean didAnyUpdate = false;
              for (Map.Entry<String, String> entry : alterMap.entrySet()) {
                didAnyUpdate = false;
                if (mNode.getTags() != null && mNode.getTags().containsKey(entry.getKey())) {
                  mNode.getTags().put(entry.getKey(), entry.getValue());
                  didAnyUpdate = true;
                }
                if (mNode.getAttributes() != null
                    && mNode.getAttributes().containsKey(entry.getKey())) {
                  mNode.getAttributes().put(entry.getKey(), entry.getValue());
                  didAnyUpdate = true;
                }
                if (!didAnyUpdate) {
                  throw new MetadataException(
                      String.format(
                          "TimeSeries [%s] does not have tag/attribute [%s].",
                          path, new String(key)),
                      true);
                }
              }
              if (didAnyUpdate) {
                readWriteHandler.updateNode(key, mNode.getRocksDBValue());
              }
            } finally {
              lock.unlock();
            }
          } else {
            lock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do setTagsOrAttributesValue: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath path)
      throws MetadataException, IOException {
    if (StringUtils.isEmpty(oldKey) || StringUtils.isEmpty(newKey)) {
      return;
    }

    try {
      if (deleteUpdateLock.readLock().tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
        byte[] nodeKey = RSchemaUtils.toMeasurementNodeKey(levelPath);
        Holder<byte[]> holder = new Holder<>();
        try {
          Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              if (!readWriteHandler.keyExist(nodeKey, holder)) {
                throw new PathNotExistException(path.getFullPath());
              }
              byte[] originValue = holder.getValue();
              RMeasurementMNode mNode =
                  new RMeasurementMNode(path.getFullPath(), originValue, readWriteHandler);
              boolean didAnyUpdate = false;
              if (mNode.getTags() != null && mNode.getTags().containsKey(oldKey)) {
                String value = mNode.getTags().get(oldKey);
                mNode.getTags().remove(oldKey);
                mNode.getTags().put(newKey, value);
                didAnyUpdate = true;
              }

              if (mNode.getAttributes() != null && mNode.getAttributes().containsKey(oldKey)) {
                String value = mNode.getAttributes().get(oldKey);
                mNode.getAttributes().remove(oldKey);
                mNode.getAttributes().put(newKey, value);
                didAnyUpdate = true;
              }

              if (didAnyUpdate) {
                readWriteHandler.updateNode(nodeKey, mNode.getRocksDBValue());
              } else {
                throw new MetadataException(
                    String.format(
                        "TimeSeries [%s] does not have tag/attribute [%s].", path, oldKey),
                    true);
              }
            } finally {
              lock.unlock();
            }
          } else {
            lock.unlock();
            throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
          }
        } catch (RocksDBException e) {
          throw new MetadataException(e);
        }
      } else {
        throw new AcquireLockTimeoutException(
            "acquire lock timeout when do renameTagOrAttributeKey: " + path.getFullPath());
      }
    } catch (InterruptedException e) {
      logger.warn("Acquire lock interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      deleteUpdateLock.readLock().unlock();
    }
  }

  @Override
  public void clear() {
    try {
      readWriteHandler.close();
    } catch (RocksDBException e) {
      logger.error("Failed to close readWriteHandler,try again.", e);
      try {
        Thread.sleep(5);
        readWriteHandler.close();
      } catch (RocksDBException e1) {
        logger.error(String.format("This schemaRegion [%s] closed failed.", this), e);
      } catch (InterruptedException e1) {
        logger.warn("Close RocksdDB instance interrupted", e1);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackSchemaBlackListWithTemplate(IRollbackPreDeactivateTemplatePlan plan)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long countPathsUsingTemplate(int templateId, PathPatternTree patternTree)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      IShowTimeSeriesPlan showTimeSeriesPlan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("database:[%s]", storageGroupFullPath);
  }

  @TestOnly
  public void printScanAllKeys() throws IOException {
    readWriteHandler.scanAllKeys(schemaRegionDirPath + File.separator + "scanAllKeys.txt");
  }
}
