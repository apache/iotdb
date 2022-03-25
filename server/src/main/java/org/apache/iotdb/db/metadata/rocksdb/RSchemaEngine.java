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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.*;
import org.apache.iotdb.db.metadata.ISchemaEngine;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rocksdb.mnode.*;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.google.common.collect.MapMaker;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import static org.apache.iotdb.commons.conf.IoTDBConstant.*;
import static org.apache.iotdb.db.metadata.rocksdb.RSchemaConstants.*;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is another implementation of ISchemaEngine. The default schema engine #{@link
 * SchemaEngine} is a pure memory implementation. In some situations, metadata can't fit into
 * memory, so we provide this alternative engine. The engine implemented base on RocksDB in which
 * all metadata all stored in a rocksdb instance. RocksDB has been proved to be a high performance
 * embed key-value storage. This implementation could achieve high throughput even with more than 1
 * billion time series(the time series often occupy more than 100GB footprint in our case).
 */
public class RSchemaEngine implements ISchemaEngine {

  private static final Logger logger = LoggerFactory.getLogger(RSchemaEngine.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // TODO: make it configurable
  public static int MAX_PATH_DEPTH = 10;

  private static final long MAX_LOCK_WAIT_TIME = 50;

  private final RSchemaReadWriteHandler readWriteHandler;

  private final Map<String, ReentrantLock> locksPool =
      new MapMaker().weakValues().initialCapacity(10000).makeMap();

  private final Map<String, Boolean> storageGroupDeletingFlagMap = new ConcurrentHashMap<>();

  public RSchemaEngine() throws MetadataException {
    try {
      readWriteHandler = new RSchemaReadWriteHandler();
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  @TestOnly
  public RSchemaEngine(String path) throws MetadataException {
    try {
      readWriteHandler = new RSchemaReadWriteHandler(path);
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  // region Interfaces and Implementation of MManager initialization、snapshot、recover and clear
  @Override
  public void init() {}

  @Override
  public void clear() {}

  @Override
  public void operation(PhysicalPlan plan) throws IOException, MetadataException {
    switch (plan.getOperatorType()) {
      case CREATE_TIMESERIES:
        CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
        createTimeseries(createTimeSeriesPlan);
        break;
      case CREATE_ALIGNED_TIMESERIES:
        CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
            (CreateAlignedTimeSeriesPlan) plan;
        createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        break;
      case DELETE_TIMESERIES:
        DeleteTimeSeriesPlan deleteTimeSeriesPlan = (DeleteTimeSeriesPlan) plan;
        // cause we only has one path for one DeleteTimeSeriesPlan
        deleteTimeseries(deleteTimeSeriesPlan.getPaths().get(0));
        break;
      case SET_STORAGE_GROUP:
        SetStorageGroupPlan setStorageGroupPlan = (SetStorageGroupPlan) plan;
        setStorageGroup(setStorageGroupPlan.getPath());
        break;
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupPlan deleteStorageGroupPlan = (DeleteStorageGroupPlan) plan;
        deleteStorageGroups(deleteStorageGroupPlan.getPaths());
        break;
      case TTL:
        SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
        setTTL(setTTLPlan.getStorageGroup(), setTTLPlan.getDataTTL());
        break;
      case CHANGE_ALIAS:
        ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        break;
      case AUTO_CREATE_DEVICE_MNODE:
        AutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan = (AutoCreateDeviceMNodePlan) plan;
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        break;
      case CHANGE_TAG_OFFSET:
      case CREATE_TEMPLATE:
      case DROP_TEMPLATE:
      case APPEND_TEMPLATE:
      case PRUNE_TEMPLATE:
      case SET_TEMPLATE:
      case ACTIVATE_TEMPLATE:
      case UNSET_TEMPLATE:
      case CREATE_CONTINUOUS_QUERY:
      case DROP_CONTINUOUS_QUERY:
        logger.error("unsupported operations {}", plan);
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete
  @Override
  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeSeries(
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
  }

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  @Override
  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    createTimeseries(path, dataType, encoding, compressor, props, null);
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
    createTimeSeries(
        path,
        new MeasurementSchema(path.getMeasurement(), dataType, encoding, compressor, props),
        alias,
        null,
        null);
  }

  protected void createTimeSeries(
      PartialPath path,
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws MetadataException {
    // regular check
    if (path.getNodes().length > RSchemaEngine.MAX_PATH_DEPTH) {
      throw new IllegalPathException(
          String.format(
              "path is too long, provide: %d, max: %d",
              path.getNodeLength(), RSchemaEngine.MAX_PATH_DEPTH));
    }
    MetaFormatUtils.checkTimeseries(path);
    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), schema.getProps());

    // sg check and create
    String[] nodes = path.getNodes();
    SchemaUtils.checkDataTypeWithEncoding(schema.getType(), schema.getEncodingType());
    int sgIndex = ensureStorageGroup(path);

    try {
      createTimeSeriesRecursively(
          nodes, nodes.length, sgIndex, schema, alias, tags, attributes, new Stack<>());
      // TODO: load tags to memory
    } catch (RocksDBException | InterruptedException | IOException e) {
      throw new MetadataException(e);
    }
  }

  private void createTimeSeriesRecursively(
      String[] nodes,
      int start,
      int end,
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes,
      Stack<Lock> lockedLocks)
      throws InterruptedException, MetadataException, RocksDBException, IOException {
    if (start <= end) {
      // nodes "root" must exist and don't need to check
      return;
    }
    String levelPath = RSchemaUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      lockedLocks.push(lock);
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath);
        if (!checkResult.existAnyKey()) {
          createTimeSeriesRecursively(
              nodes, start - 1, end, schema, alias, tags, attributes, lockedLocks);
          if (start == nodes.length) {
            createTimeSeriesNode(nodes, levelPath, schema, alias, tags, attributes);
          } else if (start == nodes.length - 1) {
            readWriteHandler.createNode(levelPath, RMNodeType.ENTITY, DEFAULT_NODE_VALUE);
          } else {
            readWriteHandler.createNode(levelPath, RMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
          }
        } else {
          if (start == nodes.length) {
            throw new PathAlreadyExistException(levelPath);
          }

          if (checkResult.getResult(RMNodeType.MEASUREMENT)
              || checkResult.getResult(RMNodeType.ALISA)) {
            throw new PathAlreadyExistException(levelPath);
          }

          if (start == nodes.length - 1) {
            if (checkResult.getResult(RMNodeType.INTERNAL)) {
              // convert the parent node to entity if it is internal node
              readWriteHandler.convertToEntityNode(levelPath, DEFAULT_NODE_VALUE);
            } else if (checkResult.getResult(RMNodeType.ENTITY)) {
              if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
                throw new AlignedTimeseriesException(
                    "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
                    RSchemaUtils.getPathByLevelPath(levelPath));
              }
            } else {
              throw new MetadataException(
                  "parent of measurement could only be entity or internal node");
            }
          }
        }
      } catch (Exception e) {
        while (!lockedLocks.isEmpty()) {
          lockedLocks.pop().unlock();
        }
        throw e;
      } finally {
        if (!lockedLocks.isEmpty()) {
          lockedLocks.pop().unlock();
        }
      }
    } else {
      while (!lockedLocks.isEmpty()) {
        lockedLocks.pop().unlock();
      }
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
            readWriteHandler.getCFHByName(TABLE_NAME_TAGS), measurementKey, DEFAULT_NODE_VALUE);
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
              throw new AliasAlreadyExistException(levelPath, alias);
            }
          } finally {
            lock.unlock();
          }
        } else {
          throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
        }
      } else {
        readWriteHandler.executeBatch(batch);
      }
    }
  }

  @Override
  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    PartialPath prefixPath = plan.getPrefixPath();
    List<String> measurements = plan.getMeasurements();
    List<TSDataType> dataTypes = plan.getDataTypes();
    List<TSEncoding> encodings = plan.getEncodings();

    if (prefixPath.getNodeLength() > MAX_PATH_DEPTH - 1) {
      throw new IllegalPathException(
          String.format(
              "Prefix path is too long, provide: %d, max: %d",
              prefixPath.getNodeLength(), RSchemaEngine.MAX_PATH_DEPTH - 1));
    }

    MetaFormatUtils.checkTimeseries(prefixPath);

    for (int i = 0; i < measurements.size(); i++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      MetaFormatUtils.checkNodeName(measurements.get(i));
    }

    int sgIndex = ensureStorageGroup(prefixPath);

    try (WriteBatch batch = new WriteBatch()) {
      createEntityRecursively(
          prefixPath.getNodes(), prefixPath.getNodeLength(), sgIndex + 1, true, new Stack<>());
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

      Stack<Lock> acquiredLock = new Stack<>();
      try {
        for (String lockKey : locks) {
          Lock lock = locksPool.computeIfAbsent(lockKey, x -> new ReentrantLock());
          if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            acquiredLock.push(lock);
            if (readWriteHandler.keyExistByAllTypes(lockKey).existAnyKey()) {
              throw new PathAlreadyExistException(lockKey);
            }
          } else {
            throw new AcquireLockTimeoutException("acquire lock timeout: " + lockKey);
          }
        }
        readWriteHandler.executeBatch(batch);
      } finally {
        while (!acquiredLock.isEmpty()) {
          Lock lock = acquiredLock.pop();
          lock.unlock();
        }
      }
      // TODO: update cache if necessary
    } catch (InterruptedException | RocksDBException | IOException e) {
      throw new MetadataException(e);
    }
  }

  /** The method assume Storage Group Node has been created */
  private void createEntityRecursively(
      String[] nodes, int start, int end, boolean aligned, Stack<Lock> lockedLocks)
      throws RocksDBException, MetadataException, InterruptedException {
    if (start <= end) {
      // nodes before "end" must exist
      return;
    }
    String levelPath = RSchemaUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath);
        if (!checkResult.existAnyKey()) {
          createEntityRecursively(nodes, start - 1, end, aligned, lockedLocks);
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
            // eg.,'root.a' is a storage group path, 'root.a.b' can not be a timeseries
            if (checkResult.getResult(RMNodeType.STORAGE_GROUP)) {
              throw new MetadataException("Storage Group Node and Entity Node could not be same!");
            }

            if (!checkResult.getResult(RMNodeType.ENTITY)) {
              throw new MetadataException(
                  "Node already exists but not entity. (Path: "
                      + RSchemaUtils.getPathByLevelPath(levelPath)
                      + ")");
            }

            if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) == 0) {
              throw new MetadataException(
                  "Timeseries under this entity is not aligned, please use createTimeseries or change entity. (Path: "
                      + RSchemaUtils.getPathByLevelPath(levelPath)
                      + ")");
            }
          } else if (checkResult.getResult(RMNodeType.MEASUREMENT)
              || checkResult.getResult(RMNodeType.ALISA)) {
            throw new MetadataException("Path contains measurement node");
          }
        }
      } catch (Exception e) {
        while (!lockedLocks.isEmpty()) {
          lockedLocks.pop().unlock();
        }
        throw e;
      } finally {
        if (!lockedLocks.isEmpty()) {
          lockedLocks.pop().unlock();
        }
      }
    } else {
      while (!lockedLocks.isEmpty()) {
        lockedLocks.pop().unlock();
      }
      throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
    }
  }

  /**
   * delete timeseries which match with pathPattern.
   *
   * @param pathPattern
   * @param isPrefixMatch
   * @return
   * @throws MetadataException
   */
  @Override
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    Set<String> failedNames = ConcurrentHashMap.newKeySet();
    Map<IStorageGroupMNode, Set<IMNode>> mapToDeletedPath = new ConcurrentHashMap<>();
    traverseOutcomeBasins(
        pathPattern.getNodes(),
        MAX_PATH_DEPTH,
        (key, value) -> {
          String path = null;
          RMeasurementMNode deletedNode;
          try {
            path = RSchemaUtils.getPathByInnerName(new String(key));
            String[] nodes = MetaUtils.splitPathToDetachedPath(path);
            String levelPath = RSchemaUtils.getLevelPath(nodes, nodes.length - 1);
            // Delete measurement node
            Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
            if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
              try {
                deletedNode = new RMeasurementMNode(path, value, readWriteHandler);
                WriteBatch batch = new WriteBatch();
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
                  batch.delete(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), key);
                  // TODO: tags invert index update
                }
                readWriteHandler.executeBatch(batch);
                IStorageGroupMNode mNode =
                    getStorageGroupNodeByPath(
                        new PartialPath(RSchemaUtils.getPathByLevelPath(levelPath)));
                mapToDeletedPath
                    .computeIfAbsent(mNode, s -> ConcurrentHashMap.newKeySet())
                    .add(deletedNode.getParent());
              } finally {
                lock.unlock();
              }
            } else {
              throw new AcquireLockTimeoutException("acquire lock timeout, " + path);
            }
          } catch (IllegalPathException e) {
          } catch (Exception e) {
            logger.error("delete timeseries [{}] fail", path, e);
            failedNames.add(path);
            return false;
          }
          return true;
        },
        new Character[] {NODE_TYPE_MEASUREMENT});

    // TODO: do we need to delete parent??
    while (true) {
      if (mapToDeletedPath.isEmpty()) {
        break;
      }
      Map<IStorageGroupMNode, Set<IMNode>> tempMap = new ConcurrentHashMap<>();
      mapToDeletedPath
          .keySet()
          .parallelStream()
          .forEach(
              sgNode -> {
                if (storageGroupDeletingFlagMap.getOrDefault(sgNode.getFullPath(), false)) {
                  // deleting the storage group, ignore parent delete
                  return;
                }
                try {
                  // lock the responding storage group
                  storageGroupDeletingFlagMap.put(sgNode.getFullPath(), true);
                  // wait for all executing createTimeseries operations are complete
                  Thread.sleep(MAX_LOCK_WAIT_TIME * MAX_PATH_DEPTH);
                  mapToDeletedPath
                      .get(sgNode)
                      .parallelStream()
                      .forEach(
                          parentNode -> {
                            if (!parentNode.isStorageGroup()) {
                              PartialPath parentPath = parentNode.getPartialPath();
                              int level = parentPath.getNodeLength();
                              int end = parentPath.getNodeLength() - 1;
                              if (!readWriteHandler.existAnySiblings(
                                  RSchemaUtils.getLevelPathPrefix(
                                      parentPath.getNodes(), end, level))) {
                                try {
                                  readWriteHandler.deleteNode(
                                      parentPath.getNodes(), RSchemaUtils.typeOfMNode(parentNode));
                                  tempMap
                                      .computeIfAbsent(sgNode, k -> ConcurrentHashMap.newKeySet())
                                      .add(parentNode.getParent());
                                } catch (Exception e) {
                                  logger.error("delete {} fail.", parentPath.getFullPath(), e);
                                  failedNames.add(parentPath.getFullPath());
                                }
                              }
                            }
                          });

                } catch (Exception e) {

                } finally {
                  storageGroupDeletingFlagMap.remove(sgNode.getFullPath());
                }
              });
      mapToDeletedPath.clear();
      mapToDeletedPath.putAll(tempMap);
    }

    return failedNames.isEmpty() ? null : String.join(",", failedNames);
  }

  @Override
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    return deleteTimeseries(pathPattern, false);
  }
  // endregion

  // region Interfaces and Implementation for StorageGroup and TTL operation
  // including sg set and delete, and ttl set
  private int ensureStorageGroup(PartialPath path) throws MetadataException {
    int sgIndex = -1;
    String[] nodes = path.getNodes();
    try {
      sgIndex = indexOfSgNode(nodes);
      if (sgIndex < 0) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        PartialPath sgPath =
            MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
        setStorageGroup(sgPath);
        sgIndex = sgPath.getNodeLength() - 1;
      }
      String sgPath =
          String.join(PATH_SEPARATOR, ArrayUtils.subarray(path.getNodes(), 0, sgIndex + 1));
      if (storageGroupDeletingFlagMap.getOrDefault(sgPath, false)) {
        throw new MetadataException("Storage Group is deleting: " + sgPath);
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
    return sgIndex;
  }

  private int indexOfSgNode(String[] nodes) throws RocksDBException {
    int result = -1;
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RSchemaUtils.getLevelPath(nodes, i);
      if (readWriteHandler.keyExistByType(levelPath, RMNodeType.STORAGE_GROUP)) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  @Override
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    if (storageGroup.getNodes().length > RSchemaEngine.MAX_PATH_DEPTH) {
      throw new IllegalPathException(
          String.format(
              "Storage group path is too long, provide: %d, max: %d",
              storageGroup.getNodeLength(), RSchemaEngine.MAX_PATH_DEPTH - 2));
    }
    MetaFormatUtils.checkStorageGroup(storageGroup.getFullPath());
    String[] nodes = storageGroup.getNodes();
    try {
      int len = nodes.length;
      for (int i = 1; i < nodes.length; i++) {
        String levelKey = RSchemaUtils.getLevelPath(nodes, i);
        Lock lock = locksPool.computeIfAbsent(levelKey, x -> new ReentrantLock());
        if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            CheckKeyResult keyCheckResult = readWriteHandler.keyExistByAllTypes(levelKey);
            if (!keyCheckResult.existAnyKey()) {
              if (i < len - 1) {
                readWriteHandler.createNode(levelKey, RMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
              } else {
                readWriteHandler.createNode(levelKey, RMNodeType.STORAGE_GROUP, DEFAULT_NODE_VALUE);
              }
            } else {
              if (i >= len - 1) {
                if (keyCheckResult.getExistType() == RMNodeType.STORAGE_GROUP) {
                  throw new StorageGroupAlreadySetException(storageGroup.getFullPath());
                } else {
                  throw new MNodeTypeMismatchException(
                      storageGroup.getFullPath(), (byte) (RMNodeType.STORAGE_GROUP.getValue() - 1));
                }
              } else {
                if (keyCheckResult.getExistType() != RMNodeType.INTERNAL) {
                  throw new StorageGroupAlreadySetException(
                      RSchemaUtils.concatNodesName(nodes, 0, i), true);
                }
              }
            }
          } finally {
            lock.unlock();
          }
        } else {
          throw new AcquireLockTimeoutException("acquire lock timeout: " + levelKey);
        }
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    storageGroups
        .parallelStream()
        .forEach(
            path -> {
              try {
                storageGroupDeletingFlagMap.put(path.getFullPath(), true);
                // wait for all executing createTimeseries operations are complete
                Thread.sleep(MAX_LOCK_WAIT_TIME * MAX_PATH_DEPTH);
                String[] nodes = path.getNodes();
                Arrays.stream(ALL_NODE_TYPE_ARRAY)
                    .parallel()
                    .forEach(
                        type -> {
                          try {
                            for (int i = nodes.length; i <= MAX_PATH_DEPTH; i++) {
                              String startPath =
                                  RSchemaUtils.getLevelPathPrefix(nodes, nodes.length - 1, i);
                              byte[] startKey = RSchemaUtils.toRocksDBKey(startPath, type);
                              byte[] endKey = new byte[startKey.length];
                              System.arraycopy(startKey, 0, endKey, 0, startKey.length - 1);
                              endKey[endKey.length - 1] = Byte.MAX_VALUE;
                              if (type == NODE_TYPE_MEASUREMENT) {
                                readWriteHandler.deleteNodeByPrefix(
                                    readWriteHandler.getCFHByName(TABLE_NAME_TAGS),
                                    startKey,
                                    endKey);
                              }
                              readWriteHandler.deleteNodeByPrefix(startKey, endKey);
                            }
                          } catch (RocksDBException e) {
                            logger.error("delete storage error {}", path.getFullPath(), e);
                          }
                        });
                if (getAllTimeseriesCount(path.concatNode(MULTI_LEVEL_PATH_WILDCARD)) <= 0) {
                  readWriteHandler.deleteNode(path.getNodes(), RMNodeType.STORAGE_GROUP);
                } else {
                  throw new MetadataException(
                      String.format(
                          "New timeseries created after sg deleted, won't delete storage node: %s",
                          path.getFullPath()));
                }
              } catch (RocksDBException | MetadataException | InterruptedException e) {
                throw new RuntimeException(e);
              } finally {
                storageGroupDeletingFlagMap.remove(path.getFullPath());
              }
            });
  }

  @Override
  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    String levelPath =
        RSchemaUtils.getLevelPath(storageGroup.getNodes(), storageGroup.getNodeLength() - 1);
    byte[] pathKey = RSchemaUtils.toStorageNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (readWriteHandler.keyExist(pathKey, holder)) {
            byte[] value = RSchemaUtils.updateTTL(holder.getValue(), dataTTL);
            readWriteHandler.updateNode(pathKey, value);
          } else {
            throw new PathNotExistException(
                "Storage group node of path doesn't exist: " + levelPath);
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (InterruptedException | RocksDBException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for get and auto create device
  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param devicePath path
   */
  private IMNode getDeviceNodeWithAutoCreate(PartialPath devicePath, boolean aligned)
      throws MetadataException {
    IMNode node;
    try {
      node = getDeviceNode(devicePath);
      return node;
    } catch (PathNotExistException e) {
      int sgIndex = ensureStorageGroup(devicePath);
      if (!config.isAutoCreateSchemaEnabled()) {
        throw new PathNotExistException(devicePath.getFullPath());
      }
      try {
        createEntityRecursively(
            devicePath.getNodes(), devicePath.getNodeLength(), sgIndex, aligned, new Stack<>());
        node = getDeviceNode(devicePath);
      } catch (RocksDBException | InterruptedException ex) {
        throw new MetadataException(ex);
      }
    }
    return node;
  }

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) {
    throw new UnsupportedOperationException();
  }

  // endregion

  // region Interfaces for metadata info Query

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    if (PATH_ROOT.equals(path.getFullPath())) {
      return true;
    }

    String innerPathName = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    try {
      CheckKeyResult checkKeyResult =
          readWriteHandler.keyExistByTypes(innerPathName, RMNodeType.values());
      if (checkKeyResult.existAnyKey()) {
        return true;
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
    return false;
  }

  /** Get metadata in string */
  @Override
  public String getMetadataInString() {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  // todo mem count
  @Override
  public long getTotalSeriesNumber() {
    return readWriteHandler.countNodesNumByType(null, RSchemaConstants.NODE_TYPE_MEASUREMENT);
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getCountByNodeType(new Character[] {NODE_TYPE_MEASUREMENT}, pathPattern.getNodes());
  }

  public void traverseOutcomeBasins(
      String[] nodes,
      int maxLevel,
      BiFunction<byte[], byte[], Boolean> function,
      Character[] nodeTypeArray)
      throws IllegalPathException {
    List<String[]> allNodesArray = RSchemaUtils.replaceMultiWildcardToSingle(nodes, maxLevel);
    allNodesArray.parallelStream().forEach(x -> traverseByPatternPath(x, function, nodeTypeArray));
  }

  public void traverseByPatternPath(
      String[] nodes, BiFunction<byte[], byte[], Boolean> function, Character[] nodeTypeArray) {
    //    String[] nodes = pathPattern.getNodes();

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

      scanKeys
          .parallelStream()
          .forEach(
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

  private int indexOfFirstNonWildcard(String[] nodes, int start) {
    int index = start;
    for (; index < nodes.length; index++) {
      if (!ONE_LEVEL_PATH_WILDCARD.equals(nodes[index])
          && !MULTI_LEVEL_PATH_WILDCARD.equals(nodes[index])) {
        break;
      }
    }
    return index;
  }

  private int indexOfFirstWildcard(String[] nodes, int start) {
    int index = start;
    for (; index < nodes.length; index++) {
      if (ONE_LEVEL_PATH_WILDCARD.equals(nodes[index])
          || MULTI_LEVEL_PATH_WILDCARD.equals(nodes[index])) {
        break;
      }
    }
    return index;
  }

  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  /**
   * To calculate the count of devices for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  @Override
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
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

  @Override
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
  }

  /**
   * To calculate the count of storage group for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  @Override
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getCountByNodeType(new Character[] {NODE_TYPE_SG}, pathPattern.getNodes());
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern. If using prefix
   * match, the path pattern is used to match prefix path. All timeseries start with the matched
   * prefix path will be counted.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    // todo support wildcard
    if (pathPattern.getFullPath().contains(ONE_LEVEL_PATH_WILDCARD)) {
      throw new MetadataException(
          "Wildcards are not currently supported for this operation [COUNT NODES pathPattern].");
    }
    String innerNameByLevel =
        RSchemaUtils.getLevelPath(pathPattern.getNodes(), pathPattern.getNodeLength() - 1, level);
    AtomicInteger atomicInteger = new AtomicInteger(0);
    Function<String, Boolean> function =
        s -> {
          atomicInteger.incrementAndGet();
          return true;
        };
    Arrays.stream(ALL_NODE_TYPE_ARRAY)
        .parallel()
        .forEach(
            x -> {
              String getKeyByInnerNameLevel =
                  x + innerNameByLevel + RSchemaConstants.PATH_SEPARATOR + level;
              readWriteHandler.getKeyByPrefix(getKeyByInnerNameLevel, function);
            });

    return atomicInteger.get();
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   */
  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
      throws MetadataException {
    return getNodesCountInGivenLevel(pathPattern, level, false);
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    return null;
  }
  // endregion

  // region Interfaces for level Node info Query

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
  @Override
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    // TODO: ignore pathPattern with *, all nodeLevel are start from "root.*"
    List<PartialPath> results = new ArrayList<>();
    if (nodeLevel == 0) {
      results.add(new PartialPath(RSchemaConstants.ROOT));
      return results;
    }
    // TODO: level one usually only contains small numbers, query in serialize
    Set<String> paths;
    StringBuilder builder = new StringBuilder();
    if (nodeLevel <= 5) {
      char level = (char) (ZERO + nodeLevel);
      String prefix =
          builder.append(RSchemaConstants.ROOT).append(PATH_SEPARATOR).append(level).toString();
      paths = readWriteHandler.getAllByPrefix(prefix);
    } else {
      paths = ConcurrentHashMap.newKeySet();
      char upperLevel = (char) (ZERO + nodeLevel - 1);
      String prefix =
          builder
              .append(RSchemaConstants.ROOT)
              .append(PATH_SEPARATOR)
              .append(upperLevel)
              .toString();
      Set<String> parentPaths = readWriteHandler.getAllByPrefix(prefix);
      parentPaths
          .parallelStream()
          .forEach(
              x -> {
                String targetPrefix = RSchemaUtils.getNextLevelOfPath(x, upperLevel);
                paths.addAll(readWriteHandler.getAllByPrefix(targetPrefix));
              });
    }
    return RSchemaUtils.convertToPartialPath(paths, nodeLevel);
  }

  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
      throws MetadataException {
    return getNodesListInGivenLevel(pathPattern, nodeLevel);
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
  @Override
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException {
    // todo support wildcard
    if (pathPattern.getFullPath().contains(ONE_LEVEL_PATH_WILDCARD)) {
      throw new MetadataException(
          "Wildcards are not currently supported for this operation [SHOW CHILD PATHS pathPattern].");
    }
    Set<String> result = Collections.synchronizedSet(new HashSet<>());
    String innerNameByLevel =
        RSchemaUtils.getLevelPath(
                pathPattern.getNodes(),
                pathPattern.getNodeLength() - 1,
                pathPattern.getNodeLength())
            + RSchemaConstants.PATH_SEPARATOR
            + pathPattern.getNodeLength();
    Function<String, Boolean> function =
        s -> {
          result.add(RSchemaUtils.getPathByInnerName(s));
          return true;
        };

    Arrays.stream(ALL_NODE_TYPE_ARRAY)
        .parallel()
        .forEach(x -> readWriteHandler.getKeyByPrefix(x + innerNameByLevel, function));
    return result;
  }

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
  @Override
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    Set<String> childPath = getChildNodePathInNextLevel(pathPattern);
    Set<String> childName = new HashSet<>();
    for (String str : childPath) {
      childName.add(str.substring(str.lastIndexOf(RSchemaConstants.PATH_SEPARATOR) + 1));
    }
    return childName;
  }
  // endregion

  // region Interfaces for StorageGroup and TTL info Query
  @Override
  public boolean isStorageGroup(PartialPath path) throws MetadataException {
    int level = RSchemaUtils.getLevelByPartialPath(path.getFullPath());
    String innerPathName =
        RSchemaUtils.convertPartialPathToInner(
            path.getFullPath(), level, RSchemaConstants.NODE_TYPE_SG);
    try {
      return readWriteHandler.keyExist(innerPathName.getBytes());
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /** Check whether the given path contains a storage group */
  @Override
  public boolean checkStorageGroupByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    try {
      if (indexOfSgNode(nodes) > 0) {
        return true;
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
    return false;
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path)
      throws StorageGroupNotSetException, IllegalPathException {
    String innerPathName =
        readWriteHandler.findBelongToSpecifiedNodeType(
            path.getNodes(), RSchemaConstants.NODE_TYPE_SG);
    if (innerPathName == null) {
      throw new StorageGroupNotSetException(
          String.format("Cannot find [%s] belong to which storage group.", path.getFullPath()));
    }
    return new PartialPath(RSchemaUtils.getPathByInnerName(innerPathName));
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
  @Override
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    GetBelongedToSpecifiedType type;
    try {
      type = new GetBelongedToSpecifiedType(pathPattern, readWriteHandler, NODE_TYPE_SG);
      return new ArrayList<>(type.getAllResult());
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Get all storage group matching given path pattern. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
  @Override
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<PartialPath> allPath = new ArrayList<>();
    getMatchedPathByNodeType(pathPattern.getNodes(), new Character[] {NODE_TYPE_SG}, allPath);
    return allPath;
  }

  private void getMatchedPathByNodeType(
      String[] nodes, Character[] nodetype, Collection<PartialPath> collection)
      throws IllegalPathException {
    List<String> allResult = Collections.synchronizedList(new ArrayList<>());
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          allResult.add(RSchemaUtils.getPathByInnerName(new String(a)));
          return true;
        };
    traverseOutcomeBasins(nodes, MAX_PATH_DEPTH, function, nodetype);

    for (String path : allResult) {
      collection.add(new PartialPath(path));
    }
  }

  /** Get all storage group paths */
  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    List<PartialPath> allStorageGroupPath = Collections.synchronizedList(new ArrayList<>());
    Function<String, Boolean> function =
        s -> {
          try {
            allStorageGroupPath.add(new PartialPath(RSchemaUtils.getPathByInnerName(s)));
          } catch (IllegalPathException e) {
            logger.error(e.getMessage());
            return false;
          }
          return true;
        };
    readWriteHandler.getKeyByPrefix(String.valueOf(NODE_TYPE_SG), function);
    return allStorageGroupPath;
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  @Override
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    List<IStorageGroupMNode> sgNodes = getAllStorageGroupNodes();
    return sgNodes.stream()
        .collect(
            Collectors.toMap(IStorageGroupMNode::getPartialPath, IStorageGroupMNode::getDataTTL));
  }
  // endregion

  // region Interfaces for Entity/Device info Query

  /**
   * Get all devices that one of the timeseries, matching the given timeseries path pattern, belongs
   * to.
   *
   * @param timeseries a path pattern of the target timeseries
   * @return A HashSet instance which stores devices paths.
   */
  @Override
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    GetBelongedToSpecifiedType type;
    try {
      type = new GetBelongedToSpecifiedType(timeseries, readWriteHandler, NODE_TYPE_ENTITY);
      return type.getAllResult();
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Get all device paths matching the path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern the pattern of the target devices.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  @Override
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {

    Set<PartialPath> allPath = new HashSet<>();
    getMatchedPathByNodeType(pathPattern.getNodes(), new Character[] {NODE_TYPE_ENTITY}, allPath);
    return allPath;
  }

  /**
   * Get all device paths and according storage group paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult.
   */
  @Override
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    List<ShowDevicesResult> res = Collections.synchronizedList(new ArrayList<>());
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          String fullPath = RSchemaUtils.getPathByInnerName(new String(a));
          try {
            res.add(
                new ShowDevicesResult(
                    fullPath,
                    RSchemaUtils.isAligned(b),
                    getBelongedToSG(plan.getPath().getNodes())));
          } catch (MetadataException e) {
            logger.error(e.getMessage());
            return false;
          }
          return true;
        };
    traverseOutcomeBasins(
        plan.getPath().getNodes(), MAX_PATH_DEPTH, function, new Character[] {NODE_TYPE_ENTITY});
    return res;
  }

  private String getBelongedToSG(String[] nodes) throws MetadataException {
    List<String> contextNodeName = new ArrayList<>();
    for (int idx = 1; idx < nodes.length; idx++) {
      contextNodeName.add(nodes[idx]);
      String innerName =
          RSchemaUtils.convertPartialPathToInnerByNodes(
              contextNodeName.toArray(new String[0]), contextNodeName.size(), NODE_TYPE_SG);
      byte[] queryResult;
      try {
        queryResult = readWriteHandler.get(null, innerName.getBytes());
      } catch (RocksDBException e) {
        throw new MetadataException(e);
      }
      if (queryResult != null) {
        return RSchemaUtils.concatNodesName(nodes, 0, idx);
      }
    }
    return StringUtil.EMPTY_STRING;
  }
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'. If using prefix match, the path pattern is used to match prefix path. All timeseries start
   * with the matched prefix path will be collected.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMatchedMeasurementPath(pathPattern.getNodes());
  }

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   */
  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern)
      throws MetadataException {
    return getMeasurementPaths(pathPattern, false);
  }

  /**
   * Similar to method getMeasurementPaths(), but return Path with alias and filter the result by
   * limit and offset. If using prefix match, the path pattern is used to match prefix path. All
   * timeseries start with the matched prefix path will be collected.
   *
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  @Override
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    // todo update offset
    return new Pair<>(getMatchedMeasurementPath(pathPattern.getNodes()), offset + limit);
  }

  private List<MeasurementPath> getMatchedMeasurementPath(String[] nodes)
      throws IllegalPathException {
    List<MeasurementPath> allResult = Collections.synchronizedList(new ArrayList<>());
    BiFunction<byte[], byte[], Boolean> function =
        (a, b) -> {
          allResult.add(
              new RMeasurementMNode(
                      RSchemaUtils.getPathByInnerName(new String(a)), b, readWriteHandler)
                  .getMeasurementPath());
          return true;
        };
    traverseOutcomeBasins(nodes, MAX_PATH_DEPTH, function, new Character[] {NODE_TYPE_MEASUREMENT});

    return allResult;
  }

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
          @SuppressWarnings("unchecked")
          Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
              new Pair<>((Map<String, String>) tag, (Map<String, String>) attributes);
          allResult.put(measurementPath, tagsAndAttributes);
          return true;
        };
    traverseOutcomeBasins(nodes, MAX_PATH_DEPTH, function, new Character[] {NODE_TYPE_MEASUREMENT});

    return allResult;
  }

  @Override
  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    if (plan.getKey() != null && plan.getValue() != null) {
      return showTimeseriesWithIndex(plan, context);
    } else {
      return showTimeseriesWithoutIndex(plan, context);
    }
  }

  private List<ShowTimeSeriesResult> showTimeseriesWithIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    // temporarily unsupported
    return Collections.emptyList();
  }

  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {

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
              getBelongedToSG(measurementPath.getNodes()),
              measurementPath.getMeasurementSchema().getType(),
              measurementPath.getMeasurementSchema().getEncodingType(),
              measurementPath.getMeasurementSchema().getCompressor(),
              0,
              entry.getValue().left,
              entry.getValue().right));
    }
    return res;
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
  @Override
  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
    if (fullPath.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }
    return getSeriesSchema(fullPath).getType();
  }

  @Override
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    try {
      String levelKey =
          RSchemaUtils.getLevelPath(fullPath.getNodes(), fullPath.getNodeLength() - 1);
      Holder<byte[]> holder = new Holder<>();
      if (readWriteHandler.keyExistByType(levelKey, RMNodeType.MEASUREMENT, holder)) {
        MeasurementSchema measurementSchema =
            (MeasurementSchema)
                RSchemaUtils.parseNodeValue(holder.getValue(), RMNodeValueType.SCHEMA);
        return measurementSchema;
      } else {
        throw new PathNotExistException(fullPath.getFullPath());
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    List<MeasurementPath> result = new ArrayList<>();
    String nextLevelPathName =
        RSchemaUtils.convertPartialPathToInner(
            devicePath.getFullPath(), devicePath.getNodeLength() + 1, NODE_TYPE_MEASUREMENT);
    Map<byte[], byte[]> allMeasurementPath =
        readWriteHandler.getKeyValueByPrefix(nextLevelPathName);
    for (Map.Entry<byte[], byte[]> entry : allMeasurementPath.entrySet()) {
      PartialPath pathName;
      try {
        pathName = new PartialPath(new String(entry.getKey()));
      } catch (IllegalPathException e) {
        throw new PathNotExistException(e.getMessage());
      }
      MeasurementSchema measurementSchema =
          (MeasurementSchema) RSchemaUtils.parseNodeValue(entry.getValue(), RMNodeValueType.SCHEMA);
      result.add(new MeasurementPath(pathName, measurementSchema));
    }
    return result;
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node by path. If storage group is not
   * set, StorageGroupNotSetException will be thrown
   */
  @Override
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return getStorageGroupNodeByPath(path);
  }

  /** Get storage group node by path. the give path don't need to be storage group path. */
  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    ensureStorageGroup(path);
    IStorageGroupMNode node = null;
    try {
      String[] nodes = path.getNodes();
      for (int i = 1; i < nodes.length; i++) {
        String levelPath = RSchemaUtils.getLevelPath(nodes, i);
        Holder<byte[]> holder = new Holder<>();
        if (readWriteHandler.keyExistByType(levelPath, RMNodeType.STORAGE_GROUP, holder)) {
          node =
              new RStorageGroupMNode(
                  MetaUtils.getStorageGroupPathByLevel(path, i).getFullPath(),
                  holder.getValue(),
                  readWriteHandler);
          break;
        }
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }

    if (node == null) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }

    return node;
  }

  /** Get all storage group MNodes */
  @Override
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    List<IStorageGroupMNode> result = new ArrayList<>();
    RocksIterator iterator = readWriteHandler.iterator(null);
    // get all storage group path
    for (iterator.seek(new byte[] {NODE_TYPE_SG}); iterator.isValid(); iterator.next()) {
      if (iterator.key()[0] != NODE_TYPE_SG) {
        break;
      }
      result.add(
          new RStorageGroupMNode(
              RSchemaUtils.getPathByInnerName(new String(iterator.key())),
              iterator.value(),
              readWriteHandler));
    }
    return result;
  }

  @Override
  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    String levelPath = RSchemaUtils.getLevelPath(nodes, nodes.length - 1);
    Holder<byte[]> holder = new Holder<>();
    try {
      if (readWriteHandler.keyExistByType(levelPath, RMNodeType.ENTITY, holder)) {
        return new REntityMNode(path.getFullPath(), holder.getValue(), readWriteHandler);
      } else {
        throw new PathNotExistException(path.getFullPath());
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  @Override
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

  @Override
  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
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
  // endregion

  // region Interfaces for alias and tag/attribute operations
  @Override
  public void changeAlias(PartialPath path, String newAlias) throws MetadataException, IOException {
    upsertTagsAndAttributes(newAlias, null, null, path);
  }

  @Override
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath path)
      throws MetadataException, IOException {
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
            Lock newAliasLock = locksPool.computeIfAbsent(newAliasLevel, x -> new ReentrantLock());
            Lock oldAliasLock = null;
            boolean lockedOldAlias = false;
            boolean lockedNewAlias = false;
            try (WriteBatch batch = new WriteBatch()) {
              if (newAliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                lockedNewAlias = true;
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
                  oldAliasLock = locksPool.computeIfAbsent(oldAliasLevel, x -> new ReentrantLock());
                  if (oldAliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                    lockedOldAlias = true;
                    if (!readWriteHandler.keyExist(oldAliasKey)) {
                      logger.error(
                          "origin node [{}] has alias but alias node [{}] doesn't exist ",
                          levelPath,
                          oldAliasLevel);
                    }
                    batch.delete(oldAliasKey);
                  } else {
                    throw new AcquireLockTimeoutException("acquire lock timeout: " + oldAliasLevel);
                  }
                }
              } else {
                throw new AcquireLockTimeoutException("acquire lock timeout: " + newAliasLevel);
              }
              // TODO: need application lock
              readWriteHandler.executeBatch(batch);
            } finally {
              if (lockedNewAlias) {
                newAliasLock.unlock();
              }
              if (oldAliasLock != null && lockedOldAlias) {
                oldAliasLock.unlock();
              }
            }
          }

          WriteBatch batch = new WriteBatch();
          if (tagsMap != null && !tagsMap.isEmpty()) {
            if (mNode.getTags() == null) {
              mNode.setTags(tagsMap);
            } else {
              mNode.getTags().putAll(tagsMap);
            }
            batch.put(
                readWriteHandler.getCFHByName(TABLE_NAME_TAGS), originKey, DEFAULT_NODE_VALUE);
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
        } finally {
          rawKeyLock.unlock();
        }
      } else {
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath path)
      throws MetadataException, IOException {
    if (attributesMap == null || attributesMap.isEmpty()) {
      return;
    }
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
                        "TimeSeries [%s] already has the attribute [%s].", path, new String(key)));
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
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath path)
      throws MetadataException, IOException {
    if (tagsMap == null || tagsMap.isEmpty()) {
      return;
    }

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
              batch.put(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), key, DEFAULT_NODE_VALUE);
            }
            batch.put(key, mNode.getRocksDBValue());
            // TODO: need application lock
            readWriteHandler.executeBatch(batch);
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath path)
      throws MetadataException, IOException {
    if (keySet == null || keySet.isEmpty()) {
      return;
    }
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
                batch.delete(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), key);
              }
              batch.put(key, mNode.getRocksDBValue());
              readWriteHandler.executeBatch(batch);
            }
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath path)
      throws MetadataException, IOException {
    if (alterMap == null || alterMap.isEmpty()) {
      return;
    }

    String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] key = RSchemaUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.computeIfAbsent(levelPath, x -> new ReentrantLock());
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
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
                      "TimeSeries [%s] does not have tag/attribute [%s].", path, new String(key)),
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
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
      if (!readWriteHandler.keyExist(key, holder)) {
        throw new PathNotExistException(path.getFullPath());
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath path)
      throws MetadataException, IOException {
    if (StringUtils.isEmpty(oldKey) || StringUtils.isEmpty(newKey)) {
      return;
    }

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
                String.format("TimeSeries [%s] does not have tag/attribute [%s].", path, oldKey),
                true);
          }
        } finally {
          lock.unlock();
        }
      } else {
        throw new AcquireLockTimeoutException("acquire lock timeout: " + levelPath);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces only for Cluster module usage
  @Override
  public void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas) {}

  @Override
  public void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas) {}

  @Override
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    return null;
  }
  // end region

  @Override
  public void cacheMeta(
      PartialPath path, IMeasurementMNode measurementMNode, boolean needSetFullPath) {
    // do noting
  }

  // region Interfaces for lastCache operations
  @Override
  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {}

  @Override
  public void updateLastCache(
      IMeasurementMNode node,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {}

  @Override
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    return null;
  }

  @Override
  public TimeValuePair getLastCache(IMeasurementMNode node) {
    return null;
  }

  @Override
  public void resetLastCache(PartialPath seriesPath) {}

  @Override
  public void deleteLastCacheByDevice(PartialPath deviceId) throws MetadataException {}

  @Override
  public void deleteLastCacheByDevice(
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)
      throws MetadataException {}
  // endregion

  // region Interfaces and Implementation for InsertPlan process
  @Override
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    // devicePath is a logical path which is parent of measurement, whether in template or not
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    IMNode deviceMNode = getDeviceNodeWithAutoCreate(devicePath, plan.isAligned());

    // check insert non-aligned InsertPlan for aligned timeseries
    if (deviceMNode.isEntity()) {
      if (plan.isAligned() && !deviceMNode.getAsEntityMNode().isAligned()) {
        throw new MetadataException(
            String.format(
                "Timeseries under path [%s] is not aligned , please set InsertPlan.isAligned() = false",
                plan.getDevicePath()));
      }

      if (!plan.isAligned() && deviceMNode.getAsEntityMNode().isAligned()) {
        throw new MetadataException(
            String.format(
                "Timeseries under path [%s] is aligned , please set InsertPlan.isAligned() = true",
                plan.getDevicePath()));
      }
    }

    // get node for each measurement
    Map<Integer, IMeasurementMNode> nodeMap = new HashMap<>();
    Map<Integer, PartialPath> missingNodeIndex = new HashMap<>();
    for (int i = 0; i < measurementList.length; i++) {
      PartialPath path = new PartialPath(devicePath.getFullPath(), measurementList[i]);
      IMeasurementMNode node = getMeasurementMNode(path);
      if (node == null) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new PathNotExistException(path.getFullPath());
        }
        missingNodeIndex.put(i, path);
      } else {
        nodeMap.put(i, node);
      }
    }

    // create missing nodes
    if (!missingNodeIndex.isEmpty()) {
      if (!(plan instanceof InsertRowPlan) && !(plan instanceof InsertTabletPlan)) {
        throw new MetadataException(
            String.format(
                "Only support insertRow and insertTablet, plan is [%s]", plan.getOperatorType()));
      }

      if (plan.isAligned()) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        for (Integer index : missingNodeIndex.keySet()) {
          measurements.add(measurementList[index]);
          TSDataType type = plan.getDataTypes()[index];
          dataTypes.add(type);
          encodings.add(getDefaultEncoding(type));
        }
        createAlignedTimeSeries(devicePath, measurements, dataTypes, encodings, null);
      } else {
        for (Map.Entry<Integer, PartialPath> entry : missingNodeIndex.entrySet()) {
          IMeasurementSchema schema =
              new MeasurementSchema(
                  entry.getValue().getMeasurement(), plan.getDataTypes()[entry.getKey()]);
          createTimeSeries(entry.getValue(), schema, null, null, null);
        }
      }

      // get the latest node
      for (Entry<Integer, PartialPath> entry : missingNodeIndex.entrySet()) {
        nodeMap.put(entry.getKey(), getMeasurementMNode(entry.getValue()));
      }
    }

    // check datatype
    for (int i = 0; i < measurementList.length; i++) {
      try {
        // check type is match
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          try {
            checkDataTypeMatch(plan, i, nodeMap.get(i).getSchema().getType());
          } catch (DataTypeMismatchException mismatchException) {
            logger.warn(
                "DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
                measurementList[i],
                plan.getDataTypes()[i],
                nodeMap.get(i).getSchema().getType());
            if (!config.isEnablePartialInsert()) {
              throw mismatchException;
            } else {
              // mark failed measurement
              plan.markFailedMeasurementInsertion(i, mismatchException);
              continue;
            }
          }
          measurementMNodes[i] = nodeMap.get(i);
          // set measurementName instead of alias
          measurementList[i] = nodeMap.get(i).getName();
        }
      } catch (MetadataException e) {
        if (IoTDB.isClusterMode()) {
          logger.debug(
              "meet error when check {}.{}, message: {}",
              devicePath,
              measurementList[i],
              e.getMessage());
        } else {
          logger.warn(
              "meet error when check {}.{}, message: {}",
              devicePath,
              measurementList[i],
              e.getMessage());
        }
        if (config.isEnablePartialInsert()) {
          // mark failed measurement
          plan.markFailedMeasurementInsertion(i, e);
        } else {
          throw e;
        }
      }
    }
    return deviceMNode;
  }

  private void checkDataTypeMatch(InsertPlan plan, int loc, TSDataType dataType)
      throws MetadataException {
    TSDataType insertDataType;
    if (plan instanceof InsertRowPlan) {
      if (!((InsertRowPlan) plan).isNeedInferType()) {
        // only when InsertRowPlan's values is object[], we should check type
        insertDataType = getTypeInLoc(plan, loc);
      } else {
        insertDataType = dataType;
      }
    } else {
      insertDataType = getTypeInLoc(plan, loc);
    }
    if (dataType != insertDataType) {
      String measurement = plan.getMeasurements()[loc];
      logger.warn(
          "DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
          measurement,
          insertDataType,
          dataType);
      throw new DataTypeMismatchException(measurement, insertDataType, dataType);
    }
  }

  /** get dataType of plan, in loc measurements only support InsertRowPlan and InsertTabletPlan */
  private TSDataType getTypeInLoc(InsertPlan plan, int loc) throws MetadataException {
    TSDataType dataType;
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      dataType =
          TypeInferenceUtils.getPredictedDataType(tPlan.getValues()[loc], tPlan.isNeedInferType());
    } else if (plan instanceof InsertTabletPlan) {
      dataType = (plan).getDataTypes()[loc];
    } else {
      throw new MetadataException(
          String.format(
              "Only support insert and insertTablet, plan is [%s]", plan.getOperatorType()));
    }
    return dataType;
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  @Override
  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void pruneSchemaTemplate(PruneTemplatePlan plan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int countMeasurementsInTemplate(String templateName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPathExistsInTemplate(String templateName, String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getMeasurementsInTemplate(String templateName, String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Pair<String, IMeasurementSchema>> getSchemasInTemplate(
      String templateName, String path) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getAllTemplates() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IMNode setUsingSchemaTemplate(IMNode plan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) {
    throw new UnsupportedOperationException();
  }
  // endregion

  // start test only region
  @TestOnly
  public void printScanAllKeys() throws IOException {
    readWriteHandler.scanAllKeys(config.getSystemDir() + "/" + "rocksdb.key");
  }

  @Override
  public void deactivate() throws MetadataException {
    try {
      readWriteHandler.close();
    } catch (RocksDBException e) {
      logger.error("Failed to close readWriteHandler,try again.", e);
      try {
        Thread.sleep(5);
        readWriteHandler.close();
      } catch (RocksDBException | InterruptedException e1) {
        throw new MetadataException(e1);
      }
    }
  }

  @TestOnly
  public String getDeviceId(PartialPath devicePath) {
    String device = null;
    try {
      IMNode deviceNode = getDeviceNode(devicePath);
      device = deviceNode.getFullPath();
    } catch (MetadataException | NullPointerException e) {
      // Cannot get deviceId from MManager, return the input deviceId
    }
    return device;
  }

  @TestOnly
  public Template getTemplate(String templateName) {
    throw new UnsupportedOperationException();
  }

  // end region
}
