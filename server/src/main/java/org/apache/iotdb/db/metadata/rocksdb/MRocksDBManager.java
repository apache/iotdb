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
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AcquireLockTimeoutException;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.IMetaManager;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rocksdb.mnode.REntityMNode;
import org.apache.iotdb.db.metadata.rocksdb.mnode.RMeasurementMNode;
import org.apache.iotdb.db.metadata.rocksdb.mnode.RStorageGroupMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.google.common.util.concurrent.Striped;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_SCHEMA;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_ALIGNED_ENTITY_VALUE;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_NODE_VALUE;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_IS_ALIGNED;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_IS_SCHEMA;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_SET_TTL;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_SG;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.TABLE_NAME_TAGS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ZERO;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>MManager Singleton
 *   <li>Interfaces and Implementation of MManager initialization、snapshot、recover and clear
 *   <li>Interfaces for CQ
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces and Implementation for StorageGroup and TTL operation
 *   <li>Interfaces for get and auto create device
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for metadata count
 *         <li>Interfaces for level Node info Query
 *         <li>Interfaces for StorageGroup and TTL info Query
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces and methods for MNode query
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces only for Cluster module usage
 *   <li>Interfaces for lastCache operations
 *   <li>Interfaces and Implementation for InsertPlan process
 *   <li>Interfaces and Implementation for Template operations
 *   <li>TestOnly Interfaces
 * </ol>
 */
public class MRocksDBManager implements IMetaManager {

  private static final Logger logger = LoggerFactory.getLogger(MRocksDBManager.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // TODO: make it configurable
  public static int MAX_PATH_DEPTH = 10;

  private static final long MAX_LOCK_WAIT_TIME = 50;

  private RocksDBReadWriteHandler readWriteHandler;

  // TODO: check how Stripped Lock consume memory
  private Striped<Lock> locksPool = Striped.lazyWeakLock(10000);

  private volatile Map<String, Boolean> storageGroupDeletingFlagMap = new ConcurrentHashMap<>();

  public MRocksDBManager() throws MetadataException {
    try {
      readWriteHandler = RocksDBReadWriteHandler.getInstance();
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  // region Interfaces and Implementation of MManager initialization、snapshot、recover and clear
  @Override
  public void init() {}

  @Override
  public void createMTreeSnapshot() {
    throw new UnsupportedOperationException(
        "RocksDB based MetaData Manager doesn't support the operation");
  }

  @TestOnly
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
        logger.error("unsupported operations {}", plan.toString());
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces for CQ
  @Override
  public void createContinuousQuery(CreateContinuousQueryPlan plan) throws MetadataException {}

  @Override
  public void dropContinuousQuery(DropContinuousQueryPlan plan) throws MetadataException {}

  @Override
  public void writeCreateContinuousQueryLog(CreateContinuousQueryPlan plan) throws IOException {}

  @Override
  public void writeDropContinuousQueryLog(DropContinuousQueryPlan plan) throws IOException {}
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
    if (path.getNodes().length > MRocksDBManager.MAX_PATH_DEPTH) {
      throw new IllegalPathException(
          String.format(
              "path is too long, provide: %d, max: %d",
              path.getNodeLength(), MRocksDBManager.MAX_PATH_DEPTH));
    }
    MetaFormatUtils.checkTimeseries(path);
    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), schema.getProps());

    // sg check and create
    String[] nodes = path.getNodes();
    SchemaUtils.checkDataTypeWithEncoding(schema.getType(), schema.getEncodingType());
    int sgIndex = ensureStorageGroup(path, path.getNodeLength() - 1);

    try {
      createTimeSeriesRecursively(
          nodes, nodes.length, sgIndex, schema, alias, tags, attributes, new Stack<>());
      // TODO: load tags to memory
    } catch (RocksDBException | InterruptedException | IOException e) {
      throw new MetadataException(e);
    }
  }

  private void createTimeSeriesRecursively(
      String nodes[],
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
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.get(levelPath);
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
            readWriteHandler.createNode(levelPath, RocksDBMNodeType.ENTITY, DEFAULT_NODE_VALUE);
          } else {
            readWriteHandler.createNode(levelPath, RocksDBMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
          }
        } else {
          if (start == nodes.length) {
            throw new PathAlreadyExistException(levelPath);
          }

          if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
              || checkResult.getResult(RocksDBMNodeType.ALISA)) {
            throw new PathAlreadyExistException(levelPath);
          }

          if (start == nodes.length - 1) {
            if (checkResult.getResult(RocksDBMNodeType.INTERNAL)) {
              // convert the parent node to entity if it is internal node
              readWriteHandler.convertToEntityNode(levelPath, DEFAULT_NODE_VALUE);
            } else if (checkResult.getResult(RocksDBMNodeType.ENTITY)) {
              if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
                throw new AlignedTimeseriesException(
                    "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
                    levelPath);
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
    WriteBatch batch = new WriteBatch();
    byte[] value = RocksDBUtils.buildMeasurementNodeValue(schema, alias, tags, attributes);
    byte[] measurementKey = RocksDBUtils.toMeasurementNodeKey(levelPath);
    batch.put(measurementKey, value);

    // measurement with tags will save in a separate table at the same time
    if (tags != null && !tags.isEmpty()) {
      batch.put(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), measurementKey, DEFAULT_NODE_VALUE);
    }

    if (StringUtils.isNotEmpty(alias)) {
      String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
      aliasNodes[nodes.length - 1] = alias;
      String aliasLevelPath = RocksDBUtils.getLevelPath(aliasNodes, aliasNodes.length - 1);
      byte[] aliasNodeKey = RocksDBUtils.toAliasNodeKey(aliasLevelPath);
      Lock lock = locksPool.get(aliasLevelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (!readWriteHandler.keyExistByAllTypes(aliasLevelPath).existAnyKey()) {
            batch.put(aliasNodeKey, RocksDBUtils.buildAliasNodeValue(measurementKey));
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

  /**
   * @param prefixPath
   * @param measurements
   * @param dataTypes
   * @param encodings
   * @param compressors
   * @throws MetadataException
   */
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
            prefixPath, measurements, dataTypes, encodings, compressors, null));
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
      String.format(
          "Prefix path is too long, provide: %d, max: %d",
          prefixPath.getNodeLength(), MRocksDBManager.MAX_PATH_DEPTH - 1);
    }

    for (int i = 0; i < measurements.size(); i++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      MetaFormatUtils.checkNodeName(measurements.get(i));
    }

    int sgIndex = ensureStorageGroup(prefixPath, prefixPath.getNodeLength() - 1);

    try {
      createEntityRecursively(
          prefixPath.getNodes(), prefixPath.getNodeLength(), sgIndex + 1, true, new Stack<>());
      WriteBatch batch = new WriteBatch();
      String[] locks = new String[measurements.size()];
      for (int i = 0; i < measurements.size(); i++) {
        String measurement = measurements.get(i);
        String levelPath = RocksDBUtils.getMeasurementLevelPath(prefixPath.getNodes(), measurement);
        locks[i] = levelPath;
        MeasurementSchema schema =
            new MeasurementSchema(measurement, dataTypes.get(i), encodings.get(i));
        byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
        byte[] value = RocksDBUtils.buildMeasurementNodeValue(schema, null, null, null);
        batch.put(key, value);
      }

      Stack<Lock> acquiredLock = new Stack<>();
      try {
        for (String lockKey : locks) {
          Lock lock = locksPool.get(lockKey);
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

  /**
   * The method assume Storage Group Node has been created
   *
   * @param nodes
   * @param start
   * @param end
   * @param aligned
   */
  private void createEntityRecursively(
      String[] nodes, int start, int end, boolean aligned, Stack<Lock> lockedLocks)
      throws RocksDBException, MetadataException, InterruptedException {
    if (start <= end) {
      // nodes before "end" must exist
      return;
    }
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Lock lock = locksPool.get(levelPath);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath);
        if (!checkResult.existAnyKey()) {
          createEntityRecursively(nodes, start - 1, end, aligned, lockedLocks);
          if (start == nodes.length) {
            byte[] nodeKey = RocksDBUtils.toEntityNodeKey(levelPath);
            byte[] value = aligned ? DEFAULT_ALIGNED_ENTITY_VALUE : DEFAULT_NODE_VALUE;
            readWriteHandler.createNode(nodeKey, value);
          } else {
            readWriteHandler.createNode(levelPath, RocksDBMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
          }
        } else {
          if (start == nodes.length) {
            if (!checkResult.getResult(RocksDBMNodeType.ENTITY)) {
              throw new PathAlreadyExistException("Node already exists but not entity");
            }

            if ((checkResult.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
              throw new PathAlreadyExistException("Entity node exists but not aligned");
            }
          } else if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
              || checkResult.getResult(RocksDBMNodeType.ALISA)) {
            throw new PathAlreadyExistException("Path contains measurement node");
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

  @Override
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<MeasurementPath> allTimeseries = getMeasurementPaths(pathPattern, isPrefixMatch);
    if (allTimeseries.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }

    Set<String> failedNames = new HashSet<>();
    for (PartialPath p : allTimeseries) {
      try {
        String[] nodes = p.getNodes();
        if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
          throw new IllegalPathException(p.getFullPath());
        }

        // Delete measurement node
        String mLevelPath = RocksDBUtils.getLevelPath(p.getNodes(), p.getNodeLength() - 1);
        Lock lock = locksPool.get(mLevelPath);
        RMeasurementMNode deletedNode;
        if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            deletedNode = (RMeasurementMNode) getMeasurementMNode(p);
            WriteBatch batch = new WriteBatch();
            // delete the last node of path
            byte[] mNodeKey = RocksDBUtils.toMeasurementNodeKey(mLevelPath);
            batch.delete(mNodeKey);
            if (deletedNode.getAlias() != null) {
              batch.delete(RocksDBUtils.toAliasNodeKey(mLevelPath));
            }
            if (deletedNode.getTags() != null && !deletedNode.getTags().isEmpty()) {
              batch.delete(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), mNodeKey);
              // TODO: tags invert index update
            }
            readWriteHandler.executeBatch(batch);
          } finally {
            lock.unlock();
          }
        } else {
          throw new AcquireLockTimeoutException("acquire lock timeout, " + p.getFullPath());
        }

        // delete parent node if is empty
        IMNode curNode = deletedNode;
        while (curNode != null) {
          PartialPath curPath = curNode.getPartialPath();
          String curLevelPath =
              RocksDBUtils.getLevelPath(curPath.getNodes(), curPath.getNodeLength() - 1);
          Lock curLock = locksPool.get(curLevelPath);
          if (curLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
            try {
              IMNode toDelete = curNode.getParent();
              if (toDelete == null || toDelete.isStorageGroup()) {
                break;
              }

              if (toDelete.isEmptyInternal()) {
                if (toDelete.isEntity()) {
                  // TODO: aligned timeseries needs special check????
                  readWriteHandler.deleteNode(
                      toDelete.getPartialPath().getNodes(), RocksDBMNodeType.ENTITY);
                } else {
                  readWriteHandler.deleteNode(
                      toDelete.getPartialPath().getNodes(), RocksDBMNodeType.INTERNAL);
                }
                curNode = toDelete;
              } else {
                break;
              }
            } finally {
              curLock.unlock();
            }
          } else {
            throw new AcquireLockTimeoutException("acquire lock timeout, " + curNode.getFullPath());
          }
        }
        // TODO: trigger engine update
        // TODO: update totalTimeSeriesNumber
      } catch (Exception e) {
        logger.error("delete timeseries [{}] fail", p.getFullPath(), e);
        failedNames.add(p.getFullPath());
      }
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
  private int ensureStorageGroup(PartialPath path, int entityIndex) throws MetadataException {
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
        if ((entityIndex - sgPath.getNodeLength()) < 1) {
          throw new MetadataException("Storage Group Node and Entity Node could not be same!");
        }
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

    // make sure sg node and entity node are different
    if ((entityIndex - sgIndex) < 1) {
      throw new MetadataException("Storage Group Node and Entity Node could not be same!");
    }

    return sgIndex;
  }

  private int indexOfSgNode(String[] nodes) throws RocksDBException {
    int result = -1;
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RocksDBUtils.getLevelPath(nodes, i);
      if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.STORAGE_GROUP)) {
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
    if (storageGroup.getNodes().length > MRocksDBManager.MAX_PATH_DEPTH) {
      throw new IllegalPathException(
          String.format(
              "Storage group path is too long, provide: %d, max: %d",
              storageGroup.getNodeLength(), MRocksDBManager.MAX_PATH_DEPTH - 2));
    }
    MetaFormatUtils.checkStorageGroup(storageGroup.getFullPath());
    String[] nodes = storageGroup.getNodes();
    try {
      int len = nodes.length;
      for (int i = 1; i < nodes.length; i++) {
        String levelKey = RocksDBUtils.getLevelPath(nodes, i);
        Lock lock = locksPool.get(levelKey);
        if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
          try {
            CheckKeyResult keyCheckResult = readWriteHandler.keyExistByAllTypes(levelKey);
            if (!keyCheckResult.existAnyKey()) {
              if (i < len - 1) {
                readWriteHandler.createNode(
                    levelKey, RocksDBMNodeType.INTERNAL, DEFAULT_NODE_VALUE);
              } else {
                readWriteHandler.createNode(
                    levelKey, RocksDBMNodeType.STORAGE_GROUP, DEFAULT_NODE_VALUE);
              }
            } else if (keyCheckResult.getResult(RocksDBMNodeType.STORAGE_GROUP)) {
              throw new StorageGroupAlreadySetException(storageGroup.toString());
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

  /**
   * @param storageGroups
   * @throws MetadataException
   */
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
                Arrays.asList(RockDBConstants.ALL_NODE_TYPE_ARRAY).stream()
                    .parallel()
                    .forEach(
                        type -> {
                          try {
                            String startPath =
                                RocksDBUtils.getLevelPathPrefix(
                                    nodes, nodes.length - 1, nodes.length);
                            byte[] startKey = RocksDBUtils.toRocksDBKey(startPath, type);
                            String endPath =
                                RocksDBUtils.getLevelPathPrefix(
                                    nodes, nodes.length - 1, MAX_PATH_DEPTH);
                            byte[] endKey = RocksDBUtils.toRocksDBKey(endPath, type);
                            if (type == NODE_TYPE_MEASUREMENT) {
                              readWriteHandler.deleteNodeByPrefix(
                                  readWriteHandler.getCFHByName(TABLE_NAME_TAGS), startKey, endKey);
                            }
                            readWriteHandler.deleteNodeByPrefix(startKey, endKey);
                          } catch (RocksDBException e) {
                            logger.error("delete storage error {}", path.getFullPath(), e);
                          }
                        });
                if (getAllTimeseriesCount(path.concatNode(MULTI_LEVEL_PATH_WILDCARD)) <= 0) {
                  readWriteHandler.deleteNode(path.getNodes(), RocksDBMNodeType.STORAGE_GROUP);
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
        RocksDBUtils.getLevelPath(storageGroup.getNodes(), storageGroup.getNodeLength() - 1);
    byte[] pathKey = RocksDBUtils.toStorageNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (readWriteHandler.keyExist(pathKey, holder)) {
            byte[] value = RocksDBUtils.updateTTL(holder.getValue(), dataTTL);
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
  // endregion

  // region Interfaces for metadata info Query

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    String innerPathName = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    try {
      CheckKeyResult checkKeyResult =
          readWriteHandler.keyExistByTypes(innerPathName, RocksDBMNodeType.values());
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
    return readWriteHandler.countNodesNumByType(null, RockDBConstants.NODE_TYPE_MEASUREMENT);
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
    List<String[]> allNodesArray = RocksDBUtils.replaceMultiWildcardToSingle(nodes, maxLevel);
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
                    RocksDBUtils.convertPartialPathToInnerByNodes(nodes, nodes.length - 1, x);
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

      Queue<String[]> tempNodes = new ConcurrentLinkedQueue<>();
      byte[] suffixToMatch =
          RocksDBUtils.getSuffixOfLevelPath(
              ArrayUtils.subarray(nodes, firstNonWildcardIndex, nextFirstWildcardIndex), level);

      scanKeys
          .parallelStream()
          .forEach(
              prefixNodes -> {
                String levelPrefix =
                    RocksDBUtils.getLevelPathPrefix(prefixNodes, prefixNodes.length - 1, level);
                Arrays.stream(nodeTypeArray)
                    .parallel()
                    .forEach(
                        x -> {
                          byte[] startKey = RocksDBUtils.toRocksDBKey(levelPrefix, x);
                          RocksIterator iterator = readWriteHandler.iterator(null);
                          iterator.seek(startKey);
                          while (iterator.isValid()) {
                            if (!RocksDBUtils.prefixMatch(iterator.key(), startKey)) {
                              break;
                            }
                            if (RocksDBUtils.suffixMatch(iterator.key(), suffixToMatch)) {
                              if (lastIteration) {
                                function.apply(iterator.key(), iterator.value());
                              } else {
                                tempNodes.add(RocksDBUtils.toMetaNodes(iterator.key()));
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

  private Map<String, byte[]> getKeyNumByPrefix(
      PartialPath pathPattern, char nodeType, boolean isPrefixMatch) {
    Map<String, byte[]> result = new ConcurrentHashMap<>();
    Set<String> seeds = new HashSet<>();

    String seedPath;

    int nonWildcardAvailablePosition =
        pathPattern.getFullPath().indexOf(ONE_LEVEL_PATH_WILDCARD) - 1;
    if (nonWildcardAvailablePosition < 0) {
      seedPath = RocksDBUtils.getLevelPath(pathPattern.getNodes(), pathPattern.getNodeLength() - 1);
    } else {
      seedPath = pathPattern.getFullPath().substring(0, nonWildcardAvailablePosition);
    }

    seeds.add(new String(RocksDBUtils.toRocksDBKey(seedPath, nodeType)));

    scanAllKeysRecursively(
        seeds,
        0,
        s -> {
          try {
            byte[] value = readWriteHandler.get(null, s.getBytes());
            if (value != null && value.length > 0 && s.charAt(0) == nodeType) {
              if (!isPrefixMatch || isMatched(pathPattern, s)) {
                result.put(s, value);
                return false;
              }
            }
          } catch (RocksDBException e) {
            return false;
          }
          return true;
        },
        isPrefixMatch);
    return result;
  }

  // eg. pathPatter:root.a.b     prefixedKey=sroot.2a.2bbb
  private boolean isMatched(PartialPath pathPattern, String prefixedKey) {
    // path = root.a.bbb
    String path = RocksDBUtils.getPathByInnerName(prefixedKey);
    if (path.length() <= pathPattern.getFullPath().length()) {
      return true;
    } else {
      String fullPath = pathPattern.getFullPath() + RockDBConstants.PATH_SEPARATOR;
      return path.startsWith(fullPath);
    }
  }

  private void scanAllKeysRecursively(
      Set<String> seeds, int level, Function<String, Boolean> op, boolean isPrefixMatch) {
    if (seeds == null || seeds.isEmpty()) {
      return;
    }
    Set<String> children = ConcurrentHashMap.newKeySet();
    seeds
        .parallelStream()
        .forEach(
            x -> {
              if (op.apply(x)) {
                if (isPrefixMatch) {
                  for (int i = level; i < MAX_PATH_DEPTH; i++) {
                    // x is not leaf node
                    String nextLevel = RocksDBUtils.getNextLevelOfPath(x, i);
                    children.addAll(readWriteHandler.getAllByPrefix(nextLevel));
                  }
                } else {
                  String nextLevel = RocksDBUtils.getNextLevelOfPath(x, level);
                  children.addAll(readWriteHandler.getAllByPrefix(nextLevel));
                }
              }
            });
    if (!children.isEmpty()) {
      scanAllKeysRecursively(children, level + 1, op, isPrefixMatch);
    }
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
    String innerNameByLevel =
        RocksDBUtils.getLevelPath(pathPattern.getNodes(), pathPattern.getNodeLength() - 1, level);
    for (RocksDBMNodeType type : RocksDBMNodeType.values()) {
      String getKeyByInnerNameLevel = type.value + innerNameByLevel;
      int queryResult = readWriteHandler.getKeyByPrefix(getKeyByInnerNameLevel).size();
      if (queryResult != 0) {
        return queryResult;
      }
    }
    return 0;
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
      results.add(new PartialPath(RockDBConstants.ROOT));
      return results;
    }
    // TODO: level one usually only contains small numbers, query in serialize
    Set<String> paths;
    StringBuilder builder = new StringBuilder();
    if (nodeLevel <= 5) {
      char level = (char) (ZERO + nodeLevel);
      String prefix =
          builder.append(RockDBConstants.ROOT).append(PATH_SEPARATOR).append(level).toString();
      paths = readWriteHandler.getAllByPrefix(prefix);
    } else {
      paths = ConcurrentHashMap.newKeySet();
      char upperLevel = (char) (ZERO + nodeLevel - 1);
      String prefix =
          builder.append(RockDBConstants.ROOT).append(PATH_SEPARATOR).append(upperLevel).toString();
      Set<String> parentPaths = readWriteHandler.getAllByPrefix(prefix);
      parentPaths
          .parallelStream()
          .forEach(
              x -> {
                String targetPrefix = RocksDBUtils.getNextLevelOfPath(x, upperLevel);
                paths.addAll(readWriteHandler.getAllByPrefix(targetPrefix));
              });
    }
    return RocksDBUtils.convertToPartialPath(paths, nodeLevel);
  }

  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, MManager.StorageGroupFilter filter)
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
    Set<String> result = new HashSet<>();
    String innerNameByLevel =
        RocksDBUtils.getLevelPath(
            pathPattern.getNodes(),
            pathPattern.getNodeLength() - 1,
            pathPattern.getNodeLength() + 1);
    Set<String> allKeyByPrefix = readWriteHandler.getKeyByPrefix(innerNameByLevel);
    for (String str : allKeyByPrefix) {
      result.add(RocksDBUtils.getPathByInnerName(str));
    }
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
      childName.add(str.substring(str.lastIndexOf(RockDBConstants.PATH_SEPARATOR) + 1));
    }
    return childName;
  }
  // endregion

  // region Interfaces for StorageGroup and TTL info Query
  @Override
  public boolean isStorageGroup(PartialPath path) throws MetadataException {
    int level = RocksDBUtils.getLevelByPartialPath(path.getFullPath());
    String innerPathName =
        RocksDBUtils.convertPartialPathToInner(
            path.getFullPath(), level, RockDBConstants.NODE_TYPE_SG);
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
            path.getNodes(), RockDBConstants.NODE_TYPE_SG);
    if (innerPathName == null) {
      throw new StorageGroupNotSetException(
          String.format("Cannot find [%s] belong to which storage group.", path.getFullPath()));
    }
    return new PartialPath(RocksDBUtils.getPathByInnerName(innerPathName));
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
          allResult.add(RocksDBUtils.getPathByInnerName(new String(a)));
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
    List<PartialPath> allStorageGroupPath = new ArrayList<>();
    Set<String> allStorageGroupInnerName =
        readWriteHandler.getKeyByPrefix(String.valueOf(NODE_TYPE_SG));
    for (String str : allStorageGroupInnerName) {
      try {
        allStorageGroupPath.add(new PartialPath(RocksDBUtils.getPathByInnerName(str)));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
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
    getMatchedPathByNodeType(pathPattern.getNodes(), new Character[] {NODE_TYPE_SG}, allPath);
    return allPath;
  }

  private Set<PartialPath> getMatchedPathWithNodeType(
      boolean isPrefixMatch, PartialPath pathPattern, char nodeType) throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    Map<String, byte[]> allMeasurement = getKeyNumByPrefix(pathPattern, nodeType, isPrefixMatch);
    for (Entry<String, byte[]> entry : allMeasurement.entrySet()) {
      try {
        PartialPath path = new PartialPath(RocksDBUtils.getPathByInnerName(entry.getKey()));
        result.add(path);
      } catch (ClassCastException | IllegalPathException e) {
        throw new MetadataException(e);
      }
    }
    return result;
  }

  /**
   * Get all device paths and according storage group paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult.
   */
  @Override
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    return null;
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
              new RMeasurementMNode(RocksDBUtils.getPathByInnerName(new String(a)), b)
                  .getMeasurementPath());
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
    List<Pair<PartialPath, String[]>> ans;
    if (plan.isOrderByHeat()) {
      ans = Collections.emptyList();
    } else {
      ans = getAllMeasurementSchema(plan);
    }
    List<ShowTimeSeriesResult> res = new LinkedList<>();
    for (Pair<PartialPath, String[]> ansString : ans) {
      Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
          new Pair<>(Collections.emptyMap(), Collections.emptyMap());
      res.add(
          new ShowTimeSeriesResult(
              ansString.left.getFullPath(),
              ansString.right[0],
              ansString.right[1],
              TSDataType.valueOf(ansString.right[2]),
              TSEncoding.valueOf(ansString.right[3]),
              CompressionType.valueOf(ansString.right[4]),
              ansString.right[6] != null ? Long.parseLong(ansString.right[6]) : 0,
              tagAndAttributePair.left,
              tagAndAttributePair.right));
    }
    return res;
  }

  private List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    List<MeasurementPath> measurementPaths = getMatchedMeasurementPath(plan.getPath().getNodes());
    List<Pair<PartialPath, String[]>> result = Collections.synchronizedList(new LinkedList<>());
    measurementPaths
        .parallelStream()
        .forEach(
            measurementPath -> {
              String[] tsRow = new String[7];
              // todo need update these properties
              tsRow[0] = measurementPath.getMeasurementAlias();
              // sg name
              tsRow[1] = measurementPath.getFullPath();
              tsRow[2] = measurementPath.getMeasurementSchema().getType().toString();
              tsRow[3] = measurementPath.getMeasurementSchema().getEncodingType().toString();
              tsRow[4] = measurementPath.getMeasurementSchema().getCompressor().toString();
              tsRow[5] = String.valueOf(0);
              tsRow[6] = null;
              Pair<PartialPath, String[]> temp = new Pair<>(measurementPath, tsRow);
              result.add(temp);
            });

    return result;
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
          RocksDBUtils.getLevelPath(fullPath.getNodes(), fullPath.getNodeLength() - 1);
      Holder<byte[]> holder = new Holder<>();
      if (readWriteHandler.keyExistByType(levelKey, RocksDBMNodeType.MEASUREMENT, holder)) {
        IMeasurementSchema schema =
            (MeasurementSchema)
                RocksDBUtils.parseNodeValue(holder.getValue(), DATA_BLOCK_TYPE_SCHEMA);
        return schema;
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
        RocksDBUtils.convertPartialPathToInner(
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
          (MeasurementSchema) RocksDBUtils.parseNodeValue(entry.getValue(), FLAG_IS_SCHEMA);
      result.add(new MeasurementPath(pathName, measurementSchema));
    }
    return result;
  }

  @Override
  public Map<PartialPath, IMeasurementSchema> getAllMeasurementSchemaByPrefix(
      PartialPath prefixPath) throws MetadataException {
    Map<PartialPath, IMeasurementSchema> result = new HashMap<>();
    Map<String, byte[]> allMeasurement = getKeyNumByPrefix(prefixPath, NODE_TYPE_MEASUREMENT, true);
    for (Entry<String, byte[]> entry : allMeasurement.entrySet()) {
      try {
        MeasurementSchema schema =
            (MeasurementSchema) RocksDBUtils.parseNodeValue(entry.getValue(), FLAG_IS_SCHEMA);
        PartialPath path = new PartialPath(RocksDBUtils.getPathByInnerName(entry.getKey()));
        result.put(path, schema);
      } catch (ClassCastException e) {
        throw new MetadataException(e);
      }
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
    IStorageGroupMNode node = null;
    try {
      String[] nodes = path.getNodes();
      for (int i = 1; i < nodes.length; i++) {
        String levelPath = RocksDBUtils.getLevelPath(nodes, i);
        Holder<byte[]> holder = new Holder<>();
        if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.STORAGE_GROUP, holder)) {
          Object ttl = RocksDBUtils.parseNodeValue(holder.getValue(), RockDBConstants.FLAG_SET_TTL);
          if (ttl == null) {
            ttl = config.getDefaultTTL();
          }
          node =
              new RStorageGroupMNode(
                  MetaUtils.getStorageGroupPathByLevel(path, i).getFullPath(), (Long) ttl);
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
      Object ttl = RocksDBUtils.parseNodeValue(iterator.value(), FLAG_SET_TTL);
      if (ttl == null) {
        ttl = config.getDefaultTTL();
      }
      result.add(
          new RStorageGroupMNode(
              RocksDBUtils.getPathByInnerName(new String(iterator.key())), (Long) ttl));
    }
    return result;
  }

  @Override
  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    String levelPath = RocksDBUtils.getLevelPath(nodes, nodes.length - 1);
    Holder<byte[]> holder = new Holder<>();
    try {
      if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.ENTITY, holder)) {
        return new REntityMNode(path.getFullPath(), holder.getValue());
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
    String key = RocksDBUtils.getLevelPath(nodes, nodes.length - 1);
    IMeasurementMNode node = null;
    try {
      Holder<byte[]> holder = new Holder<>();
      if (readWriteHandler.keyExistByType(key, RocksDBMNodeType.MEASUREMENT, holder)) {
        node = new RMeasurementMNode(fullPath.getFullPath(), holder.getValue());
      }
      return node;
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for alias and tag/attribute operations
  @Override
  public void changeAlias(PartialPath path, String alias) throws MetadataException, IOException {
    upsertTagsAndAttributes(alias, null, null, path);
  }

  @Override
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath path)
      throws MetadataException, IOException {
    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] originKey = RocksDBUtils.toMeasurementNodeKey(levelPath);
    try {
      Lock rawKeyLock = locksPool.get(levelPath);
      if (rawKeyLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          String[] nodes = path.getNodes();
          RMeasurementMNode mNode = (RMeasurementMNode) getMeasurementMNode(path);
          // upsert alias
          if (StringUtils.isEmpty(mNode.getAlias()) || !mNode.getAlias().equals(alias)) {
            WriteBatch batch = new WriteBatch();
            String[] newAlias = Arrays.copyOf(nodes, nodes.length);
            newAlias[nodes.length - 1] = alias;
            String newAliasLevel = RocksDBUtils.getLevelPath(newAlias, newAlias.length - 1);
            byte[] newAliasKey = RocksDBUtils.toAliasNodeKey(newAliasLevel);

            Lock newAliasLock = locksPool.get(newAliasLevel);
            Lock oldAliasLock = null;
            boolean lockedOldAlias = false;
            try {
              if (newAliasLock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                if (readWriteHandler.keyExistByAllTypes(newAliasLevel).existAnyKey()) {
                  throw new PathAlreadyExistException("Alias node has exist: " + newAliasLevel);
                }
                batch.put(newAliasKey, RocksDBUtils.buildAliasNodeValue(originKey));
              } else {
                throw new AcquireLockTimeoutException("acquire lock timeout: " + newAliasLevel);
              }

              if (StringUtils.isNotEmpty(mNode.getAlias()) && !mNode.getAlias().equals(alias)) {
                String[] oldAlias = Arrays.copyOf(nodes, nodes.length);
                oldAlias[nodes.length - 1] = mNode.getAlias();
                String oldAliasLevel = RocksDBUtils.getLevelPath(oldAlias, oldAlias.length - 1);
                byte[] oldAliasKey = RocksDBUtils.toAliasNodeKey(oldAliasLevel);
                oldAliasLock = locksPool.get(oldAliasLevel);
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
              // TODO: need application lock
              readWriteHandler.executeBatch(batch);
            } finally {
              newAliasLock.unlock();
              if (oldAliasLock != null && lockedOldAlias) {
                oldAliasLock.unlock();
              }
            }
          }

          WriteBatch batch = new WriteBatch();
          boolean hasUpdate = false;
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
    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (!readWriteHandler.keyExist(key, holder)) {
            throw new PathNotExistException(path.getFullPath());
          }

          byte[] originValue = holder.getValue();
          RMeasurementMNode mNode = new RMeasurementMNode(path.getFullPath(), originValue);
          if (mNode.getAttributes() != null) {
            for (Map.Entry<String, String> entry : attributesMap.entrySet()) {
              if (mNode.getAttributes().containsKey(entry.getKey())) {
                throw new MetadataException(
                    String.format("TimeSeries [%s] already has the attribute [%s].", path, key));
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

    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (!readWriteHandler.keyExist(key, holder)) {
            throw new PathNotExistException(path.getFullPath());
          }
          byte[] originValue = holder.getValue();
          RMeasurementMNode mNode = new RMeasurementMNode(path.getFullPath(), originValue);
          boolean hasTags = false;
          if (mNode.getTags() != null && mNode.getTags().size() > 0) {
            hasTags = true;
            for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
              if (mNode.getTags().containsKey(entry.getKey())) {
                throw new MetadataException(
                    String.format("TimeSeries [%s] already has the tag [%s].", path, key));
              }
              tagsMap.putAll(mNode.getTags());
            }
          }
          mNode.setTags(tagsMap);
          WriteBatch batch = new WriteBatch();
          if (!hasTags) {
            batch.put(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), key, DEFAULT_NODE_VALUE);
          }
          batch.put(key, mNode.getRocksDBValue());
          // TODO: need application lock
          readWriteHandler.executeBatch(batch);
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
    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (!readWriteHandler.keyExist(key, holder)) {
            throw new PathNotExistException(path.getFullPath());
          }

          byte[] originValue = holder.getValue();
          RMeasurementMNode mNode = new RMeasurementMNode(path.getFullPath(), originValue);
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
            WriteBatch batch = new WriteBatch();
            if (tagLen > 0 && mNode.getTags().size() <= 0) {
              batch.delete(readWriteHandler.getCFHByName(TABLE_NAME_TAGS), key);
            }
            batch.put(key, mNode.getRocksDBValue());
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
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath path)
      throws MetadataException, IOException {
    if (alterMap == null || alterMap.isEmpty()) {
      return;
    }

    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          byte[] originValue = holder.getValue();
          RMeasurementMNode mNode = new RMeasurementMNode(path.getFullPath(), originValue);
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
                  String.format("TimeSeries [%s] does not have tag/attribute [%s].", path, key),
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

    String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
    byte[] nodeKey = RocksDBUtils.toMeasurementNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      Lock lock = locksPool.get(levelPath);
      if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
        try {
          if (!readWriteHandler.keyExist(nodeKey, holder)) {
            throw new PathNotExistException(path.getFullPath());
          }
          byte[] originValue = holder.getValue();
          RMeasurementMNode mNode = new RMeasurementMNode(path.getFullPath(), originValue);
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
      if (plan.isAligned() && deviceMNode.getAsEntityMNode().isAligned()) {
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
        missingNodeIndex.put(i, path);
      } else {
        nodeMap.put(i, node);
      }
    }

    // create missing nodes
    if (!missingNodeIndex.isEmpty()) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw new PathNotExistException(devicePath + PATH_SEPARATOR);
      }

      if (!(plan instanceof InsertRowPlan) && !(plan instanceof InsertTabletPlan)) {
        throw new MetadataException(
            String.format(
                "Only support insertRow and insertTablet, plan is [%s]", plan.getOperatorType()));
      }

      if (plan.isAligned()) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        for (Integer index : missingNodeIndex.keySet()) {
          measurements.add(measurementList[index]);
          dataTypes.add(plan.getDataTypes()[index]);
        }
        createAlignedTimeSeries(devicePath, measurements, dataTypes, null, null);
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
            MetaUtils.checkDataTypeMatch(plan, i, nodeMap.get(i).getSchema().getType());
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

  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param devicePath path
   */
  protected IMNode getDeviceNodeWithAutoCreate(PartialPath devicePath, boolean aligned)
      throws MetadataException {
    IMNode node;
    try {
      node = getDeviceNode(devicePath);
      return node;
    } catch (PathNotExistException e) {
      int sgIndex = ensureStorageGroup(devicePath, devicePath.getNodeLength() - 1);
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

  private void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) {
    throw new UnsupportedOperationException();
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
  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) {
    throw new UnsupportedOperationException();
  }
  // endregion

  // start test only region
  @TestOnly
  public void printScanAllKeys() throws IOException {
    readWriteHandler.scanAllKeys(config.getSystemDir() + "/" + "rocksdb.key");
  }

  @TestOnly
  public void close() {
    readWriteHandler.close();
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

  @TestOnly
  public void flushAllMlogForTest() {
    throw new UnsupportedOperationException();
  }
  // end region
}
