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
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
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
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

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
    int sgIndex = ensureStorageGroup(path, path.getNodeLength() - 2);

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
    Holder<byte[]> holder = new Holder<>();
    Lock lock = locksPool.get(levelPath);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      Thread.sleep(5);
      lockedLocks.push(lock);
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
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
            throw new PathAlreadyExistException("Measurement node already exists");
          }

          if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
              || checkResult.getResult(RocksDBMNodeType.ALISA)) {
            throw new PathAlreadyExistException("Path contains measurement node");
          }

          if (start == nodes.length - 1) {
            if (checkResult.getResult(RocksDBMNodeType.INTERNAL)) {
              // convert the parent node to entity if it is internal node
              readWriteHandler.convertToEntityNode(levelPath, DEFAULT_NODE_VALUE);
            } else if (checkResult.getResult(RocksDBMNodeType.ENTITY)) {
              if ((holder.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
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
      throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
            throw new MetadataException("acquire lock timeout: " + lockKey);
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
    Holder<byte[]> holder = new Holder<>();
    Lock lock = locksPool.get(levelPath);
    if (lock.tryLock(MAX_LOCK_WAIT_TIME, TimeUnit.MILLISECONDS)) {
      try {
        CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
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

            if ((holder.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
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
      throw new MetadataException("acquire lock timeout: " + levelPath);
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
          throw new MetadataException("acquire lock timeout, " + p.getFullPath());
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
            throw new MetadataException("acquire lock timeout, " + curNode.getFullPath());
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
          throw new MetadataException("acquire lock timeout: " + levelKey);
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
                storageGroupDeletingFlagMap.put(path.getFullPath(), false);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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

    return getKeyNumByPrefix(pathPattern, NODE_TYPE_MEASUREMENT, isPrefixMatch).size();
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
    return getKeyNumByPrefix(pathPattern, NODE_TYPE_ENTITY, isPrefixMatch).size();
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
    return getKeyNumByPrefix(pathPattern, NODE_TYPE_SG, isPrefixMatch).size();
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
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String key = RocksDBUtils.getLevelPath(nodes, i);
      byte[] value;
      try {
        if ((value = readWriteHandler.get(null, key.getBytes())) != null) {
          return value.length > 0 && value[0] == NODE_TYPE_SG;
        }
      } catch (RocksDBException e) {
        throw new MetadataException(e);
      }
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
    return new ArrayList<>(getMatchedPathWithNodeType(isPrefixMatch, pathPattern, NODE_TYPE_SG));
  }

  /** Get all storage group paths */
  @Override
  public List<PartialPath> getAllStorageGroupPaths() throws IllegalPathException {
    List<PartialPath> allStorageGroupPath = new ArrayList<>();
    Set<String> allStorageGroupInnerName =
        readWriteHandler.getKeyByPrefix(String.valueOf(NODE_TYPE_SG));
    for (String str : allStorageGroupInnerName) {
      allStorageGroupPath.add(new PartialPath(RocksDBUtils.getPathByInnerName(str)));
    }
    return allStorageGroupPath;
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  @Override
  public Map<PartialPath, Long> getStorageGroupsTTL() throws IllegalPathException {
    Map<PartialPath, Long> allStorageGroupAndTTL = new HashMap<>();
    RocksIterator iterator = readWriteHandler.iterator(null);

    for (iterator.seek(new byte[] {NODE_TYPE_SG}); iterator.isValid(); iterator.next()) {
      if (iterator.key()[0] != (NODE_TYPE_SG)) {
        break;
      }
      byte[] value = iterator.value();
      String key = new String(iterator.key());
      Object ttl = RocksDBUtils.parseNodeValue(value, RockDBConstants.FLAG_SET_TTL);
      // initialize a value
      if (ttl == null) {
        ttl = 0L;
      }
      allStorageGroupAndTTL.put(new PartialPath(RocksDBUtils.getPathByInnerName(key)), (Long) ttl);
    }
    return allStorageGroupAndTTL;
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
    return getMatchedPathWithNodeType(isPrefixMatch, pathPattern, NODE_TYPE_ENTITY);
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
    List<MeasurementPath> result = new ArrayList<>();
    Map<String, byte[]> allMeasurement =
        getKeyNumByPrefix(pathPattern, NODE_TYPE_MEASUREMENT, isPrefixMatch);
    for (Entry<String, byte[]> entry : allMeasurement.entrySet()) {
      try {
        MeasurementSchema schema =
            (MeasurementSchema) RocksDBUtils.parseNodeValue(entry.getValue(), FLAG_IS_SCHEMA);
        PartialPath path = new PartialPath(RocksDBUtils.getPathByInnerName(entry.getKey()));
        result.add(new MeasurementPath(path, schema));
      } catch (ClassCastException e) {
        throw new MetadataException(e);
      }
    }
    return result;
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
    return null;
  }

  @Override
  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    return null;
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
      byte[] value =
          readWriteHandler.get(
              null,
              RocksDBUtils.convertPartialPathToInner(
                      fullPath.getFullPath(), fullPath.getNodeLength(), NODE_TYPE_MEASUREMENT)
                  .getBytes());
      if (value == null) {
        throw new MetadataException("can not find this measurement:" + fullPath.getFullPath());
      }
      Object schema = RocksDBUtils.parseNodeValue(value, DATA_BLOCK_TYPE_SCHEMA);
      if (schema != null) {
        return (MeasurementSchema) schema;
      } else {
        throw new MetadataException(
            String.format("Schema of this measurement [%s] is null !", fullPath.getFullPath()));
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
    String innerName =
        RocksDBUtils.convertPartialPathToInner(
            path.getFullPath(), path.getNodeLength(), NODE_TYPE_SG);
    byte[] value;
    try {
      value = readWriteHandler.get(null, innerName.getBytes());
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
    if (value == null) {
      throw new StorageGroupNotSetException(
          String.format("Can not find storage group by path : %s", path.getFullPath()));
    }
    Object ttl = RocksDBUtils.parseNodeValue(value, RockDBConstants.FLAG_SET_TTL);
    if (ttl == null) {
      ttl = 0L;
    }
    return new RStorageGroupMNode(path.getFullPath(), (Long) ttl);
  }

  /** Get storage group node by path. the give path don't need to be storage group path. */
  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    byte[] innerPathName;
    int i;
    byte[] value = null;
    for (i = nodes.length; i > 0; i--) {
      String[] copy = Arrays.copyOf(nodes, i);
      innerPathName =
          RocksDBUtils.toStorageNodeKey(RocksDBUtils.getLevelPath(copy, copy.length - 1));
      try {
        value = readWriteHandler.get(null, innerPathName);
      } catch (RocksDBException e) {
        throw new MetadataException(e);
      }
      if (value != null) {
        break;
      }
    }
    if (value == null) {
      throw new StorageGroupNotSetException(
          String.format("Cannot find the storage group by %s.", path.getFullPath()));
    }

    Object ttl = RocksDBUtils.parseNodeValue(value, RockDBConstants.FLAG_SET_TTL);
    if (ttl == null) {
      ttl = 0L;
    }
    return new RStorageGroupMNode(path.getFullPath(), (Long) ttl);
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
        ttl = 0L;
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
    byte[] innerPathName;
    int i;
    boolean isDevice = false;
    for (i = nodes.length; i > 0; i--) {
      String[] copy = Arrays.copyOf(nodes, i);
      innerPathName =
          RocksDBUtils.toEntityNodeKey(RocksDBUtils.getLevelPath(copy, copy.length - 1));
      try {
        isDevice = readWriteHandler.keyExist(innerPathName);
      } catch (RocksDBException e) {
        throw new MetadataException(e);
      }
      if (isDevice) {
        break;
      }
    }
    if (!isDevice) {
      throw new StorageGroupNotSetException(
          String.format("Cannot find the storage group by %s.", path.getFullPath()));
    }
    return new REntityMNode(path.getFullPath());
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
    try {
      byte[] value = readWriteHandler.get(null, key.getBytes());
      if (value == null) {
        logger.warn("path not exist: {}", key);
        throw new MetadataException("key not exist");
      }
      return new RMeasurementMNode(fullPath.getFullPath());
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for alias and tag/attribute operations
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
                throw new MetadataException("acquire lock timeout: " + newAliasLevel);
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
                  throw new MetadataException("acquire lock timeout: " + oldAliasLevel);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
        throw new MetadataException("acquire lock timeout: " + levelPath);
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
    return null;
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
  // end region
}
