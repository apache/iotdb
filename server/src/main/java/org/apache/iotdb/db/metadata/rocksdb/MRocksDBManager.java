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
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_VERSION;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_ENTITY_NODE_VALUE;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_FLAG;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_INTERNAL_NODE_VALUE;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_SG_NODE_VALUE;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_IS_ALIGNED;
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

  private RocksDBReadWriteHandler readWriteHandler;

  public MRocksDBManager() throws MetadataException {
    try {
      readWriteHandler = new RocksDBReadWriteHandler();
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
    throw new UnsupportedOperationException();
  }

  @TestOnly
  @Override
  public void clear() {}

  @Override
  public void operation(PhysicalPlan plan) throws IOException, MetadataException {}
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

  private void createTimeSeries(
      PartialPath path,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws MetadataException {
    // regular check
    MetaFormatUtils.checkTimeseries(path);
    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), schema.getProps());

    // sg check and create
    String[] nodes = path.getNodes();
    SchemaUtils.checkDataTypeWithEncoding(schema.getType(), schema.getEncodingType());
    int sgIndex = ensureStorageGroup(path, path.getNodeLength() - 2);

    try {
      createTimeSeriesRecursively(nodes, nodes.length, schema, alias, tags, attributes);
      // TODO: insert node to tag table
      // TODO: load tags to memory
    } catch (RocksDBException | InterruptedException | IOException e) {
      throw new MetadataException(e);
    }
  }

  private void createTimeSeriesRecursively(
      String nodes[],
      int start,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws InterruptedException, MetadataException, RocksDBException, IOException {
    if (start <= 1) {
      // nodes "root" must exist and don't need to check
      return;
    }
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Holder<byte[]> holder = new Holder<>();
    CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
    if (!checkResult.existAnyKey()) {
      createTimeSeriesRecursively(nodes, start - 1, schema, alias, tags, attributes);
      if (start == nodes.length) {
        createTimeSeriesNode(nodes, levelPath, schema, alias, tags, attributes);
      } else if (start == nodes.length - 1) {
        // create entity node
        try {
          readWriteHandler.createNode(
              levelPath, RocksDBMNodeType.ENTITY, DEFAULT_ENTITY_NODE_VALUE);
        } catch (PathAlreadyExistException e) {
          Holder<byte[]> tempHolder = new Holder<>();
          if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.ENTITY, tempHolder)) {
            logger.info("Entity node created by another thread: {}", levelPath);
          } else {
            throw new PathAlreadyExistException("Entity type node is expected for " + levelPath);
          }
        }
      } else {
        // create internal node
        try {
          readWriteHandler.createNode(
              levelPath, RocksDBMNodeType.INTERNAL, DEFAULT_INTERNAL_NODE_VALUE);
        } catch (PathAlreadyExistException e) {
          Holder<byte[]> tempHolder = new Holder<>();
          if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.INTERNAL, tempHolder)) {
            logger.info("Internal node created by another thread: {}", levelPath);
          } else {
            throw new PathAlreadyExistException("Internal type node is expected for " + levelPath);
          }
        }
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
          WriteBatch batch = new WriteBatch();
          byte[] internalKey = RocksDBUtils.toInternalNodeKey(levelPath);
          byte[] entityKey = RocksDBUtils.toEntityNodeKey(levelPath);
          batch.delete(internalKey);
          batch.put(entityKey, DEFAULT_ENTITY_NODE_VALUE);
          readWriteHandler.convertToEntityNode(levelPath, entityKey, batch);
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
  }

  private void createTimeSeriesNode(
      String[] nodes,
      String levelPath,
      MeasurementSchema schema,
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
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ReadWriteIOUtils.write(tags, outputStream);
      batch.put(
          readWriteHandler.getCFHByName(TABLE_NAME_TAGS),
          measurementKey,
          outputStream.toByteArray());
    }

    if (StringUtils.isNotEmpty(alias)) {
      String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
      aliasNodes[nodes.length - 1] = alias;
      String aliasLevelPath = RocksDBUtils.getLevelPath(aliasNodes, aliasNodes.length - 1);
      byte[] aliasNodeKey = RocksDBUtils.toAliasNodeKey(aliasLevelPath);
      if (!readWriteHandler.keyExist(aliasNodeKey)) {
        batch.put(aliasNodeKey, RocksDBUtils.buildAliasNodeValue(measurementKey));
        readWriteHandler.batchCreateTwoKeys(levelPath, aliasLevelPath, batch);
      } else {
        throw new AliasAlreadyExistException(levelPath, alias);
      }
    } else {
      readWriteHandler.batchCreateOneKey(levelPath, measurementKey, batch);
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

    for (int i = 0; i < measurements.size(); i++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      MetaFormatUtils.checkNodeName(measurements.get(i));
    }

    int sgIndex = ensureStorageGroup(prefixPath, prefixPath.getNodeLength() - 1);

    try {
      createEntityRecursively(prefixPath.getNodes(), prefixPath.getNodeLength(), sgIndex + 1, true);
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
      readWriteHandler.batchCreateWithLocks(locks, batch);
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
  private void createEntityRecursively(String[] nodes, int start, int end, boolean aligned)
      throws RocksDBException, MetadataException, InterruptedException {
    if (start <= end) {
      // nodes before "end" must exist
      return;
    }
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Holder<byte[]> holder = new Holder<>();
    CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
    if (!checkResult.existAnyKey()) {
      createEntityRecursively(nodes, start - 1, end, aligned);
      if (start == nodes.length) {
        byte[] nodeKey = RocksDBUtils.toEntityNodeKey(levelPath);
        byte[] value =
            aligned
                ? new byte[] {DATA_VERSION, FLAG_IS_ALIGNED}
                : new byte[] {DATA_VERSION, DEFAULT_FLAG};
        readWriteHandler.createNode(levelPath, nodeKey, value);
      } else {
        readWriteHandler.createNode(
            levelPath, RocksDBMNodeType.INTERNAL, DEFAULT_ENTITY_NODE_VALUE);
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
  }

  @Override
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    throw new UnsupportedOperationException();
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
    MetaFormatUtils.checkStorageGroup(storageGroup.getFullPath());
    String[] nodes = storageGroup.getNodes();
    try {
      int len = nodes.length;
      for (int i = 1; i < nodes.length; i++) {
        String levelKey = RocksDBUtils.getLevelPath(nodes, i);
        CheckKeyResult keyCheckResult = readWriteHandler.keyExistByAllTypes(levelKey);
        if (!keyCheckResult.existAnyKey()) {
          if (i < len - 1) {
            readWriteHandler.createNode(
                levelKey, RocksDBMNodeType.INTERNAL, DEFAULT_INTERNAL_NODE_VALUE);
          } else {
            readWriteHandler.createNode(
                levelKey, RocksDBMNodeType.STORAGE_GROUP, DEFAULT_SG_NODE_VALUE);
          }
        } else if (keyCheckResult.getResult(RocksDBMNodeType.STORAGE_GROUP)) {
          throw new StorageGroupAlreadySetException(storageGroup.toString());
        }
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    String levelPath =
        RocksDBUtils.getLevelPath(storageGroup.getNodes(), storageGroup.getNodeLength() - 1);
    byte[] pathKey = RocksDBUtils.toStorageNodeKey(levelPath);
    Holder<byte[]> holder = new Holder<>();
    try {
      if (readWriteHandler.keyExist(pathKey, holder)) {
        byte[] value = RocksDBUtils.updateTTL(holder.getValue(), dataTTL);
        readWriteHandler.updateNode(levelPath, pathKey, value);
      } else {
        throw new PathNotExistException("Storage group node of path doesn't exist: " + levelPath);
      }
    } catch (InterruptedException | RocksDBException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for get and auto create device
  // endregion

  // region Interfaces for metadata info Query
  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    String innerPathName = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength());
    try {
      return readWriteHandler.keyExist(RocksDBUtils.toStorageNodeKey(innerPathName));
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public String getMetadataInString() {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  @Override
  public long getTotalSeriesNumber() {
    return readWriteHandler.countNodesNumByType(null, RockDBConstants.NODE_TYPE_MEASUREMENT);
  }

  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getKeyNumByPrefix(pathPattern, NODE_TYPE_MEASUREMENT);
  }

  private int getKeyNumByPrefix(PartialPath pathPattern, byte nodeType) throws MetadataException {
    AtomicInteger counter = new AtomicInteger(0);
    Set<String> seeds = new HashSet<>();

    String seedPath;

    int nonWildcardAvailablePosition =
        pathPattern.getFullPath().indexOf(ONE_LEVEL_PATH_WILDCARD) - 1;
    if (nonWildcardAvailablePosition < 0) {
      seedPath = RocksDBUtils.getLevelPath(pathPattern.getNodes(), pathPattern.getNodeLength());
    } else {
      seedPath = pathPattern.getFullPath().substring(0, nonWildcardAvailablePosition);
    }

    seeds.add(seedPath);
    scanAllKeysRecursively(
        seeds,
        0,
        s -> {
          try {
            byte[] value = readWriteHandler.get(null, s.getBytes());
            if (value != null && value.length > 0 && value[0] == nodeType) {
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

  private Set<String> getAllByPrefix(String prefix) {
    Set<String> result = new HashSet<>();
    byte[] prefixKey = prefix.getBytes();
    RocksIterator iterator = readWriteHandler.iterator(null);
    for (iterator.seek(prefixKey); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      if (!key.startsWith(prefix)) {
        break;
      }
      result.add(key);
    }
    return result;
  }

  private String getNextLevelOfPath(String innerPath, int currentLevel) {
    char levelChar = (char) (ZERO + currentLevel);
    String old = PATH_SEPARATOR + levelChar;
    String target = PATH_SEPARATOR + (char) (levelChar + 1);
    return innerPath.replace(old, target);
  }

  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  @Override
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getKeyNumByPrefix(pathPattern, NODE_TYPE_ENTITY);
  }

  @Override
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
  }

  @Override
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getKeyNumByPrefix(pathPattern, NODE_TYPE_SG);
  }

  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    String innerNameByLevel =
        RocksDBUtils.getLevelPath(pathPattern.getNodes(), pathPattern.getNodeLength(), level);
    return readWriteHandler.getKeyByPrefix(innerNameByLevel).size();
  }

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
  @Override
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, MManager.StorageGroupFilter filter)
      throws MetadataException {
    return getNodesListInGivenLevel(pathPattern, nodeLevel);
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException {
    Set<String> result = new HashSet<>();
    String innerNameByLevel =
        RocksDBUtils.getLevelPath(
            pathPattern.getNodes(), pathPattern.getNodeLength(), pathPattern.getNodeLength() + 1);
    Set<String> allKeyByPrefix = readWriteHandler.getKeyByPrefix(innerNameByLevel);
    for (String str : allKeyByPrefix) {
      result.add(RocksDBUtils.getPathByInnerName(str));
    }
    return result;
  }

  @Override
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    Set<String> result = new HashSet<>();
    String innerNameByLevel =
        RocksDBUtils.getLevelPath(
            pathPattern.getNodes(), pathPattern.getNodeLength(), pathPattern.getNodeLength() + 1);
    Set<String> allKeyByPrefix = readWriteHandler.getKeyByPrefix(innerNameByLevel);
    for (String str : allKeyByPrefix) {
      result.add(RocksDBUtils.getPathByInnerName(str));
    }
    return result;
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

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    return false;
  }

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

  @Override
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return null;
  }

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
  @Override
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    return null;
  }

  @Override
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return null;
  }

  @Override
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    return null;
  }
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query
  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return null;
  }

  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern)
      throws MetadataException {
    return null;
  }

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

  @Override
  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
    return null;
  }

  @Override
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    return null;
  }

  @Override
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    return null;
  }

  @Override
  public Map<PartialPath, IMeasurementSchema> getAllMeasurementSchemaByPrefix(
      PartialPath prefixPath) throws MetadataException {
    return null;
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    String innerName =
        RocksDBUtils.convertPartialPathToInner(
            path.getFullPath(), path.getNodeLength(), NODE_TYPE_SG);
    byte[] value = new byte[0];
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
    return new StorageGroupEntityMNode(null, path.getFullPath(), (Long) ttl);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    byte[] innerPathName;
    int i;
    byte[] value = null;
    for (i = nodes.length; i > 0; i--) {
      String[] copy = Arrays.copyOf(nodes, i);
      innerPathName = RocksDBUtils.toStorageNodeKey(RocksDBUtils.getLevelPath(copy, copy.length));
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
    return new StorageGroupMNode(null, path.getFullPath(), (Long) ttl);
  }

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
          new StorageGroupMNode(
              null, RocksDBUtils.getPathByInnerName(new String(iterator.key())), (Long) ttl));
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
      innerPathName = RocksDBUtils.toEntityNodeKey(RocksDBUtils.getLevelPath(copy, copy.length));
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
    return new EntityMNode(null, path.getFullPath());
  }

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
    return null;
  }
  // endregion

  // region Interfaces for alias and tag/attribute operations
  @Override
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {}

  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {}

  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {}

  @Override
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {}

  @Override
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {}

  @Override
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {}
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
