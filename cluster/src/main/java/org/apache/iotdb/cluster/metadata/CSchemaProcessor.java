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

package org.apache.iotdb.cluster.metadata;

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.MeasurementSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.handlers.caller.ShowTimeSeriesHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClusterQueryUtils;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.BatchPlan;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.LOG_FAIL_CONNECT;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.THREAD_POOL_SIZE;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.waitForThreadPool;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

@SuppressWarnings("java:S1135") // ignore todos
public class CSchemaProcessor extends LocalSchemaProcessor {

  private static final Logger logger = LoggerFactory.getLogger(CSchemaProcessor.class);

  private ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
  // only cache the series who is writing, we need not to cache series who is reading
  // because the read is slow, so pull from remote is little cost comparing to the disk io
  private RemoteMetaCache mRemoteMetaCache;
  private MetaPuller metaPuller;
  private MetaGroupMember metaGroupMember;
  private Coordinator coordinator;

  private CSchemaProcessor() {
    super();
    metaPuller = MetaPuller.getInstance();
    int remoteCacheSize = config.getmRemoteSchemaCacheSize();
    mRemoteMetaCache = new RemoteMetaCache(remoteCacheSize);
  }

  private static class CSchemaProcessorHolder {

    private CSchemaProcessorHolder() {
      // allowed to do nothing
    }

    private static final CSchemaProcessor INSTANCE = new CSchemaProcessor();
  }

  /**
   * we should not use this function in other place, but only in IoTDB class
   *
   * @return
   */
  public static CSchemaProcessor getInstance() {
    return CSchemaProcessorHolder.INSTANCE;
  }

  /**
   * sync meta leader to get the newest partition table and storage groups.
   *
   * @throws MetadataException throws MetadataException if necessary
   */
  public void syncMetaLeader() throws MetadataException {
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    cacheLock.writeLock().lock();
    mRemoteMetaCache.removeItem(pathPattern, isPrefixMatch);
    cacheLock.writeLock().unlock();
    return super.deleteTimeseries(pathPattern, isPrefixMatch);
  }

  @Override
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    cacheLock.writeLock().lock();
    for (PartialPath storageGroup : storageGroups) {
      mRemoteMetaCache.removeItem(storageGroup, true);
    }
    cacheLock.writeLock().unlock();
    super.deleteStorageGroups(storageGroups);
  }

  @Override
  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {

    if (fullPath.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }

    String measurement = fullPath.getMeasurement();
    if (fullPath instanceof AlignedPath) {
      if (((AlignedPath) fullPath).getMeasurementList().size() != 1) {
        return TSDataType.VECTOR;
      } else {
        measurement = ((AlignedPath) fullPath).getMeasurement(0);
      }
    }

    // try remote cache first
    try {
      cacheLock.readLock().lock();
      IMeasurementMNode measurementMNode = mRemoteMetaCache.get(fullPath);
      if (measurementMNode != null) {
        return measurementMNode.getDataType(measurement);
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // try local MTree
    TSDataType seriesType;
    try {
      seriesType = super.getSeriesType(fullPath);
    } catch (PathNotExistException e) {
      // pull from remote node
      List<IMeasurementSchema> schemas =
          metaPuller.pullMeasurementSchemas(Collections.singletonList(fullPath));
      if (!schemas.isEmpty()) {
        IMeasurementSchema measurementSchema = schemas.get(0);
        IMeasurementMNode measurementMNode =
            MeasurementMNode.getMeasurementMNode(
                null, measurementSchema.getMeasurementId(), measurementSchema, null);
        if (measurementSchema instanceof VectorMeasurementSchema) {
          for (String subMeasurement : measurementSchema.getSubMeasurementsList()) {
            cacheMeta(
                new AlignedPath(fullPath.getDeviceIdString(), subMeasurement),
                measurementMNode,
                false);
          }
        } else {
          cacheMeta(fullPath, measurementMNode, true);
        }
        return measurementMNode.getDataType(measurement);
      } else {
        throw e;
      }
    }
    return seriesType;
  }

  @Override
  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    IMeasurementMNode node = null;
    // try remote cache first
    try {
      cacheLock.readLock().lock();
      IMeasurementMNode measurementMNode = mRemoteMetaCache.get(fullPath);
      if (measurementMNode != null) {
        node = measurementMNode;
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    if (node == null) {
      // try local MTree
      try {
        node = super.getMeasurementMNode(fullPath);
      } catch (PathNotExistException e) {
        // pull from remote node
        List<IMeasurementSchema> schemas =
            metaPuller.pullMeasurementSchemas(Collections.singletonList(fullPath));
        if (!schemas.isEmpty()) {
          IMeasurementSchema measurementSchema = schemas.get(0);
          IMeasurementMNode measurementMNode =
              MeasurementMNode.getMeasurementMNode(
                  null, measurementSchema.getMeasurementId(), measurementSchema, null);
          cacheMeta(fullPath, measurementMNode, true);
          node = measurementMNode;
        } else {
          throw e;
        }
      }
    }
    return node;
  }

  /**
   * Get the first index of non-exist schema in the local cache.
   *
   * @return -1 if all schemas are found, or the first index of the non-exist schema
   */
  private int getMNodesLocally(
      PartialPath deviceId, String[] measurements, IMeasurementMNode[] measurementMNodes) {
    int failedMeasurementIndex = -1;
    cacheLock.readLock().lock();
    try {
      for (int i = 0; i < measurements.length && failedMeasurementIndex == -1; i++) {
        IMeasurementMNode measurementMNode =
            mRemoteMetaCache.get(deviceId.concatNode(measurements[i]));
        if (measurementMNode == null) {
          failedMeasurementIndex = i;
        } else {
          measurementMNodes[i] = measurementMNode;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return failedMeasurementIndex;
  }

  private void pullSeriesSchemas(PartialPath deviceId, String[] measurementList)
      throws MetadataException {
    List<PartialPath> schemasToPull = new ArrayList<>();
    for (String s : measurementList) {
      schemasToPull.add(deviceId.concatNode(s));
    }
    List<IMeasurementSchema> schemas = metaPuller.pullMeasurementSchemas(schemasToPull);
    for (IMeasurementSchema schema : schemas) {
      // TODO-Cluster: also pull alias?
      // take care, the pulled schema's measurement Id is only series name
      IMeasurementMNode measurementMNode =
          MeasurementMNode.getMeasurementMNode(null, schema.getMeasurementId(), schema, null);
      cacheMeta(deviceId.concatNode(schema.getMeasurementId()), measurementMNode, true);
    }
    logger.debug("Pulled {}/{} schemas from remote", schemas.size(), measurementList.length);
  }

  /*
  do not set FullPath for Vector subSensor
   */
  @Override
  public void cacheMeta(
      PartialPath seriesPath, IMeasurementMNode measurementMNode, boolean needSetFullPath) {
    if (needSetFullPath) {
      measurementMNode.setFullPath(seriesPath.getFullPath());
    }
    cacheLock.writeLock().lock();
    mRemoteMetaCache.put(seriesPath, measurementMNode);
    cacheLock.writeLock().unlock();
  }

  @Override
  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    cacheLock.writeLock().lock();
    try {
      IMeasurementMNode measurementMNode = mRemoteMetaCache.get(seriesPath);
      if (measurementMNode != null) {
        LastCacheManager.updateLastCache(
            measurementMNode, timeValuePair, highPriorityUpdate, latestFlushedTime);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
    // maybe local also has the timeseries
    super.updateLastCache(seriesPath, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  @Override
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    IMeasurementMNode measurementMNode = mRemoteMetaCache.get(seriesPath);
    if (measurementMNode != null) {
      return LastCacheManager.getLastCache(measurementMNode);
    }

    return super.getLastCache(seriesPath);
  }

  @Override
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[plan.getMeasurements().length];
    int nonExistSchemaIndex =
        getMNodesLocally(plan.getDevicePath(), plan.getMeasurements(), measurementMNodes);
    if (nonExistSchemaIndex == -1) {
      plan.setMeasurementMNodes(measurementMNodes);
      return new InternalMNode(null, plan.getDevicePath().getDeviceIdString());
    }
    // auto-create schema in IoTDBConfig is always disabled in the cluster version, and we have
    // another config in ClusterConfig to do this
    return super.getSeriesSchemasAndReadLockDevice(plan);
  }

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  @Override
  public boolean isPathExist(PartialPath path) {
    boolean localExist = super.isPathExist(path);
    if (localExist) {
      return true;
    }

    // search the cache
    cacheLock.readLock().lock();
    try {
      return mRemoteMetaCache.containsKey(path);
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  private static class RemoteMetaCache extends LRUCache<PartialPath, IMeasurementMNode> {

    RemoteMetaCache(int cacheSize) {
      super(cacheSize);
    }

    @Override
    protected IMeasurementMNode loadObjectByKey(PartialPath key) {
      return null;
    }

    public synchronized void removeItem(PartialPath key, boolean isPrefixMatch) {
      cache.keySet().removeIf(s -> isPrefixMatch ? key.matchPrefixPath(s) : key.matchFullPath(s));
    }

    @Override
    public synchronized void removeItem(PartialPath key) {
      removeItem(key, false);
    }

    @Override
    public synchronized IMeasurementMNode get(PartialPath key) {
      try {
        return super.get(key);
      } catch (IOException e) {
        // not happening
        return null;
      }
    }

    public synchronized boolean containsKey(PartialPath key) {
      return cache.containsKey(key);
    }
  }

  /**
   * create storage groups for CreateTimeseriesPlan, CreateMultiTimeseriesPlan and InsertPlan, also
   * create timeseries for InsertPlan. Only the three kind of plans can use this method.
   */
  public void createSchema(PhysicalPlan plan) throws MetadataException, CheckConsistencyException {
    List<PartialPath> storageGroups = new ArrayList<>();
    // for InsertPlan, try to just use deviceIds to get related storage groups because there's no
    // need to call getPaths to concat deviceId and sensor as they will gain same result,
    // for CreateTimeSeriesPlan, use getPath() to get timeseries to get related storage group,
    // for CreateMultiTimeSeriesPlan, use getPaths() to get all timeseries to get related storage
    // groups.
    if (plan instanceof BatchPlan) {
      storageGroups.addAll(getStorageGroups(getValidStorageGroups((BatchPlan) plan)));
    } else if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
      storageGroups.addAll(
          getStorageGroups(Collections.singletonList(((InsertPlan) plan).getDevicePath())));
    } else if (plan instanceof CreateTimeSeriesPlan) {
      storageGroups.addAll(
          getStorageGroups(Collections.singletonList(((CreateTimeSeriesPlan) plan).getPath())));
    } else if (plan instanceof CreateAlignedTimeSeriesPlan) {
      storageGroups.addAll(
          getStorageGroups(
              Collections.singletonList(((CreateAlignedTimeSeriesPlan) plan).getPrefixPath())));
    } else if (plan instanceof SetTemplatePlan) {
      storageGroups.addAll(
          getStorageGroups(
              Collections.singletonList(
                  new PartialPath(((SetTemplatePlan) plan).getPrefixPath()))));
    } else {
      storageGroups.addAll(getStorageGroups(plan.getPaths()));
    }

    // create storage groups
    createStorageGroups(storageGroups);

    // need to verify the storage group is created
    verifyCreatedSgSuccess(storageGroups, plan);

    // try to create timeseries for insertPlan
    if (plan instanceof InsertPlan && !createTimeseries((InsertPlan) plan)) {
      throw new MetadataException("Failed to create timeseries from InsertPlan automatically.");
    }
  }

  private List<PartialPath> getValidStorageGroups(BatchPlan plan) {
    List<PartialPath> paths = new ArrayList<>();
    List<PartialPath> originalPaths = plan.getPrefixPaths();
    for (int i = 0; i < originalPaths.size(); i++) {
      // has permission to create sg
      if (!plan.getResults().containsKey(i)) {
        paths.add(originalPaths.get(i));
      }
    }
    return paths;
  }

  /** return storage groups paths for given deviceIds or timeseries. */
  private List<PartialPath> getStorageGroups(List<? extends PartialPath> paths)
      throws MetadataException {
    Set<PartialPath> storageGroups = new HashSet<>();
    for (PartialPath path : paths) {
      storageGroups.add(
          MetaUtils.getStorageGroupPathByLevel(
              path, IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel()));
    }
    return new ArrayList<>(storageGroups);
  }

  @SuppressWarnings("squid:S3776")
  private void verifyCreatedSgSuccess(List<PartialPath> storageGroups, PhysicalPlan physicalPlan) {
    long startTime = System.currentTimeMillis();
    boolean[] ready = new boolean[storageGroups.size()];
    Arrays.fill(ready, false);
    while (true) {
      boolean allReady = true;
      for (int i = 0; i < storageGroups.size(); i++) {
        if (ready[i]) {
          continue;
        }
        if (IoTDB.schemaProcessor.isStorageGroup(storageGroups.get(i))) {
          ready[i] = true;
        } else {
          allReady = false;
        }
      }

      if (allReady
          || System.currentTimeMillis() - startTime
              > ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS()) {
        break;
      } else {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          logger.debug("Failed to wait for creating sgs for plan {}", physicalPlan, e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Create storage groups automatically for paths.
   *
   * @param storageGroups the uncreated storage groups
   */
  private void createStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    for (PartialPath storageGroup : storageGroups) {
      SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan(storageGroup);
      TSStatus setStorageGroupResult =
          metaGroupMember.processNonPartitionedMetaPlan(setStorageGroupPlan);
      if (setStorageGroupResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && setStorageGroupResult.getCode()
              != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw new MetadataException(
            String.format(
                "Status Code: %d, failed to set storage group %s",
                setStorageGroupResult.getCode(), storageGroup));
      }
    }
  }

  /**
   * @param insertMultiTabletsPlan the InsertMultiTabletsPlan
   * @return true if all InsertTabletPlan in InsertMultiTabletsPlan create timeseries success,
   *     otherwise false
   */
  public boolean createTimeseries(InsertMultiTabletsPlan insertMultiTabletsPlan)
      throws CheckConsistencyException, IllegalPathException {
    boolean allSuccess = true;
    for (InsertTabletPlan insertTabletPlan : insertMultiTabletsPlan.getInsertTabletPlanList()) {
      boolean success = createTimeseries(insertTabletPlan);
      allSuccess = allSuccess && success;
      if (!success) {
        logger.error(
            "create timeseries for device={} failed, plan={}",
            insertTabletPlan.getDevicePath(),
            insertTabletPlan);
      }
    }
    return allSuccess;
  }

  public boolean createTimeseries(InsertRowsPlan insertRowsPlan)
      throws CheckConsistencyException, IllegalPathException {
    boolean allSuccess = true;
    for (InsertRowPlan insertRowPlan : insertRowsPlan.getInsertRowPlanList()) {
      boolean success = createTimeseries(insertRowPlan);
      allSuccess = allSuccess && success;
      if (!success) {
        logger.error(
            "create timeseries for device={} failed, plan={}",
            insertRowPlan.getDevicePath(),
            insertRowPlan);
      }
    }
    return allSuccess;
  }

  public boolean createTimeseries(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws CheckConsistencyException, IllegalPathException {
    boolean allSuccess = true;
    for (InsertRowPlan insertRowPlan : insertRowsOfOneDevicePlan.getRowPlans()) {
      boolean success = createTimeseries(insertRowPlan);
      allSuccess = allSuccess && success;
      if (!success) {
        logger.error(
            "create timeseries for device={} failed, plan={}",
            insertRowPlan.getDevicePath(),
            insertRowPlan);
      }
    }
    return allSuccess;
  }

  /**
   * Create timeseries automatically for an InsertPlan.
   *
   * @param insertPlan some of the timeseries in it are not created yet
   * @return true of all uncreated timeseries are created
   */
  public boolean createTimeseries(InsertPlan insertPlan)
      throws IllegalPathException, CheckConsistencyException {
    if (insertPlan instanceof InsertMultiTabletsPlan) {
      return createTimeseries((InsertMultiTabletsPlan) insertPlan);
    }

    if (insertPlan instanceof InsertRowsPlan) {
      return createTimeseries((InsertRowsPlan) insertPlan);
    }

    if (insertPlan instanceof InsertRowsOfOneDevicePlan) {
      return createTimeseries((InsertRowsOfOneDevicePlan) insertPlan);
    }

    List<String> seriesList = new ArrayList<>();
    PartialPath deviceId = insertPlan.getDevicePath();
    PartialPath storageGroupName;
    try {
      storageGroupName =
          MetaUtils.getStorageGroupPathByLevel(
              deviceId, IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel());
    } catch (MetadataException e) {
      logger.error("Failed to infer storage group from deviceId {}", deviceId);
      return false;
    }
    for (String measurementId : insertPlan.getMeasurements()) {
      seriesList.add(deviceId.getFullPath() + TsFileConstant.PATH_SEPARATOR + measurementId);
    }
    if (insertPlan.isAligned()) {
      return createAlignedTimeseries(seriesList, insertPlan);
    }
    PartitionGroup partitionGroup =
        metaGroupMember.getPartitionTable().route(storageGroupName.getFullPath(), 0);
    List<String> unregisteredSeriesList = getUnregisteredSeriesList(seriesList, partitionGroup);
    if (unregisteredSeriesList.isEmpty()) {
      return true;
    }
    logger.debug("Unregisterd series of {} are {}", seriesList, unregisteredSeriesList);

    return createTimeseries(unregisteredSeriesList, seriesList, insertPlan);
  }

  private boolean createAlignedTimeseries(List<String> seriesList, InsertPlan insertPlan)
      throws IllegalPathException {
    List<String> measurements = new ArrayList<>();
    for (String series : seriesList) {
      measurements.add((new PartialPath(series)).getMeasurement());
    }

    List<TSDataType> dataTypes = new ArrayList<>(measurements.size());
    List<TSEncoding> encodings = new ArrayList<>(measurements.size());
    List<CompressionType> compressors = new ArrayList<>(measurements.size());
    for (int index = 0; index < measurements.size(); index++) {
      TSDataType dataType;
      if (insertPlan.getDataTypes() != null && insertPlan.getDataTypes()[index] != null) {
        dataType = insertPlan.getDataTypes()[index];
      } else {
        dataType =
            TypeInferenceUtils.getPredictedDataType(
                insertPlan instanceof InsertTabletPlan
                    ? Array.get(((InsertTabletPlan) insertPlan).getColumns()[index], 0)
                    : ((InsertRowPlan) insertPlan).getValues()[index],
                true);
      }
      dataTypes.add(dataType);
      encodings.add(getDefaultEncoding(dataType));
      compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
    }

    CreateAlignedTimeSeriesPlan plan =
        new CreateAlignedTimeSeriesPlan(
            insertPlan.getDevicePath(),
            measurements,
            dataTypes,
            encodings,
            compressors,
            null,
            null,
            null);
    TSStatus result;
    try {
      result = coordinator.processPartitionedPlan(plan);
    } catch (UnsupportedPlanException e) {
      logger.error(
          "Failed to create timeseries {} automatically. Unsupported plan exception {} ",
          plan,
          e.getMessage());
      return false;
    }
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && result.getCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()
        && result.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      logger.error(
          "{} failed to execute create timeseries {}: {}",
          metaGroupMember.getThisNode(),
          plan,
          result);
      return false;
    }
    return true;
  }

  /**
   * create timeseries from paths in "unregisteredSeriesList". If data types are provided by the
   * InsertPlan, use them, otherwise infer the types from the values. Use default encodings and
   * compressions of the corresponding data type.
   */
  private boolean createTimeseries(
      List<String> unregisteredSeriesList, List<String> seriesList, InsertPlan insertPlan)
      throws IllegalPathException {
    List<PartialPath> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (String seriesPath : unregisteredSeriesList) {
      paths.add(new PartialPath(seriesPath));
      int index = seriesList.indexOf(seriesPath);
      TSDataType dataType;
      // use data types in insertPlan if provided, otherwise infer them from the values
      if (insertPlan.getDataTypes() != null && insertPlan.getDataTypes()[index] != null) {
        dataType = insertPlan.getDataTypes()[index];
      } else {
        dataType =
            TypeInferenceUtils.getPredictedDataType(
                insertPlan instanceof InsertTabletPlan
                    ? Array.get(((InsertTabletPlan) insertPlan).getColumns()[index], 0)
                    : ((InsertRowPlan) insertPlan).getValues()[index],
                true);
      }
      dataTypes.add(dataType);
      // use default encoding and compression from the config
      encodings.add(getDefaultEncoding(dataType));
      compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
    }
    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
    plan.setPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setEncodings(encodings);
    plan.setCompressors(compressors);

    TSStatus result;
    try {
      result = coordinator.processPartitionedPlan(plan);
    } catch (UnsupportedPlanException e) {
      logger.error(
          "Failed to create timeseries {} automatically. Unsupported plan exception {} ",
          paths,
          e.getMessage());
      return false;
    }
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && result.getCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()
        && result.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()
        && !(result.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()
            && result.getSubStatus().stream()
                .allMatch(
                    s -> s.getCode() == TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()))) {
      logger.error(
          "{} failed to execute create timeseries {}: {}",
          metaGroupMember.getThisNode(),
          paths,
          result);
      return false;
    }
    return true;
  }

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * To check which timeseries in the input list is unregistered from one node in "partitionGroup".
   */
  private List<String> getUnregisteredSeriesList(
      List<String> seriesList, PartitionGroup partitionGroup) throws CheckConsistencyException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      return getUnregisteredSeriesListLocally(seriesList, partitionGroup);
    } else {
      return getUnregisteredSeriesListRemotely(seriesList, partitionGroup);
    }
  }

  private List<String> getUnregisteredSeriesListLocally(
      List<String> seriesList, PartitionGroup partitionGroup) throws CheckConsistencyException {
    DataGroupMember dataMember =
        ClusterIoTDB.getInstance()
            .getDataGroupEngine()
            .getDataMember(partitionGroup.getHeader(), null, null);
    return dataMember.getLocalQueryExecutor().getUnregisteredTimeseries(seriesList);
  }

  private List<String> getUnregisteredSeriesListRemotely(
      List<String> seriesList, PartitionGroup partitionGroup) {
    for (Node node : partitionGroup) {
      List<String> result = null;
      try {
        result = getUnregisteredSeriesListRemotelyForOneNode(node, seriesList, partitionGroup);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(
            "{}: getting unregistered series list {} ... {} is interrupted from {}",
            metaGroupMember.getName(),
            seriesList.get(0),
            seriesList.get(seriesList.size() - 1),
            node,
            e);
      } catch (Exception e) {
        logger.error(
            "{}: cannot getting unregistered {} and other {} paths from {}",
            metaGroupMember.getName(),
            seriesList.get(0),
            seriesList.get(seriesList.size() - 1),
            node,
            e);
      }
      if (result != null) {
        return result;
      }
    }
    return Collections.emptyList();
  }

  private List<String> getUnregisteredSeriesListRemotelyForOneNode(
      Node node, List<String> seriesList, PartitionGroup partitionGroup)
      throws IOException, TException, InterruptedException {
    List<String> result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      result =
          SyncClientAdaptor.getUnregisteredMeasurements(
              client, partitionGroup.getHeader(), seriesList);
    } else {
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        result = syncDataClient.getUnregisteredTimeseries(partitionGroup.getHeader(), seriesList);
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        syncDataClient.close();
        throw e;
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }
    return result;
  }

  /**
   * Get all devices after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return all paths after removing wildcards in the path.
   */
  @Override
  public Set<PartialPath> getMatchedDevices(PartialPath originPath, boolean isPrefixMatch)
      throws MetadataException {
    Map<String, List<PartialPath>> sgPathMap = groupPathByStorageGroup(originPath);
    Set<PartialPath> ret = getMatchedDevices(sgPathMap, isPrefixMatch);
    logger.debug("The devices of path {} are {}", originPath, ret);
    return ret;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *     storage group added
   * @return a collection of all queried paths
   */
  private List<MeasurementPath> getMatchedPaths(
      Map<String, List<PartialPath>> sgPathMap, boolean withAlias) throws MetadataException {
    List<MeasurementPath> result = new ArrayList<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> remoteGroupPathMap = new HashMap<>();
    for (Entry<String, List<PartialPath>> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      List<PartialPath> paths = sgPathEntry.getValue();
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup =
          metaGroupMember.getPartitionTable().route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        try {
          metaGroupMember
              .getLocalDataMember(partitionGroup.getHeader(), partitionGroup.getRaftId())
              .syncLeader(null);
        } catch (CheckConsistencyException e) {
          logger.warn("Failed to check consistency.", e);
        }
        List<MeasurementPath> allTimeseriesName = new ArrayList<>();
        for (PartialPath path : paths) {
          allTimeseriesName.addAll(getMatchedPathsLocally(path, withAlias));
        }
        logger.debug(
            "{}: get matched paths of {} locally, result {}",
            metaGroupMember.getName(),
            partitionGroup,
            allTimeseriesName);
        result.addAll(allTimeseriesName);
      } else {
        // batch the queries of the same group to reduce communication
        for (PartialPath path : paths) {
          remoteGroupPathMap
              .computeIfAbsent(partitionGroup, p -> new ArrayList<>())
              .add(path.getFullPath());
        }
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry :
        remoteGroupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();
      result.addAll(getMatchedPaths(partitionGroup, pathsToQuery, withAlias));
    }

    return result;
  }

  private List<MeasurementPath> getMatchedPathsLocally(PartialPath partialPath, boolean withAlias)
      throws MetadataException {
    if (!withAlias) {
      return getMeasurementPaths(partialPath);
    } else {
      return super.getMeasurementPathsWithAlias(partialPath, -1, -1, false).left;
    }
  }

  private List<MeasurementPath> getMatchedPaths(
      PartitionGroup partitionGroup, List<String> pathsToQuery, boolean withAlias)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        List<MeasurementPath> paths =
            getMatchedPaths(node, partitionGroup.getHeader(), pathsToQuery, withAlias);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: get matched paths of {} and other {} paths from {} in {}, result {}",
              metaGroupMember.getName(),
              pathsToQuery.get(0),
              pathsToQuery.size() - 1,
              node,
              partitionGroup.getHeader(),
              paths);
        }
        if (paths != null) {
          // a non-null result contains correct result even if it is empty, so query next group
          return paths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptyList();
  }

  @SuppressWarnings("java:S1168") // null and empty list are different
  private List<MeasurementPath> getMatchedPaths(
      Node node, RaftNode header, List<String> pathsToQuery, boolean withAlias)
      throws IOException, TException, InterruptedException {
    GetAllPathsResult result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      result = SyncClientAdaptor.getAllPaths(client, header, pathsToQuery, withAlias);
    } else {
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        result = syncDataClient.getAllPaths(header, pathsToQuery, withAlias);
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        syncDataClient.close();
        throw e;
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }

    if (result != null) {
      // paths may be empty, implying that the group does not contain matched paths, so we do not
      // need to query other nodes in the group
      List<MeasurementPath> measurementPaths = new ArrayList<>();
      for (int i = 0; i < result.paths.size(); i++) {
        MeasurementPath matchedPath =
            ClusterQueryUtils.getAssembledPathFromRequest(
                result.getPaths().get(i), result.getDataTypes().get(i));
        measurementPaths.add(matchedPath);
        if (withAlias && matchedPath != null) {
          matchedPath.setMeasurementAlias(result.aliasList.get(i));
        }
        if (matchedPath != null) {
          matchedPath.setUnderAlignedEntity(result.getUnderAlignedEntity().get(i));
        }
      }
      return measurementPaths;
    } else {
      // a null implies a network failure, so we have to query other nodes in the group
      return null;
    }
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path pattern to be
   *     queried with storage group added
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return a collection of all queried devices
   */
  private Set<PartialPath> getMatchedDevices(
      Map<String, List<PartialPath>> sgPathMap, boolean isPrefixMatch) throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, List<PartialPath>> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      List<PartialPath> paths = sgPathEntry.getValue();
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup =
          metaGroupMember.getPartitionTable().route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        try {
          metaGroupMember
              .getLocalDataMember(partitionGroup.getHeader(), partitionGroup.getRaftId())
              .syncLeader(null);
        } catch (CheckConsistencyException e) {
          logger.warn("Failed to check consistency.", e);
        }
        Set<PartialPath> allDevices = new HashSet<>();
        for (PartialPath path : paths) {
          allDevices.addAll(super.getMatchedDevices(path, isPrefixMatch));
        }
        logger.debug(
            "{}: get matched paths of {} locally, result {}",
            metaGroupMember.getName(),
            partitionGroup,
            allDevices);
        result.addAll(allDevices);
      } else {
        // batch the queries of the same group to reduce communication
        for (PartialPath path : paths) {
          groupPathMap
              .computeIfAbsent(partitionGroup, p -> new ArrayList<>())
              .add(path.getFullPath());
        }
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();

      result.addAll(getMatchedDevices(partitionGroup, pathsToQuery, isPrefixMatch));
    }

    return result;
  }

  private Set<PartialPath> getMatchedDevices(
      PartitionGroup partitionGroup, List<String> pathsToQuery, boolean isPrefixMatch)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        Set<String> paths =
            getMatchedDevices(node, partitionGroup.getHeader(), pathsToQuery, isPrefixMatch);
        logger.debug(
            "{}: get matched paths of {} from {}, result {} for {}",
            metaGroupMember.getName(),
            partitionGroup,
            node,
            paths,
            pathsToQuery);
        if (paths != null) {
          // query next group
          Set<PartialPath> partialPaths = new HashSet<>();
          for (String path : paths) {
            partialPaths.add(new PartialPath(path));
          }
          return partialPaths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptySet();
  }

  private Set<String> getMatchedDevices(
      Node node, RaftNode header, List<String> pathsToQuery, boolean isPrefixMatch)
      throws IOException, TException, InterruptedException {
    Set<String> paths;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      paths = SyncClientAdaptor.getAllDevices(client, header, pathsToQuery, isPrefixMatch);
    } else {
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        try {
          paths = syncDataClient.getAllDevices(header, pathsToQuery, isPrefixMatch);
        } catch (TException e) {
          // the connection may be broken, close it to avoid it being reused
          syncDataClient.close();
          throw e;
        }
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }
    return paths;
  }

  /**
   * Similar to method getAllTimeseriesPath(), but return Path with alias.
   *
   * <p>Please note that for a returned measurement path, the name, alias and datatype are
   * guaranteed to be accurate, while the compression type, encoding and other fields are not. See
   * {@link GetAllPathsResult}
   */
  @Override
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    Map<String, List<PartialPath>> sgPathMap = groupPathByStorageGroup(pathPattern);

    if (isPrefixMatch) {
      // adapt to prefix match of IoTDB v0.12
      Map<String, List<PartialPath>> prefixSgPathMap =
          groupPathByStorageGroup(pathPattern.concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
      List<PartialPath> originPaths;
      List<PartialPath> addedPaths;
      for (String sg : prefixSgPathMap.keySet()) {
        originPaths = sgPathMap.get(sg);
        addedPaths = prefixSgPathMap.get(sg);
        if (originPaths == null) {
          sgPathMap.put(sg, addedPaths);
        } else {
          for (PartialPath path : addedPaths) {
            if (!originPaths.contains(path)) {
              originPaths.add(path);
            }
          }
        }
      }
    }

    List<MeasurementPath> result = getMatchedPaths(sgPathMap, true);

    int skippedOffset = 0;
    // apply offset and limit
    if (offset > 0 && result.size() > offset) {
      skippedOffset = offset;
      result = result.subList(offset, result.size());
    } else if (offset > 0) {
      skippedOffset = result.size();
      result = Collections.emptyList();
    }
    if (limit > 0 && result.size() > limit) {
      result = result.subList(0, limit);
    }
    logger.debug("The paths of path {} are {}", pathPattern, result);

    return new Pair<>(result, skippedOffset);
  }

  /**
   * Get all paths after removing wildcards in the path.
   *
   * <p>Please note that for a returned measurement path, the name, alias and datatype are
   * guaranteed to be accurate, while the compression type, encoding and other fields are not. See
   * {@link GetAllPathsResult}.
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public List<MeasurementPath> getMatchedPaths(PartialPath originPath) throws MetadataException {
    Map<String, List<PartialPath>> sgPathMap = groupPathByStorageGroup(originPath);
    List<MeasurementPath> ret = getMatchedPaths(sgPathMap, false);
    logger.debug("The paths of path {} are {}", originPath, ret);
    return ret;
  }

  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originalPaths a list of paths, potentially with wildcard
   * @return a pair of path lists, the first are the existing full paths, the second are invalid
   *     original paths
   */
  public Pair<List<PartialPath>, List<PartialPath>> getMatchedPaths(
      List<? extends PartialPath> originalPaths) {
    ConcurrentSkipListSet<PartialPath> fullPaths = new ConcurrentSkipListSet<>();
    ConcurrentSkipListSet<PartialPath> nonExistPaths = new ConcurrentSkipListSet<>();
    // TODO it is not suitable for register and deregister an Object to JMX to such a frequent
    // function call.
    // BUT is it suitable to create a thread pool for each calling??
    ExecutorService getAllPathsService =
        Executors.newFixedThreadPool(metaGroupMember.getPartitionTable().getGlobalGroups().size());
    for (PartialPath pathStr : originalPaths) {
      getAllPathsService.submit(
          () -> {
            try {
              List<MeasurementPath> fullPathStrs = getMatchedPaths(pathStr);
              if (fullPathStrs.isEmpty()) {
                nonExistPaths.add(pathStr);
                logger.debug("Path {} is not found.", pathStr);
              } else {
                fullPaths.addAll(fullPathStrs);
              }
            } catch (MetadataException e) {
              logger.error("Failed to get full paths of the prefix path: {} because", pathStr, e);
            }
          });
    }
    getAllPathsService.shutdown();
    try {
      getAllPathsService.awaitTermination(
          ClusterConstant.getReadOperationTimeoutMS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for get all paths services to stop", e);
    }
    return new Pair<>(new ArrayList<>(fullPaths), new ArrayList<>(nonExistPaths));
  }

  /**
   * Get the local devices that match any path in "paths". The result is deduplicated.
   *
   * @param paths paths potentially contain wildcards.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  public Set<String> getAllDevices(List<String> paths, boolean isPrefixMatch)
      throws MetadataException {
    Set<String> results = new HashSet<>();
    for (String path : paths) {
      this.getMatchedDevices(new PartialPath(path), isPrefixMatch).stream()
          .map(PartialPath::getFullPath)
          .forEach(results::add);
    }
    return results;
  }

  /**
   * Get the nodes of a prefix "path" at "nodeLevel". The method currently requires strong
   * consistency.
   *
   * @param path
   * @param nodeLevel
   */
  public List<String> getNodeList(String path, int nodeLevel) throws MetadataException {
    return getNodesListInGivenLevel(new PartialPath(path), nodeLevel).stream()
        .map(PartialPath::getFullPath)
        .collect(Collectors.toList());
  }

  public Set<String> getChildNodeInNextLevel(String path) throws MetadataException {
    return getChildNodeNameInNextLevel(new PartialPath(path));
  }

  public Set<String> getChildNodePathInNextLevel(String path) throws MetadataException {
    return getChildNodePathInNextLevel(new PartialPath(path));
  }

  /**
   * Replace partial paths (paths not containing measurements), and abstract paths (paths containing
   * wildcards) with full paths.
   */
  public void convertToFullPaths(PhysicalPlan plan)
      throws PathNotExistException, CheckConsistencyException {
    // make sure this node knows all storage groups
    metaGroupMember.syncLeaderWithConsistencyCheck(false);

    Pair<List<PartialPath>, List<PartialPath>> getMatchedPathsRet =
        getMatchedPaths(plan.getPaths());
    List<PartialPath> fullPaths = getMatchedPathsRet.left;
    List<PartialPath> nonExistPath = getMatchedPathsRet.right;
    plan.setPaths(fullPaths);
    if (!nonExistPath.isEmpty()) {
      throw new PathNotExistException(
          nonExistPath.stream().map(PartialPath::getFullPath).collect(Collectors.toList()));
    }
  }

  @Override
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws MetadataException {
    IMeasurementMNode child = super.getMeasurementMNode(deviceMNode, measurementName);
    if (child == null) {
      child = mRemoteMetaCache.get(deviceMNode.getPartialPath().concatNode(measurementName));
    }
    return child;
  }

  public List<ShowTimeSeriesResult> showLocalTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    return super.showTimeseries(plan, context);
  }

  public List<ShowDevicesResult> getLocalDevices(ShowDevicesPlan plan) throws MetadataException {
    return super.getMatchedDevices(plan);
  }

  @Override
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    ConcurrentSkipListSet<ShowDevicesResult> resultSet = new ConcurrentSkipListSet<>();
    ExecutorService pool =
        new ThreadPoolExecutor(
            THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();

    int limit = plan.getLimit() == 0 ? Integer.MAX_VALUE : plan.getLimit();
    int offset = plan.getOffset();
    // do not use limit and offset in sub-queries unless offset is 0, otherwise the results are
    // not combinable
    if (offset != 0) {
      if (limit > Integer.MAX_VALUE - offset) {
        plan.setLimit(0);
      } else {
        plan.setLimit(limit + offset);
      }
      plan.setOffset(0);
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Fetch devices schemas of {} from {} groups", plan.getPath(), globalGroups.size());
    }

    List<Future<Void>> futureList = new ArrayList<>();
    for (PartitionGroup group : globalGroups) {
      futureList.add(
          pool.submit(
              () -> {
                try {
                  getDevices(group, plan, resultSet);
                } catch (CheckConsistencyException e) {
                  logger.error("Cannot get show devices result of {} from {}", plan, group);
                }
                return null;
              }));
    }

    waitForThreadPool(futureList, pool, "getDevices()");
    List<ShowDevicesResult> showDevicesResults =
        applyShowDevicesLimitOffset(resultSet, limit, offset);
    logger.debug("show devices {} has {} results", plan.getPath(), showDevicesResults.size());
    return showDevicesResults;
  }

  @Override
  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    ExecutorService pool =
        new ThreadPoolExecutor(
            THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    List<PartitionGroup> groups = new ArrayList<>();
    try {
      PartitionGroup partitionGroup =
          metaGroupMember.getPartitionTable().partitionByPathTime(plan.getPath(), 0);
      groups.add(partitionGroup);
    } catch (MetadataException e) {
      // if the path location is not find, obtain the path location from all groups.
      groups = metaGroupMember.getPartitionTable().getGlobalGroups();
    }

    int limit = plan.getLimit() == 0 ? Integer.MAX_VALUE : plan.getLimit();
    int offset = plan.getOffset();
    // do not use limit and offset in sub-queries unless offset is 0, otherwise the results are
    // not combinable
    if (offset != 0) {
      if (limit > Integer.MAX_VALUE - offset) {
        plan.setLimit(0);
      } else {
        plan.setLimit(limit + offset);
      }
      plan.setOffset(0);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Fetch timeseries schemas of {} from {} groups", plan.getPath(), groups.size());
    }

    ShowTimeSeriesHandler handler = new ShowTimeSeriesHandler(groups.size(), plan.getPath());
    List<Future<Void>> futureList = new ArrayList<>();
    for (PartitionGroup group : groups) {
      futureList.add(
          pool.submit(
              () -> {
                showTimeseries(group, plan, context, handler);
                return null;
              }));
    }

    waitForThreadPool(futureList, pool, "showTimeseries()");
    List<ShowTimeSeriesResult> showTimeSeriesResults =
        applyShowTimeseriesLimitOffset(handler.getResult(), limit, offset);
    logger.debug("Show {} has {} results", plan.getPath(), showTimeSeriesResults.size());
    return showTimeSeriesResults;
  }

  private List<ShowTimeSeriesResult> applyShowTimeseriesLimitOffset(
      List<ShowTimeSeriesResult> results, int limit, int offset) {
    List<ShowTimeSeriesResult> showTimeSeriesResults = new ArrayList<>();
    Iterator<ShowTimeSeriesResult> iterator = results.iterator();
    while (iterator.hasNext() && limit > 0) {
      if (offset > 0) {
        offset--;
        iterator.next();
      } else {
        limit--;
        showTimeSeriesResults.add(iterator.next());
      }
    }

    return showTimeSeriesResults;
  }

  private List<ShowDevicesResult> applyShowDevicesLimitOffset(
      Set<ShowDevicesResult> resultSet, int limit, int offset) {
    List<ShowDevicesResult> showDevicesResults = new ArrayList<>();
    Iterator<ShowDevicesResult> iterator = resultSet.iterator();
    while (iterator.hasNext() && limit > 0) {
      if (offset > 0) {
        offset--;
        iterator.next();
      } else {
        limit--;
        showDevicesResults.add(iterator.next());
      }
    }
    return showDevicesResults;
  }

  private void showTimeseries(
      PartitionGroup group,
      ShowTimeSeriesPlan plan,
      QueryContext context,
      ShowTimeSeriesHandler handler) {
    if (group.contains(metaGroupMember.getThisNode())) {
      showLocalTimeseries(group, plan, context, handler);
    } else {
      showRemoteTimeseries(group, plan, context, handler);
    }
  }

  private void getDevices(
      PartitionGroup group, ShowDevicesPlan plan, Set<ShowDevicesResult> resultSet)
      throws CheckConsistencyException, MetadataException {
    if (group.contains(metaGroupMember.getThisNode())) {
      getLocalDevices(group, plan, resultSet);
    } else {
      getRemoteDevices(group, plan, resultSet);
    }
  }

  private void getLocalDevices(
      PartitionGroup group, ShowDevicesPlan plan, Set<ShowDevicesResult> resultSet)
      throws CheckConsistencyException, MetadataException {
    DataGroupMember localDataMember =
        metaGroupMember.getLocalDataMember(group.getHeader(), group.getRaftId());
    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      List<ShowDevicesResult> localResult = super.getMatchedDevices(plan);
      resultSet.addAll(localResult);
      logger.debug("Fetched {} devices of {} from {}", localResult.size(), plan.getPath(), group);
    } catch (MetadataException e) {
      logger.error("Cannot execute show devices plan {} from {} locally.", plan, group);
      throw e;
    }
  }

  private void showLocalTimeseries(
      PartitionGroup group,
      ShowTimeSeriesPlan plan,
      QueryContext context,
      ShowTimeSeriesHandler handler) {
    try {
      DataGroupMember localDataMember =
          metaGroupMember.getLocalDataMember(group.getHeader(), group.getRaftId());
      localDataMember.syncLeaderWithConsistencyCheck(false);
      List<ShowTimeSeriesResult> localResult = super.showTimeseries(plan, context);
      handler.onComplete(localResult);
    } catch (MetadataException | CheckConsistencyException e) {
      handler.onError(e);
    }
  }

  private void showRemoteTimeseries(
      PartitionGroup group,
      ShowTimeSeriesPlan plan,
      QueryContext context,
      ShowTimeSeriesHandler handler) {
    ByteBuffer resultBinary = null;
    for (Node node : group) {
      try {
        resultBinary = showRemoteTimeseries(context, node, group, plan);
        if (resultBinary != null) {
          break;
        }
      } catch (IOException | TException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting timeseries schemas in node {}.", node, e);
        Thread.currentThread().interrupt();
      } finally {
        // record the queried node to release resources later
        ((RemoteQueryContext) context).registerRemoteNode(node, group.getHeader());
      }
    }

    if (resultBinary != null) {
      int size = resultBinary.getInt();
      List<ShowTimeSeriesResult> results = new ArrayList<>();
      logger.debug(
          "Fetched remote timeseries {} schemas of {} from {}", size, plan.getPath(), group);
      for (int i = 0; i < size; i++) {
        results.add(ShowTimeSeriesResult.deserialize(resultBinary));
      }
      handler.onComplete(results);
    } else {
      String errMsg =
          String.format("Failed to get timeseries in path %s from group %s", plan.getPath(), group);
      handler.onError(new MetadataException(errMsg));
    }
  }

  private void getRemoteDevices(
      PartitionGroup group, ShowDevicesPlan plan, Set<ShowDevicesResult> resultSet) {
    ByteBuffer resultBinary = null;
    for (Node node : group) {
      try {
        resultBinary = getRemoteDevices(node, group, plan);
        if (resultBinary != null) {
          break;
        }
      } catch (IOException | TException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting devices schemas in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }

    if (resultBinary != null) {
      int size = resultBinary.getInt();
      logger.debug("Fetched remote devices {} schemas of {} from {}", size, plan.getPath(), group);
      for (int i = 0; i < size; i++) {
        resultSet.add(ShowDevicesResult.deserialize(resultBinary));
      }
    } else {
      logger.error("Failed to execute show devices {} in group: {}.", plan, group);
    }
  }

  private ByteBuffer showRemoteTimeseries(
      QueryContext context, Node node, PartitionGroup group, ShowTimeSeriesPlan plan)
      throws IOException, TException, InterruptedException {
    ByteBuffer resultBinary;

    // prepare request
    MeasurementSchemaRequest request;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);
      request =
          new MeasurementSchemaRequest(
              context.getQueryId(),
              group.getHeader(),
              node,
              ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    }

    // execute remote query
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      resultBinary = SyncClientAdaptor.getAllMeasurementSchema(client, request);
    } else {
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        resultBinary = syncDataClient.getAllMeasurementSchema(request);
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        syncDataClient.close();
        throw e;
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }
    return resultBinary;
  }

  private ByteBuffer getRemoteDevices(Node node, PartitionGroup group, ShowDevicesPlan plan)
      throws IOException, TException, InterruptedException {
    ByteBuffer resultBinary;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      resultBinary = SyncClientAdaptor.getDevices(client, group.getHeader(), plan);
    } else {
      SyncDataClient syncDataClient = null;
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        plan.serialize(dataOutputStream);
        resultBinary =
            syncDataClient.getDevices(
                group.getHeader(), ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        syncDataClient.close();
        throw e;
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }
    return resultBinary;
  }

  public GetAllPathsResult getAllPaths(List<String> paths, boolean withAlias)
      throws MetadataException {
    List<String> retPaths = new ArrayList<>();
    List<Byte> dataTypes = new ArrayList<>();
    List<String> alias = withAlias ? new ArrayList<>() : null;
    List<Boolean> underAlignedEntity = new ArrayList<>();

    for (String path : paths) {
      List<MeasurementPath> allTimeseriesPathWithAlias =
          super.getMeasurementPathsWithAlias(new PartialPath(path), -1, -1, false).left;
      for (MeasurementPath timeseriesPathWithAlias : allTimeseriesPathWithAlias) {
        retPaths.add(timeseriesPathWithAlias.getFullPath());
        dataTypes.add(timeseriesPathWithAlias.getSeriesTypeInByte());
        if (withAlias) {
          alias.add(timeseriesPathWithAlias.getMeasurementAlias());
        }
        underAlignedEntity.add(timeseriesPathWithAlias.isUnderAlignedEntity());
      }
    }

    GetAllPathsResult getAllPathsResult = new GetAllPathsResult();
    getAllPathsResult.setPaths(retPaths);
    getAllPathsResult.setDataTypes(dataTypes);
    getAllPathsResult.setAliasList(alias);
    getAllPathsResult.setUnderAlignedEntity(underAlignedEntity);
    return getAllPathsResult;
  }

  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    try {
      return super.getBelongedStorageGroup(path);
    } catch (StorageGroupNotSetException e) {
      try {
        metaGroupMember.syncLeader(null);
      } catch (CheckConsistencyException ex) {
        logger.warn("Failed to check consistency.", e);
      }
      return super.getBelongedStorageGroup(path);
    }
  }
}
