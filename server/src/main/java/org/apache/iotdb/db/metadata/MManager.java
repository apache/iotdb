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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEngine;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.exception.metadata.DifferentTemplateException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.NoTemplateOnMNodeException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MultiMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.UnaryMeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTree;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.SetSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.UnsetSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetUsingSchemaTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
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
@SuppressWarnings("java:S1135") // ignore todos
public class MManager {

  private static final Logger logger = LoggerFactory.getLogger(MManager.class);

  public static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";

  /** A thread will check whether the MTree is modified lately each such interval. Unit: second */
  private static final long MTREE_SNAPSHOT_THREAD_CHECK_TIME = 600L;

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** threshold total size of MTree */
  private static final long MTREE_SIZE_THRESHOLD = config.getAllocateMemoryForSchema();

  private static final int ESTIMATED_SERIES_SIZE = config.getEstimatedSeriesSize();

  private boolean isRecovering;
  private boolean initialized;
  private boolean allowToCreateNewSeries = true;

  private AtomicLong totalSeriesNumber = new AtomicLong();

  private final int mtreeSnapshotInterval;
  private final long mtreeSnapshotThresholdTime;
  private ScheduledExecutorService timedCreateMTreeSnapshotThread;
  private ScheduledExecutorService timedForceMLogThread;

  // the log file seriesPath
  private String logFilePath;
  private File logFile;
  private MLogWriter logWriter;

  private MTree mtree;
  // device -> DeviceMNode
  private RandomDeleteCache<PartialPath, IMNode> mNodeCache;
  private TagManager tagManager = TagManager.getInstance();
  private TemplateManager templateManager = TemplateManager.getInstance();

  // region MManager Singleton
  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final MManager INSTANCE = new MManager();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
  }
  // endregion

  // region Interfaces and Implementation of MManager initialization、snapshot、recover and clear
  protected MManager() {
    mtreeSnapshotInterval = config.getMtreeSnapshotInterval();
    mtreeSnapshotThresholdTime = config.getMtreeSnapshotThresholdTime() * 1000L;
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
    logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;

    // do not write log when recover
    isRecovering = true;

    int cacheSize = config.getmManagerCacheSize();
    mNodeCache =
        new RandomDeleteCache<PartialPath, IMNode>(cacheSize) {

          @Override
          public IMNode loadObjectByKey(PartialPath key) throws CacheException {
            try {
              return mtree.getNodeByPathWithStorageGroupCheck(key);
            } catch (MetadataException e) {
              throw new CacheException(e);
            }
          }
        };

    if (config.isEnableMTreeSnapshot()) {
      timedCreateMTreeSnapshotThread =
          Executors.newSingleThreadScheduledExecutor(
              r -> new Thread(r, "timedCreateMTreeSnapshotThread"));
      timedCreateMTreeSnapshotThread.scheduleAtFixedRate(
          this::checkMTreeModified,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          TimeUnit.SECONDS);
    }
  }

  // Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  public synchronized void init() {
    if (initialized) {
      return;
    }
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);

    try {
      isRecovering = true;

      tagManager.init();
      mtree = new MTree();
      mtree.init();

      int lineNumber = initFromLog(logFile);

      logWriter = new MLogWriter(config.getSchemaDir(), MetadataConstant.METADATA_LOG);
      logWriter.setLogNum(lineNumber);
      isRecovering = false;
    } catch (IOException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }
    initialized = true;
  }

  /** @return line number of the logFile */
  @SuppressWarnings("squid:S3776")
  private int initFromLog(File logFile) throws IOException {
    long time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      int idx = 0;
      try (MLogReader mLogReader =
          new MLogReader(config.getSchemaDir(), MetadataConstant.METADATA_LOG); ) {
        idx = applyMLog(mLogReader);
        logger.debug(
            "spend {} ms to deserialize mtree from mlog.bin", System.currentTimeMillis() - time);
        return idx;
      } catch (Exception e) {
        throw new IOException("Failed to parser mlog.bin for err:" + e);
      }
    } else {
      return 0;
    }
  }

  private int applyMLog(MLogReader mLogReader) {
    int idx = 0;
    PhysicalPlan plan;
    while (mLogReader.hasNext()) {
      try {
        plan = mLogReader.next();
        idx++;
      } catch (Exception e) {
        logger.error("Parse mlog error at lineNumber {} because:", idx, e);
        break;
      }
      if (plan == null) {
        continue;
      }
      try {
        operation(plan);
      } catch (MetadataException | IOException e) {
        logger.error("Can not operate cmd {} for err:", plan.getOperatorType(), e);
      }
    }

    return idx;
  }

  private void checkMTreeModified() {
    if (logWriter == null || logFile == null) {
      // the logWriter is not initialized now, we skip the check once.
      return;
    }
    if (System.currentTimeMillis() - logFile.lastModified() >= mtreeSnapshotThresholdTime
        || logWriter.getLogNum() >= mtreeSnapshotInterval) {
      logger.info(
          "New mlog line number: {}, time from last modification: {} ms",
          logWriter.getLogNum(),
          System.currentTimeMillis() - logFile.lastModified());
      createMTreeSnapshot();
    }
  }

  public void createMTreeSnapshot() {
    try {
      mtree.createSnapshot();
      logWriter.clear();
    } catch (IOException e) {
      logger.warn("Failed to create MTree snapshot", e);
    }
  }

  /** function for clearing MTree */
  public synchronized void clear() {
    try {
      if (this.mtree != null) {
        this.mtree.clear();
      }
      if (this.mNodeCache != null) {
        this.mNodeCache.clear();
      }
      this.totalSeriesNumber.set(0);
      this.templateManager.clear();
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      tagManager.clear();
      initialized = false;
      if (config.isEnableMTreeSnapshot() && timedCreateMTreeSnapshotThread != null) {
        timedCreateMTreeSnapshotThread.shutdownNow();
        timedCreateMTreeSnapshotThread = null;
      }
      if (timedForceMLogThread != null) {
        timedForceMLogThread.shutdownNow();
        timedForceMLogThread = null;
      }
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
  }

  public void operation(PhysicalPlan plan) throws IOException, MetadataException {
    switch (plan.getOperatorType()) {
      case CREATE_TIMESERIES:
        CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
        createTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
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
      case CHANGE_TAG_OFFSET:
        ChangeTagOffsetPlan changeTagOffsetPlan = (ChangeTagOffsetPlan) plan;
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
        break;
      case CREATE_TEMPLATE:
        CreateTemplatePlan createTemplatePlan = (CreateTemplatePlan) plan;
        createSchemaTemplate(createTemplatePlan);
        break;
      case SET_SCHEMA_TEMPLATE:
        SetSchemaTemplatePlan setSchemaTemplatePlan = (SetSchemaTemplatePlan) plan;
        setSchemaTemplate(setSchemaTemplatePlan);
        break;
      case SET_USING_SCHEMA_TEMPLATE:
        SetUsingSchemaTemplatePlan setUsingSchemaTemplatePlan = (SetUsingSchemaTemplatePlan) plan;
        setUsingSchemaTemplate(setUsingSchemaTemplatePlan);
        break;
      case AUTO_CREATE_DEVICE_MNODE:
        AutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan = (AutoCreateDeviceMNodePlan) plan;
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        break;
      case UNSET_SCHEMA_TEMPLATE:
        UnsetSchemaTemplatePlan unsetSchemaTemplatePlan = (UnsetSchemaTemplatePlan) plan;
        unsetSchemaTemplate(unsetSchemaTemplatePlan);
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces for CQ
  public void createContinuousQuery(CreateContinuousQueryPlan plan) throws IOException {
    logWriter.createContinuousQuery(plan);
  }

  public void dropContinuousQuery(DropContinuousQueryPlan plan) throws IOException {
    logWriter.dropContinuousQuery(plan);
  }
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!allowToCreateNewSeries) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    try {
      PartialPath path = plan.getPath();
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

      ensureStorageGroup(path);

      TSDataType type = plan.getDataType();
      // create time series in MTree
      IMeasurementMNode leafMNode =
          mtree.createTimeseries(
              path,
              type,
              plan.getEncoding(),
              plan.getCompressor(),
              plan.getProps(),
              plan.getAlias());

      // the cached mNode may be replaced by new entityMNode in mtree
      mNodeCache.removeObject(path.getDevicePath());

      // update tag index
      if (plan.getTags() != null) {
        // tag key, tag value
        for (Entry<String, String> entry : plan.getTags().entrySet()) {
          if (entry.getKey() == null || entry.getValue() == null) {
            continue;
          }
          tagManager.addIndex(entry.getKey(), entry.getValue(), leafMNode);
        }
      }

      // update statistics and schemaDataTypeNumMap
      totalSeriesNumber.addAndGet(1);
      if (totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE >= MTREE_SIZE_THRESHOLD) {
        logger.warn("Current series number {} is too large...", totalSeriesNumber);
        allowToCreateNewSeries = false;
      }

      // write log
      if (!isRecovering) {
        // either tags or attributes is not empty
        if ((plan.getTags() != null && !plan.getTags().isEmpty())
            || (plan.getAttributes() != null && !plan.getAttributes().isEmpty())) {
          offset = tagManager.writeTagFile(plan.getTags(), plan.getAttributes());
        }
        plan.setTagOffset(offset);
        logWriter.createTimeseries(plan);
      }
      leafMNode.setOffset(offset);

    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
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
      Map<String, String> props)
      throws MetadataException {
    try {
      createTimeseries(
          new CreateTimeSeriesPlan(path, dataType, encoding, compressor, props, null, null, null));
    } catch (PathAlreadyExistException | AliasAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore PathAlreadyExistException and AliasAlreadyExistException when Concurrent inserting"
                + " a non-exist time series {}",
            path);
      }
    }
  }

  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressor, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    if (!allowToCreateNewSeries) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    try {
      PartialPath prefixPath = plan.getPrefixPath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      ensureStorageGroup(prefixPath);

      // create time series in MTree
      mtree.createAlignedTimeseries(
          prefixPath, measurements, plan.getDataTypes(), plan.getEncodings(), plan.getCompressor());

      // the cached mNode may be replaced by new entityMNode in mtree
      mNodeCache.removeObject(prefixPath.getDevicePath());

      // update statistics and schemaDataTypeNumMap
      totalSeriesNumber.addAndGet(measurements.size());
      if (totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE >= MTREE_SIZE_THRESHOLD) {
        logger.warn("Current series number {} is too large...", totalSeriesNumber);
        allowToCreateNewSeries = false;
      }
      // write log
      if (!isRecovering) {
        logWriter.createAlignedTimeseries(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  private void ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      mtree.getBelongedStorageGroup(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      try {
        setStorageGroup(storageGroupPath);
      } catch (StorageGroupAlreadySetException storageGroupAlreadySetException) {
        // do nothing
        // concurrent timeseries creation may result concurrent ensureStorageGroup
        // it's ok that the storageGroup has already been set
      }
    }
  }

  /**
   * Delete all timeseries matching the given path pattern, may cross different storage group The
   * given pathPattern only match measurement but not flat measurement. For example, given MTree:
   * root.sg.d.vector(s1, s2), root.sg.d.s2; give pathPattern root.**.s2 and then only root.sg.d.s2
   * will be deleted; give pathPattern like root.sg.d.* or root.sg.d.vector and then the deletion
   * will work on root.sg.d.vector(s1, s2)
   *
   * @param pathPattern path to be deleted
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    try {
      List<PartialPath> allTimeseries = mtree.getMeasurementPaths(pathPattern);
      if (allTimeseries.isEmpty()) {
        throw new MetadataException(
            String.format(
                "No matched timeseries or aligned timeseries for Path [%s]",
                pathPattern.getFullPath()));
      }

      // Monitor storage group seriesPath is not allowed to be deleted
      allTimeseries.removeIf(p -> p.startsWith(MonitorConstants.STAT_STORAGE_GROUP_ARRAY));

      Set<String> failedNames = new HashSet<>();
      for (PartialPath p : allTimeseries) {
        deleteSingleTimeseriesInternal(p, failedNames);
      }
      return failedNames.isEmpty() ? null : String.join(",", failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  private void deleteSingleTimeseriesInternal(PartialPath p, Set<String> failedNames)
      throws MetadataException, IOException {
    DeleteTimeSeriesPlan deleteTimeSeriesPlan = new DeleteTimeSeriesPlan();
    try {
      PartialPath emptyStorageGroup = deleteOneTimeseriesUpdateStatisticsAndDropTrigger(p);
      if (!isRecovering) {
        if (emptyStorageGroup != null) {
          StorageEngine.getInstance().deleteAllDataFilesInOneStorageGroup(emptyStorageGroup);
          StorageEngine.getInstance()
              .releaseWalDirectByteBufferPoolInOneStorageGroup(emptyStorageGroup);
        }
        deleteTimeSeriesPlan.setDeletePathList(Collections.singletonList(p));
        logWriter.deleteTimeseries(deleteTimeSeriesPlan);
      }
    } catch (DeleteFailedException e) {
      failedNames.add(e.getName());
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return After delete if the storage group is empty, return its path, otherwise return null
   */
  private PartialPath deleteOneTimeseriesUpdateStatisticsAndDropTrigger(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, IMeasurementMNode> pair =
        mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);
    // if one of the aligned timeseries is deleted, pair.right could be null
    IMeasurementMNode measurementMNode = pair.right;
    int timeseriesNum = 0;
    if (measurementMNode.isUnaryMeasurement()) {
      removeFromTagInvertedIndex(measurementMNode);
      timeseriesNum = 1;
    } else if (measurementMNode.isMultiMeasurement()) {
      timeseriesNum += measurementMNode.getSchema().getSubMeasurementsTSDataTypeList().size();
    }
    PartialPath storageGroupPath = pair.left;

    // drop trigger with no exceptions
    TriggerEngine.drop(pair.right);

    IMNode node = measurementMNode.getParent();

    if (node.isUseTemplate() && node.getSchemaTemplate().hasSchema(measurementMNode.getName())) {
      // measurement represent by template doesn't affect the MTree structure and memory control
      return storageGroupPath;
    }

    while (node.isEmptyInternal()) {
      mNodeCache.removeObject(node.getPartialPath());
      node = node.getParent();
    }
    totalSeriesNumber.addAndGet(-timeseriesNum);
    if (!allowToCreateNewSeries
        && totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
      logger.info("Current series number {} come back to normal level", totalSeriesNumber);
      allowToCreateNewSeries = true;
    }
    return storageGroupPath;
  }
  // endregion

  // region Interfaces and Implementation for StorageGroup and TTL operation
  // including sg set and delete, and ttl set

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    try {
      mtree.setStorageGroup(storageGroup);
      if (!config.isEnableMemControl()) {
        MemTableManager.getInstance().addOrDeleteStorageGroup(1);
      }
      if (!isRecovering) {
        logWriter.setStorageGroup(storageGroup);
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Delete storage groups of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3"
   *
   * @param storageGroups list of paths to be deleted. Format: root.node
   */
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    try {
      for (PartialPath storageGroup : storageGroups) {
        totalSeriesNumber.addAndGet(
            -mtree.getAllTimeseriesCount(storageGroup.concatNode(MULTI_LEVEL_PATH_WILDCARD)));
        // clear cached MNode
        if (!allowToCreateNewSeries
            && totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
          logger.info("Current series number {} come back to normal level", totalSeriesNumber);
          allowToCreateNewSeries = true;
        }
        mNodeCache.clear();

        // try to delete storage group
        List<IMeasurementMNode> leafMNodes = mtree.deleteStorageGroup(storageGroup);
        for (IMeasurementMNode leafMNode : leafMNodes) {
          removeFromTagInvertedIndex(leafMNode);
        }

        // drop triggers with no exceptions
        TriggerEngine.drop(leafMNodes);

        if (!config.isEnableMemControl()) {
          MemTableManager.getInstance().addOrDeleteStorageGroup(-1);
        }

        // if success
        if (!isRecovering) {
          logWriter.deleteStorageGroup(storageGroup);
        }
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
    if (!isRecovering) {
      logWriter.setTTL(storageGroup, dataTTL);
    }
  }
  // endregion

  // region Interfaces for get and auto create device
  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param path path
   * @param allowCreateSg The stand-alone version can create an sg at will, but the cluster version
   *     needs to make the Meta group aware of the creation of an SG, so an exception needs to be
   *     thrown here
   */
  protected IMNode getDeviceNodeWithAutoCreate(
      PartialPath path, boolean autoCreateSchema, boolean allowCreateSg, int sgLevel)
      throws IOException, MetadataException {
    IMNode node;
    boolean shouldSetStorageGroup;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (CacheException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path.getFullPath());
      }
      shouldSetStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
    }

    try {
      if (shouldSetStorageGroup) {
        if (allowCreateSg) {
          PartialPath storageGroupPath = MetaUtils.getStorageGroupPathByLevel(path, sgLevel);
          setStorageGroup(storageGroupPath);
        } else {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
      }
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      if (!(node.isStorageGroup())) {
        logWriter.autoCreateDeviceMNode(new AutoCreateDeviceMNodePlan(node.getPartialPath()));
      }
      return node;
    } catch (StorageGroupAlreadySetException e) {
      if (e.isHasChild()) {
        // if setStorageGroup failure is because of child, the deviceNode should not be created.
        // Timeseries can't be create under a deviceNode without storageGroup.
        throw e;
      }
      // ignore set storage group concurrently
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      if (!(node.isStorageGroup())) {
        logWriter.autoCreateDeviceMNode(new AutoCreateDeviceMNodePlan(node.getPartialPath()));
      }
      return node;
    }
  }

  protected IMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws MetadataException, IOException {
    return getDeviceNodeWithAutoCreate(
        path, config.isAutoCreateSchemaEnabled(), true, config.getDefaultStorageGroupLevel());
  }

  private void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath(), config.getDefaultStorageGroupLevel());
  }
  // endregion

  // region Interfaces for metadata info Query
  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    return mtree.isPathExist(path);
  }

  /** Get metadata in string */
  public String getMetadataInString() {
    return TIME_SERIES_TREE_HEADER + mtree;
  }

  // region Interfaces for metadata count

  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern);
  }

  /**
   * To calculate the count of timeseries component matching given path. The path could be a pattern
   * of a full path, may contain wildcard.
   */
  public int getAllTimeseriesFlatCount(PartialPath pathPattern) throws MetadataException {
    return mtree.getAllTimeseriesFlatCount(pathPattern);
  }

  /** To calculate the count of devices for given path pattern. */
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return mtree.getDevicesNum(pathPattern);
  }

  /** To calculate the count of storage group for given path pattern. */
  public int getStorageGroupNum(PartialPath pathPattern) throws MetadataException {
    return mtree.getStorageGroupNum(pathPattern);
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
      throws MetadataException {
    return mtree.getNodesCountInGivenLevel(pathPattern, level);
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
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    return getNodesListInGivenLevel(pathPattern, nodeLevel, null);
  }

  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
    return mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, filter);
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
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException {
    return mtree.getChildNodePathInNextLevel(pathPattern);
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
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    return mtree.getChildNodeNameInNextLevel(pathPattern);
  }
  // endregion

  // region Interfaces for StorageGroup and TTL info Query
  /**
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    return mtree.isStorageGroup(path);
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    return mtree.checkStorageGroupByPath(path);
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return mtree.getBelongedStorageGroup(path);
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
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getBelongedStorageGroups(pathPattern);
  }

  /**
   * Get all storage group matching given path pattern.
   *
   * @param pathPattern a pattern of a full path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getMatchedStorageGroups(pathPattern);
  }

  /** Get all storage group paths */
  public List<PartialPath> getAllStorageGroupPaths() {
    return mtree.getAllStorageGroupPaths();
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    Map<PartialPath, Long> storageGroupsTTL = new HashMap<>();
    try {
      List<PartialPath> storageGroups = this.getAllStorageGroupPaths();
      for (PartialPath storageGroup : storageGroups) {
        long ttl = getStorageGroupNodeByStorageGroupPath(storageGroup).getDataTTL();
        storageGroupsTTL.put(storageGroup, ttl);
      }
    } catch (MetadataException e) {
      logger.error("get storage groups ttl failed.", e);
    }
    return storageGroupsTTL;
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
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    return mtree.getDevicesByTimeseries(timeseries);
  }

  /**
   * Get all device paths matching the path pattern.
   *
   * @param pathPattern the pattern of the target devices.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern) throws MetadataException {
    return mtree.getDevices(pathPattern, false);
  }

  /**
   * Get all device paths and according storage group paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult.
   */
  public List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException {
    return mtree.getDevices(plan);
  }
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query

  /**
   * PartialPath of aligned time series will be organized to one VectorPartialPath. BEFORE this
   * method, all the aligned time series is NOT united. For example, given root.sg.d1.vector1[s1]
   * and root.sg.d1.vector1[s2], they will be organized to root.sg.d1.vector1 [s1,s2]
   *
   * @param fullPaths full path list without uniting the sub measurement under the same aligned time
   *     series.
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     VectorMeasurementSchema (s1,s2) would be returned once.
   */
  public List<PartialPath> groupVectorPaths(List<PartialPath> fullPaths) throws MetadataException {
    Map<IMNode, PartialPath> nodeToPartialPath = new LinkedHashMap<>();
    for (PartialPath path : fullPaths) {
      IMeasurementMNode node = getMeasurementMNode(path);
      if (!nodeToPartialPath.containsKey(node)) {
        nodeToPartialPath.put(node, path.copy());
      } else {
        // if nodeToPartialPath contains node
        PartialPath existPath = nodeToPartialPath.get(node);
        if (!existPath.equals(path)) {
          // could be VectorPartialPath
          ((VectorPartialPath) existPath)
              .addSubSensor(((VectorPartialPath) path).getSubSensorsList());
        }
      }
    }
    return new ArrayList<>(nodeToPartialPath.values());
  }

  /**
   * Return all flat measurement paths for given path if the path is abstract. Or return the path
   * itself. Regular expression in this method is formed by the amalgamation of seriesPath and the
   * character '*'.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   */
  public List<PartialPath> getFlatMeasurementPaths(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getFlatMeasurementPaths(pathPattern);
  }

  /**
   * Similar to method getAllTimeseriesPath(), but return Path with alias and filter the result by
   * limit and offset.
   */
  public Pair<List<PartialPath>, Integer> getFlatMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset) throws MetadataException {
    return mtree.getFlatMeasurementPathsWithAlias(pathPattern, limit, offset);
  }

  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    // show timeseries with index
    if (plan.getKey() != null && plan.getValue() != null) {
      return showTimeseriesWithIndex(plan, context);
    } else {
      return showTimeseriesWithoutIndex(plan, context);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private List<ShowTimeSeriesResult> showTimeseriesWithIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {

    List<IMeasurementMNode> allMatchedNodes = tagManager.getMatchedTimeseriesInIndex(plan, context);

    List<ShowTimeSeriesResult> res = new LinkedList<>();
    PartialPath pathPattern = plan.getPath();
    int curOffset = -1;
    int count = 0;
    int limit = plan.getLimit();
    int offset = plan.getOffset();
    for (IMeasurementMNode leaf : allMatchedNodes) {
      if (pathPattern.matchFullPath(leaf.getPartialPath())) {
        if (limit != 0 || offset != 0) {
          curOffset++;
          if (curOffset < offset || count == limit) {
            continue;
          }
        }
        try {
          Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
              tagManager.readTagFile(leaf.getOffset());
          IMeasurementSchema measurementSchema = leaf.getSchema();
          res.add(
              new ShowTimeSeriesResult(
                  leaf.getFullPath(),
                  leaf.getAlias(),
                  getBelongedStorageGroup(leaf.getPartialPath()).getFullPath(),
                  measurementSchema.getType(),
                  measurementSchema.getEncodingType(),
                  measurementSchema.getCompressor(),
                  tagAndAttributePair.left,
                  tagAndAttributePair.right));
          if (limit != 0) {
            count++;
          }
        } catch (IOException e) {
          throw new MetadataException(
              "Something went wrong while deserialize tag info of " + leaf.getFullPath(), e);
        }
      }
    }
    return res;
  }

  /**
   * Get the result of ShowTimeseriesPlan
   *
   * @param plan show time series query plan
   */
  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    List<Pair<PartialPath, String[]>> ans;
    if (plan.isOrderByHeat()) {
      ans = mtree.getAllFlatMeasurementSchemaByHeatOrder(plan, context);
    } else {
      ans = mtree.getAllFlatMeasurementSchema(plan);
    }
    List<ShowTimeSeriesResult> res = new LinkedList<>();
    for (Pair<PartialPath, String[]> ansString : ans) {
      long tagFileOffset = Long.parseLong(ansString.right[5]);
      try {
        Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
            new Pair<>(Collections.emptyMap(), Collections.emptyMap());
        if (tagFileOffset >= 0) {
          tagAndAttributePair = tagManager.readTagFile(tagFileOffset);
        }
        res.add(
            new ShowTimeSeriesResult(
                ansString.left.getExactFullPath(),
                ansString.right[0],
                ansString.right[1],
                TSDataType.valueOf(ansString.right[2]),
                TSEncoding.valueOf(ansString.right[3]),
                CompressionType.valueOf(ansString.right[4]),
                tagAndAttributePair.left,
                tagAndAttributePair.right));
      } catch (IOException e) {
        throw new MetadataException(
            "Something went wrong while deserialize tag info of " + ansString.left.getFullPath(),
            e);
      }
    }
    return res;
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param fullPath full path
   */
  public TSDataType getSeriesType(PartialPath fullPath) throws MetadataException {
    if (fullPath.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }

    IMeasurementSchema schema = mtree.getSchema(fullPath);
    if (schema instanceof UnaryMeasurementSchema) {
      return schema.getType();
    } else {
      if (((VectorPartialPath) fullPath).getSubSensorsList().size() != 1) {
        return TSDataType.VECTOR;
      } else {
        String subSensor = ((VectorPartialPath) fullPath).getSubSensor(0);
        List<String> measurements = schema.getSubMeasurementsList();
        return schema.getSubMeasurementsTSDataTypeList().get(measurements.indexOf(subSensor));
      }
    }
  }

  /**
   * get MeasurementSchema or VectorMeasurementSchema which contains the measurement
   *
   * @param device device path
   * @param measurement measurement name, could be vector name
   * @return MeasurementSchema or VectorMeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath device, String measurement)
      throws MetadataException {
    IMNode deviceIMNode = getDeviceNode(device);
    IMeasurementMNode measurementMNode = deviceIMNode.getChild(measurement).getAsMeasurementMNode();
    if (measurementMNode == null) {
      // Just for the initial adaptation of the template functionality and merge functionality
      // The getSeriesSchema interface needs to be cleaned up later
      return getSeriesSchema(device.concatNode(measurement));
    }
    return measurementMNode.getSchema();
  }

  /**
   * Get schema of paritialPath
   *
   * @param fullPath (may be ParitialPath or VectorPartialPath)
   * @return MeasurementSchema or VectorMeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    IMeasurementMNode leaf = getMeasurementMNode(fullPath);
    return getSeriesSchema(fullPath, leaf);
  }

  protected IMeasurementSchema getSeriesSchema(PartialPath fullPath, IMeasurementMNode leaf) {
    IMeasurementSchema schema = leaf.getSchema();

    if (!(fullPath instanceof VectorPartialPath)
        || schema == null
        || schema.getType() != TSDataType.VECTOR) {
      return schema;
    }
    List<String> measurementsInLeaf = schema.getSubMeasurementsList();
    List<String> subMeasurements = ((VectorPartialPath) fullPath).getSubSensorsList();
    TSDataType[] types = new TSDataType[subMeasurements.size()];
    TSEncoding[] encodings = new TSEncoding[subMeasurements.size()];

    for (int i = 0; i < subMeasurements.size(); i++) {
      int index = measurementsInLeaf.indexOf(subMeasurements.get(i));
      types[i] = schema.getSubMeasurementsTSDataTypeList().get(index);
      encodings[i] = schema.getSubMeasurementsTSEncodingList().get(index);
    }
    String[] array = new String[subMeasurements.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = subMeasurements.get(i);
    }
    return new VectorMeasurementSchema(
        schema.getMeasurementId(), array, types, encodings, schema.getCompressor());
  }

  // attention: this path must be a device node
  public List<IMeasurementSchema> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    Set<IMeasurementSchema> res = new HashSet<>();
    try {
      IMNode node = mNodeCache.get(devicePath);
      Template template = node.getUpperTemplate();

      for (IMNode child : node.getChildren().values()) {
        if (child.isMeasurement()) {
          IMeasurementMNode measurementMNode = child.getAsMeasurementMNode();
          res.add(measurementMNode.getSchema());
        }
      }

      // template
      if (node.isUseTemplate() && template != null) {
        res.addAll(template.getSchemaMap().values());
      }
    } catch (CacheException e) {
      throw new PathNotExistException(devicePath.getFullPath());
    }

    return new ArrayList<>(res);
  }

  public Map<PartialPath, IMeasurementSchema> getAllMeasurementSchemaByPrefix(
      PartialPath prefixPath) throws MetadataException {
    return mtree.getAllMeasurementSchemaByPrefix(prefixPath);
  }
  // endregion
  // endregion

  // region Interfaces and methods for MNode query
  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node by path. If storage group is not
   * set, StorageGroupNotSetException will be thrown
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return mtree.getStorageGroupNodeByStorageGroupPath(path);
  }

  /** Get storage group node by path. the give path don't need to be storage group path. */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getStorageGroupNodeByPath(path);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  IMNode getDeviceNode(PartialPath path) throws MetadataException {
    IMNode node;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (CacheException e) {
      throw new PathNotExistException(path.getFullPath());
    }
  }

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

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    return mtree.getMeasurementMNode(fullPath);
  }

  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws PathAlreadyExistException {
    IMNode result = deviceMNode.getChild(measurementName);
    if (result == null) {
      return null;
    }

    if (result.isMeasurement()) {
      return result.getAsMeasurementMNode();
    } else {
      throw new PathAlreadyExistException(
          deviceMNode.getFullPath() + PATH_SEPARATOR + measurementName);
    }
  }
  // endregion

  // region Interfaces for alias and tag/attribute operations
  /**
   * Check whether the given path contains a storage group change or set the new offset of a
   * timeseries
   *
   * @param path timeseries
   * @param offset offset in the tag file
   */
  public void changeOffset(PartialPath path, long offset) throws MetadataException {
    mtree.getMeasurementMNode(path).setOffset(offset);
  }

  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(path);
    if (leafMNode.getAlias() != null) {
      leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
    }
    leafMNode.getParent().addAlias(alias, leafMNode);
    leafMNode.setAlias(alias);
  }

  /**
   * upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // upsert alias
    upsertAlias(alias, fullPath, leafMNode);

    if (tagsMap == null && attributesMap == null) {
      return;
    }
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(tagsMap, attributesMap);
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      tagManager.addIndex(tagsMap, leafMNode);
      return;
    }

    tagManager.updateTagsAndAttributes(tagsMap, attributesMap, leafMNode);
  }

  private void upsertAlias(String alias, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {
    // upsert alias
    if (alias != null && !alias.equals(leafMNode.getAlias())) {
      if (!leafMNode.getParent().addAlias(alias, leafMNode)) {
        throw new MetadataException("The alias already exists.");
      }

      if (leafMNode.getAlias() != null) {
        leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
      }

      leafMNode.setAlias(alias);
      // persist to WAL
      logWriter.changeAlias(fullPath, alias);
    }
  }

  /**
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      return;
    }

    tagManager.addAttributes(attributesMap, fullPath, leafMNode);
  }

  /**
   * add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      tagManager.addIndex(tagsMap, leafMNode);
      return;
    }

    tagManager.addTags(tagsMap, fullPath, leafMNode);
  }

  /**
   * drop tags or attributes of the timeseries
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, just do nothing.
    if (leafMNode.getOffset() < 0) {
      return;
    }
    tagManager.dropTagsOrAttributes(keySet, fullPath, leafMNode);
  }

  /**
   * set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
    }

    // tags, attributes
    tagManager.setTagsOrAttributesValue(alterMap, fullPath, leafMNode);
  }

  /**
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey),
          true);
    }
    // tags, attributes
    tagManager.renameTagOrAttributeKey(oldKey, newKey, fullPath, leafMNode);
  }

  /** remove the node from the tag inverted index */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(IMeasurementMNode node) throws IOException {
    tagManager.removeFromTagInvertedIndex(node);
  }
  // endregion

  // region Interfaces only for Cluster module usage

  /**
   * Collect the timeseries schemas as IMeasurementSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas) {
    try {
      mtree.collectMeasurementSchema(prefixPath, measurementSchemas);
    } catch (MetadataException ignored) {
      // do nothing
    }
  }

  /**
   * Collect the timeseries schemas as TimeseriesSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas) {
    try {
      mtree.collectTimeseriesSchema(prefixPath, timeseriesSchemas);
    } catch (MetadataException ignored) {
      // do nothing
    }
  }

  /**
   * For a path, infer all storage groups it may belong to. The path can have wildcards. Resolve the
   * path or path pattern into StorageGroupName-FullPath pairs that FullPath matches the given path.
   *
   * <p>Consider the path into two parts: (1) the sub path which can not contain a storage group
   * name and (2) the sub path which is substring that begin after the storage group name.
   *
   * <p>(1) Suppose the part of the path can not contain a storage group name (e.g.,
   * "root".contains("root.sg") == false), then: For each one level wildcard *, only one level will
   * be inferred and the wildcard will be removed. For each multi level wildcard **, then the
   * inference will go on until the storage groups are found and the wildcard will be kept. (2)
   * Suppose the part of the path is a substring that begin after the storage group name. (e.g., For
   * "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part is "a.*.b.*"). For
   * this part, keep what it is.
   *
   * <p>Assuming we have three SGs: root.group1, root.group2, root.area1.group3 Eg1: for input
   * "root.**", returns ("root.group1", "root.group1.**"), ("root.group2", "root.group2.**")
   * ("root.area1.group3", "root.area1.group3.**") Eg2: for input "root.*.s1", returns
   * ("root.group1", "root.group1.s1"), ("root.group2", "root.group2.s1")
   *
   * <p>Eg3: for input "root.area1.**", returns ("root.area1.group3", "root.area1.group3.**")
   *
   * @param path can be a path pattern or a full path.
   * @return StorageGroupName-FullPath pairs
   * @apiNote :for cluster
   */
  public Map<String, String> groupPathByStorageGroup(PartialPath path) throws MetadataException {
    Map<String, String> sgPathMap = mtree.groupPathByStorageGroup(path);
    if (logger.isDebugEnabled()) {
      logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    }
    return sgPathMap;
  }

  /**
   * if the path is in local mtree, nothing needed to do (because mtree is in the memory); Otherwise
   * cache the path to mRemoteSchemaCache
   */
  public void cacheMeta(
      PartialPath path, IMeasurementMNode measurementMNode, boolean needSetFullPath) {
    // do nothing
  }

  /**
   * StorageGroupFilter filters unsatisfied storage groups in metadata queries to speed up and
   * deduplicate.
   */
  @FunctionalInterface
  public interface StorageGroupFilter {

    boolean satisfy(String storageGroup);
  }
  // endregion

  // region Interfaces for lastCache operations
  /**
   * Update the last cache value of time series of given seriesPath.
   *
   * <p>MManager will use the seriesPath to search the node first and then process the lastCache in
   * the MeasurementMNode
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param seriesPath the PartialPath of full path from root to UnaryMeasurement or the
   *     VectorPartialPath contains target subMeasurement
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to update last cache for the {}, err:{}", seriesPath, e.getMessage());
      return;
    }

    LastCacheManager.updateLastCache(
        seriesPath, timeValuePair, highPriorityUpdate, latestFlushedTime, node);
  }

  /**
   * Update the last cache value in given unary MeasurementMNode. Vector lastCache operation won't
   * work.
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param node the measurementMNode holding the lastCache, must be unary measurement
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      UnaryMeasurementMNode node,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    LastCacheManager.updateLastCache(
        node.getPartialPath(), timeValuePair, highPriorityUpdate, latestFlushedTime, node);
  }

  /**
   * Update the last cache value of subMeasurement given Vector MeasurementMNode.
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @param subMeasurement the subMeasurement of aligned timeseries
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      MultiMeasurementMNode node,
      String subMeasurement,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    LastCacheManager.updateLastCache(
        new VectorPartialPath(node.getPartialPath(), subMeasurement),
        timeValuePair,
        highPriorityUpdate,
        latestFlushedTime,
        node);
  }

  /**
   * Get the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param seriesPath the PartialPath of full path from root to UnaryMeasurement or the
   *     VectorPartialPath contains target subMeasurement
   * @return the last cache value
   */
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to get last cache for the {}, err:{}", seriesPath, e.getMessage());
      return null;
    }

    return LastCacheManager.getLastCache(seriesPath, node);
  }

  /**
   * Get the last cache value in given unary MeasurementMNode. Vector case won't work.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param node the measurementMNode holding the lastCache, must be unary measurement
   * @return the last cache value
   */
  public TimeValuePair getLastCache(UnaryMeasurementMNode node) {
    return LastCacheManager.getLastCache(node.getPartialPath(), node);
  }

  /**
   * Get the last cache value of given subMeasurement of given MeasurementMNode. Must be Vector
   * case.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @param subMeasurement the subMeasurement of aligned timeseries
   * @return the last cache value
   */
  public TimeValuePair getLastCache(MultiMeasurementMNode node, String subMeasurement) {
    return LastCacheManager.getLastCache(
        new VectorPartialPath(node.getPartialPath(), subMeasurement), node);
  }

  /**
   * Reset the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * @param seriesPath the PartialPath of full path from root to UnaryMeasurement or the
   *     VectorPartialPath contains target subMeasurement
   */
  public void resetLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to reset last cache for the {}, err:{}", seriesPath, e.getMessage());
      return;
    }

    LastCacheManager.resetLastCache(seriesPath, node);
  }

  /**
   * delete all the last cache value of any timeseries or aligned timeseries under the device
   *
   * <p>Invoking scenario (1) after upload tsfile
   *
   * @param deviceId path of device
   */
  public void deleteLastCacheByDevice(PartialPath deviceId) throws MetadataException {
    IMNode node = getDeviceNode(deviceId);
    if (node.isEntity()) {
      LastCacheManager.deleteLastCacheByDevice(node.getAsEntityMNode());
    }
  }

  /**
   * delete the last cache value of timeseries or subMeasurement of some aligned timeseries, which
   * is under the device and matching the originalPath
   *
   * <p>Invoking scenario (1) delete timeseries
   *
   * @param deviceId path of device
   * @param originalPath origin timeseries path
   * @param startTime startTime
   * @param endTime endTime
   */
  public void deleteLastCacheByDevice(
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)
      throws MetadataException {
    IMNode node = IoTDB.metaManager.getDeviceNode(deviceId);
    if (node.isEntity()) {
      LastCacheManager.deleteLastCacheByDevice(
          node.getAsEntityMNode(), originalPath, startTime, endTime);
    }
  }
  // endregion

  // region Interfaces and Implementation for InsertPlan process
  /** get schema for device. Attention!!! Only support insertPlan */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    PartialPath prefixPath = plan.getPrefixPath();
    PartialPath deviceId = prefixPath;
    String vectorId = null;
    if (plan.isAligned()) {
      deviceId = prefixPath.getDevicePath();
      vectorId = prefixPath.getMeasurement();
    }
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device node
    IMNode deviceMNode = getDeviceNodeWithAutoCreate(deviceId);

    // check insert non-aligned InsertPlan for aligned timeseries
    if (deviceMNode.isMeasurement()
        && deviceMNode.getAsMeasurementMNode().getSchema() instanceof VectorMeasurementSchema
        && !plan.isAligned()) {
      throw new MetadataException(
          String.format(
              "Path [%s] is an aligned timeseries, please set InsertPlan.isAligned() = true",
              prefixPath));
    }
    // check insert aligned InsertPlan for non-aligned timeseries
    else if (plan.isAligned()
        && deviceMNode.getChild(vectorId) != null
        && !(deviceMNode.getChild(vectorId).isMeasurement())) {
      throw new MetadataException(
          String.format(
              "Path [%s] is not an aligned timeseries, please set InsertPlan.isAligned() = false",
              prefixPath));
    }

    // 2. get schema of each measurement
    // if do not have measurement
    IMeasurementMNode measurementMNode;
    for (int i = 0; i < measurementList.length; i++) {
      try {
        String measurement = measurementList[i];
        measurementMNode =
            getMeasurementMNode(deviceMNode, plan.isAligned() ? vectorId : measurement);
        if (measurementMNode == null) {
          measurementMNode = findTemplate(deviceMNode, measurement, vectorId);
        }
        if (measurementMNode == null) {
          if (!config.isAutoCreateSchemaEnabled()) {
            throw new PathNotExistException(deviceId + PATH_SEPARATOR + measurement);
          } else {
            if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
              if (!plan.isAligned()) {
                internalCreateTimeseries(
                    prefixPath.concatNode(measurement), plan.getDataTypes()[i]);
                // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
                deviceMNode = mtree.getNodeByPath(deviceId);
                measurementMNode = deviceMNode.getChild(measurement).getAsMeasurementMNode();
              } else {
                internalAlignedCreateTimeseries(
                    prefixPath, Arrays.asList(measurementList), Arrays.asList(plan.getDataTypes()));
                // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
                deviceMNode = mtree.getNodeByPath(deviceId);
                measurementMNode = deviceMNode.getChild(vectorId).getAsMeasurementMNode();
              }
            } else {
              throw new MetadataException(
                  String.format(
                      "Only support insertRow and insertTablet, plan is [%s]",
                      plan.getOperatorType()));
            }
          }
        }

        // check type is match
        TSDataType insertDataType;
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          if (plan.isAligned()) {
            TSDataType dataTypeInNode =
                measurementMNode.getSchema().getSubMeasurementsTSDataTypeList().get(i);
            insertDataType = plan.getDataTypes()[i];
            if (insertDataType == null) {
              insertDataType = dataTypeInNode;
            }
            if (dataTypeInNode != insertDataType) {
              logger.warn(
                  "DataType mismatch, Insert measurement {} in {} type {}, metadata tree type {}",
                  measurementMNode.getSchema().getSubMeasurementsList().get(i),
                  measurementList[i],
                  insertDataType,
                  dataTypeInNode);
              DataTypeMismatchException mismatchException =
                  new DataTypeMismatchException(measurementList[i], insertDataType, dataTypeInNode);
              if (!config.isEnablePartialInsert()) {
                throw mismatchException;
              } else {
                // mark failed measurement
                plan.markFailedMeasurementAlignedInsertion(mismatchException);
                for (int j = 0; j < i; j++) {
                  // all the measurementMNodes should be null
                  measurementMNodes[j] = null;
                }
                break;
              }
            }
            measurementMNodes[i] = measurementMNode;
          } else {
            if (plan instanceof InsertRowPlan) {
              if (!((InsertRowPlan) plan).isNeedInferType()) {
                // only when InsertRowPlan's values is object[], we should check type
                insertDataType = getTypeInLoc(plan, i);
              } else {
                insertDataType = measurementMNode.getSchema().getType();
              }
            } else {
              insertDataType = getTypeInLoc(plan, i);
            }
            if (measurementMNode.getSchema().getType() != insertDataType) {
              logger.warn(
                  "DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
                  measurementList[i],
                  insertDataType,
                  measurementMNode.getSchema().getType());
              DataTypeMismatchException mismatchException =
                  new DataTypeMismatchException(
                      measurementList[i], insertDataType, measurementMNode.getSchema().getType());
              if (!config.isEnablePartialInsert()) {
                throw mismatchException;
              } else {
                // mark failed measurement
                plan.markFailedMeasurementInsertion(i, mismatchException);
                continue;
              }
            }
            measurementMNodes[i] = measurementMNode;
            // set measurementName instead of alias
            measurementList[i] = measurementMNode.getName();
          }
        }
      } catch (MetadataException e) {
        if (IoTDB.isClusterMode()) {
          logger.debug(
              "meet error when check {}.{}, message: {}",
              deviceId,
              measurementList[i],
              e.getMessage());
        } else {
          logger.warn(
              "meet error when check {}.{}, message: {}",
              deviceId,
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

  private IMeasurementMNode findTemplate(IMNode deviceMNode, String measurement, String vectorId)
      throws MetadataException {
    Template curTemplate = deviceMNode.getUpperTemplate();
    if (curTemplate != null) {
      Map<String, IMeasurementSchema> curTemplateMap = curTemplate.getSchemaMap();

      String schemaName = vectorId != null ? vectorId : measurement;
      IMeasurementSchema schema = curTemplateMap.get(schemaName);
      if (!deviceMNode.isUseTemplate()) {
        deviceMNode = setUsingSchemaTemplate(deviceMNode);
      }

      if (schema != null) {
        if (schema instanceof UnaryMeasurementSchema) {
          return MeasurementMNode.getMeasurementMNode(
              deviceMNode.getAsEntityMNode(), measurement, schema, null);
        } else if (schema instanceof VectorMeasurementSchema) {
          return MeasurementMNode.getMeasurementMNode(
              deviceMNode.getAsEntityMNode(), vectorId, schema, null);
        }
      }
      return null;
    }
    return null;
  }

  /** create timeseries ignoring PathAlreadyExistException */
  private void internalCreateTimeseries(PartialPath path, TSDataType dataType)
      throws MetadataException {
    createTimeseries(
        path,
        dataType,
        getDefaultEncoding(dataType),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  /** create aligned timeseries ignoring PathAlreadyExistException */
  private void internalAlignedCreateTimeseries(
      PartialPath prefixPath, List<String> measurements, List<TSDataType> dataTypes)
      throws MetadataException {
    List<TSEncoding> encodings = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      encodings.add(getDefaultEncoding(dataType));
    }
    createAlignedTimeSeries(
        prefixPath,
        measurements,
        dataTypes,
        encodings,
        TSFileDescriptor.getInstance().getConfig().getCompressor());
  }
  // endregion

  // region Interfaces and Implementation for Template operations
  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    try {
      templateManager.createSchemaTemplate(plan);
      // write wal
      if (!isRecovering) {
        logWriter.createSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public synchronized void setSchemaTemplate(SetSchemaTemplatePlan plan) throws MetadataException {
    // get mnode and update template should be atomic
    Template template = templateManager.getTemplate(plan.getTemplateName());

    try {
      PartialPath path = new PartialPath(plan.getPrefixPath());

      mtree.checkTemplateOnPath(path);

      IMNode node = getDeviceNodeWithAutoCreate(path);

      templateManager.checkIsTemplateAndMNodeCompatible(template, node);

      node.setSchemaTemplate(template);

      // write wal
      if (!isRecovering) {
        logWriter.setSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public synchronized void unsetSchemaTemplate(UnsetSchemaTemplatePlan plan)
      throws MetadataException {
    // get mnode should be atomic
    try {
      PartialPath path = new PartialPath(plan.getPrefixPath());
      IMNode node = mtree.getNodeByPath(path);
      if (node.getSchemaTemplate() == null) {
        throw new NoTemplateOnMNodeException(plan.getPrefixPath());
      } else if (!node.getSchemaTemplate().getName().equals(plan.getTemplateName())) {
        throw new DifferentTemplateException(plan.getPrefixPath(), plan.getTemplateName());
      } else if (node.isUseTemplate()) {
        throw new TemplateIsInUseException(plan.getPrefixPath());
      }
      mtree.checkTemplateInUseOnLowerNode(node);
      node.setSchemaTemplate(null);
      // write wal
      if (!isRecovering) {
        logWriter.unsetSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void setUsingSchemaTemplate(SetUsingSchemaTemplatePlan plan) throws MetadataException {
    try {
      setUsingSchemaTemplate(getDeviceNode(plan.getPrefixPath()));
    } catch (PathNotExistException e) {
      // the order of SetUsingSchemaTemplatePlan and AutoCreateDeviceMNodePlan cannot be guaranteed
      // when writing concurrently, so we need a auto-create mechanism here
      try {
        getDeviceNodeWithAutoCreate(plan.getPrefixPath());
      } catch (IOException ioException) {
        throw new MetadataException(ioException);
      }
      setUsingSchemaTemplate(getDeviceNode(plan.getPrefixPath()));
    }
  }

  IEntityMNode setUsingSchemaTemplate(IMNode node) throws MetadataException {
    // this operation may change mtree structure and node type
    // invoke mnode.setUseTemplate is invalid
    IEntityMNode entityMNode = mtree.setToEntity(node);
    entityMNode.setUseTemplate(true);
    if (node != entityMNode) {
      mNodeCache.removeObject(entityMNode.getPartialPath());
    }
    if (!isRecovering) {
      try {
        logWriter.setUsingSchemaTemplate(node.getPartialPath());
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
    return entityMNode;
  }
  // endregion

  // region TestOnly Interfaces
  /**
   * To reduce the String number in memory, use the deviceId from MManager instead of the deviceId
   * read from disk
   *
   * @param devicePath read from disk
   * @return deviceId
   */
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

  /**
   * Attention!!!!!, this method could only be used for Tests involving multiple mmanagers. The
   * singleton of templateManager and tagManager will cause interference between mmanagers if one of
   * the mmanagers invoke init method or clear method
   */
  @TestOnly
  public void initForMultiMManagerTest() {
    templateManager = TemplateManager.getNewInstanceForTest();
    tagManager = TagManager.getNewInstanceForTest();
    init();
  }

  @TestOnly
  public void flushAllMlogForTest() throws IOException {
    logWriter.close();
  }
  // endregion
}
