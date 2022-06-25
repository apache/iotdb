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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
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
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTree;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
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
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
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
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private LoadingCache<PartialPath, IMNode> mNodeCache;
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
        Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .build(
                new com.github.benmanes.caffeine.cache.CacheLoader<PartialPath, IMNode>() {
                  @Override
                  public @Nullable IMNode load(@NonNull PartialPath partialPath)
                      throws MetadataException {

                    return mtree.getNodeByPathWithStorageGroupCheck(partialPath);
                  }
                });

    if (config.isEnableMTreeSnapshot()) {
      timedCreateMTreeSnapshotThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedCreateMTreeSnapshot");
      timedCreateMTreeSnapshotThread.scheduleAtFixedRate(
          this::checkMTreeModified,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          TimeUnit.SECONDS);
    }

    if (config.getSyncMlogPeriodInMs() != 0) {
      timedForceMLogThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("timedForceMLogThread");

      timedForceMLogThread.scheduleAtFixedRate(
          this::forceMlog,
          config.getSyncMlogPeriodInMs(),
          config.getSyncMlogPeriodInMs(),
          TimeUnit.MILLISECONDS);
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

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      startStatisticCounts();
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.MEM.toString(),
              MetricLevel.IMPORTANT,
              mtree,
              RamUsageEstimator::sizeOf,
              Tag.NAME.toString(),
              "mtree");
    }
  }

  private void startStatisticCounts() {
    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            totalSeriesNumber,
            AtomicLong::get,
            Tag.NAME.toString(),
            "timeSeries");

    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mtree,
            tree -> {
              try {
                return tree.getDevicesNum(new PartialPath("root.**"));
              } catch (MetadataException e) {
                logger.error("get deviceNum error", e);
              }
              return 0;
            },
            Tag.NAME.toString(),
            "device");

    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mtree,
            tree -> {
              try {
                return tree.getStorageGroupNum(new PartialPath("root.**"));
              } catch (MetadataException e) {
                logger.error("get storageGroupNum error", e);
              }
              return 0;
            },
            Tag.NAME.toString(),
            "storageGroup");
  }

  private void forceMlog() {
    try {
      logWriter.force();
    } catch (IOException e) {
      logger.error("Cannot force mlog to the storage device", e);
    }
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
        this.mNodeCache.invalidateAll();
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
      case DROP_TEMPLATE:
        DropTemplatePlan dropTemplatePlan = (DropTemplatePlan) plan;
        dropSchemaTemplate(dropTemplatePlan);
        break;
      case APPEND_TEMPLATE:
        AppendTemplatePlan appendTemplatePlan = (AppendTemplatePlan) plan;
        appendSchemaTemplate(appendTemplatePlan);
        break;
      case PRUNE_TEMPLATE:
        PruneTemplatePlan pruneTemplatePlan = (PruneTemplatePlan) plan;
        pruneSchemaTemplate(pruneTemplatePlan);
        break;
      case SET_TEMPLATE:
        SetTemplatePlan setTemplatePlan = (SetTemplatePlan) plan;
        setSchemaTemplate(setTemplatePlan);
        break;
      case ACTIVATE_TEMPLATE:
        ActivateTemplatePlan activateTemplatePlan = (ActivateTemplatePlan) plan;
        setUsingSchemaTemplate(activateTemplatePlan);
        break;
      case AUTO_CREATE_DEVICE_MNODE:
        AutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan = (AutoCreateDeviceMNodePlan) plan;
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        break;
      case UNSET_TEMPLATE:
        UnsetTemplatePlan unsetTemplatePlan = (UnsetTemplatePlan) plan;
        unsetSchemaTemplate(unsetTemplatePlan);
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
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
      mNodeCache.invalidate(path.getDevicePath());

      // update tag index

      if (offset != -1 && isRecovering) {
        // the timeseries has already been created and now system is recovering, using the tag info
        // in tagFile to recover index directly
        tagManager.recoverIndex(offset, leafMNode);
      } else if (plan.getTags() != null) {
        // tag key, tag value
        tagManager.addIndex(plan.getTags(), leafMNode);
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

    // update id table if not in recovering or disable id table log file
    if (config.isEnableIDTable() && (!isRecovering || !config.isEnableIDTableLogFile())) {
      IDTable idTable = IDTableManager.getInstance().getIDTable(plan.getPath().getDevicePath());
      idTable.createTimeseries(plan);
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
          prefixPath,
          measurements,
          plan.getDataTypes(),
          plan.getEncodings(),
          plan.getCompressors());

      // the cached mNode may be replaced by new entityMNode in mtree
      mNodeCache.invalidate(prefixPath);

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

    // update id table if not in recovering or disable id table log file
    if (config.isEnableIDTable() && (!isRecovering || !config.isEnableIDTableLogFile())) {
      IDTable idTable = IDTableManager.getInstance().getIDTable(plan.getPrefixPath());
      idTable.createAlignedTimeseries(plan);
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
   * Delete all timeseries matching the given path pattern, may cross different storage group. If
   * using prefix match, the path pattern is used to match prefix path. All timeseries start with
   * the matched prefix path will be deleted.
   *
   * @param pathPattern path to be deleted
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    try {
      List<MeasurementPath> allTimeseries = mtree.getMeasurementPaths(pathPattern, isPrefixMatch);
      if (allTimeseries.isEmpty()) {
        // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
        // nodes that do not have the metadata of the timeseries, the coordinator expects a
        // PathNotExistException.
        throw new PathNotExistException(pathPattern.getFullPath());
      }

      Set<String> failedNames = new HashSet<>();
      for (PartialPath p : allTimeseries) {
        deleteSingleTimeseriesInternal(p, failedNames);
      }
      return failedNames.isEmpty() ? null : String.join(",", failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Delete all timeseries matching the given path pattern, may cross different storage group
   *
   * @param pathPattern path to be deleted
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    return deleteTimeseries(pathPattern, false);
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
    removeFromTagInvertedIndex(measurementMNode);
    PartialPath storageGroupPath = pair.left;

    // drop trigger with no exceptions
    TriggerEngine.drop(pair.right);

    IMNode node = measurementMNode.getParent();

    if (node.isUseTemplate() && node.getSchemaTemplate().hasSchema(measurementMNode.getName())) {
      // measurement represent by template doesn't affect the MTree structure and memory control
      return storageGroupPath;
    }

    while (node.isEmptyInternal()) {
      mNodeCache.invalidate(node.getPartialPath());
      node = node.getParent();
    }
    totalSeriesNumber.addAndGet(-1);
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
        mNodeCache.invalidateAll();

        // try to delete storage group
        List<IMeasurementMNode> leafMNodes = mtree.deleteStorageGroup(storageGroup);
        for (IMeasurementMNode leafMNode : leafMNodes) {
          removeFromTagInvertedIndex(leafMNode);
        }

        // unmark all storage group from related templates
        for (Template template : templateManager.getTemplateMap().values()) {
          template.unmarkStorageGroups(storageGroups);
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
    } catch (Exception e) {
      if (e.getCause() instanceof MetadataException) {
        if (!autoCreateSchema) {
          throw new PathNotExistException(path.getFullPath());
        }
        shouldSetStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
      } else {
        throw e;
      }
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
      if (!(node.isStorageGroup()) && !isRecovering) {
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
      if (!(node.isStorageGroup()) && !isRecovering) {
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
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, isPrefixMatch);
  }

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  /**
   * To calculate the count of devices for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getDevicesNum(pathPattern, isPrefixMatch);
  }

  /** To calculate the count of devices for given path pattern. */
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
  }

  /**
   * To calculate the count of storage group for given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   */
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getStorageGroupNum(pathPattern, isPrefixMatch);
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
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
  }

  /**
   * To calculate the count of nodes in the given level for given path pattern.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
      throws MetadataException {
    return getNodesCountInGivenLevel(pathPattern, level, false);
  }

  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    return mtree.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
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
   * Get all storage group matching given path pattern. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern a pattern of a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return A ArrayList instance which stores storage group paths matching given path pattern.
   */
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getMatchedStorageGroups(pathPattern, isPrefixMatch);
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
   * Get all device paths matching the path pattern. If using prefix match, the path pattern is used
   * to match prefix path. All timeseries start with the matched prefix path will be collected.
   *
   * @param pathPattern the pattern of the target devices.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path.
   * @return A HashSet instance which stores devices paths matching the given path pattern.
   */
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getDevices(pathPattern, isPrefixMatch);
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
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'. If using prefix match, the path pattern is used to match prefix path. All timeseries start
   * with the matched prefix path will be collected.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch).left;
  }

  /**
   * Return all measurement paths for given path if the path is abstract. Or return the path itself.
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   */
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
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getMeasurementPathsWithAlias(pathPattern, limit, offset, isPrefixMatch);
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
      if (plan.isPrefixMatch()
          ? pathPattern.matchPrefixPath(leaf.getPartialPath())
          : pathPattern.matchFullPath(leaf.getPartialPath())) {
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
                  leaf.getLastCacheContainer().getCachedLast() != null
                      ? leaf.getLastCacheContainer().getCachedLast().getTimestamp()
                      : 0,
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
      ans = mtree.getAllMeasurementSchemaByHeatOrder(plan, context);
    } else {
      ans = mtree.getAllMeasurementSchema(plan);
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
                ansString.left.getFullPath(),
                ansString.right[0],
                ansString.right[1],
                TSDataType.valueOf(ansString.right[2]),
                TSEncoding.valueOf(ansString.right[3]),
                CompressionType.valueOf(ansString.right[4]),
                ansString.right[6] != null ? Long.parseLong(ansString.right[6]) : 0,
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
    return getSeriesSchema(fullPath).getType();
  }

  /**
   * Get schema of paritialPath
   *
   * @param fullPath (may be ParitialPath or AlignedPath)
   * @return MeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    return getMeasurementMNode(fullPath).getSchema();
  }

  // attention: this path must be a device node
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    List<MeasurementPath> res = new LinkedList<>();
    try {
      IMNode node = mNodeCache.get(devicePath);

      for (IMNode child : node.getChildren().values()) {
        if (child.isMeasurement()) {
          IMeasurementMNode measurementMNode = child.getAsMeasurementMNode();
          res.add(measurementMNode.getMeasurementPath());
        }
      }

      // template
      Template template = node.getUpperTemplate();
      if (node.isUseTemplate() && template != null) {
        MeasurementPath measurementPath;
        for (IMeasurementSchema schema : template.getSchemaMap().values()) {
          measurementPath =
              new MeasurementPath(devicePath.concatNode(schema.getMeasurementId()), schema);
          measurementPath.setUnderAlignedEntity(node.getAsEntityMNode().isAligned());
          res.add(measurementPath);
        }
      }
    } catch (Exception e) {
      if (e.getCause() instanceof MetadataException) {
        throw new PathNotExistException(devicePath.getFullPath());
      }
      throw e;
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
    ensureStorageGroup(path);
    return mtree.getStorageGroupNodeByPath(path);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    IMNode node;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (Exception e) {
      if (e.getCause() instanceof MetadataException) {
        throw new PathNotExistException(path.getFullPath());
      }
      throw e;
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
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    Map<String, List<PartialPath>> sgPathMap = mtree.groupPathByStorageGroup(path);
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
   * @param seriesPath the PartialPath of full path from root to Measurement
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

    LastCacheManager.updateLastCache(node, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /**
   * Update the last cache value in given MeasurementMNode. work.
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   */
  public void updateLastCache(
      IMeasurementMNode node,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    LastCacheManager.updateLastCache(node, timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /**
   * Get the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param seriesPath the PartialPath of full path from root to Measurement
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

    return LastCacheManager.getLastCache(node);
  }

  /**
   * Get the last cache value in given MeasurementMNode.
   *
   * <p>Invoking scenario: last cache read during last Query
   *
   * @param node the measurementMNode holding the lastCache
   * @return the last cache value
   */
  public TimeValuePair getLastCache(IMeasurementMNode node) {
    return LastCacheManager.getLastCache(node);
  }

  /**
   * Reset the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.The LastCacheContainer will be set as the EmptyLastCacheContainer.
   *
   * @param seriesPath the PartialPath of full path from root to Measurement
   */
  @TestOnly
  public void resetLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = getMeasurementMNode(seriesPath);
    } catch (MetadataException e) {
      logger.warn("failed to reset last cache for the {}, err:{}", seriesPath, e.getMessage());
      return;
    }

    LastCacheManager.resetLastCache(node);
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
    // devicePath is a logical path which is parent of measurement, whether in template or not
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device node, set using template if accessed.
    boolean mountedNodeFound = false;
    // check every measurement path
    for (String measurementId : measurementList) {
      PartialPath fullPath = devicePath.concatNode(measurementId);
      int index = mtree.getMountedNodeIndexOnMeasurementPath(fullPath);
      if ((index != fullPath.getNodeLength() - 1) && !mountedNodeFound) {
        // this measurement is in template, need to assure mounted node exists and set using
        // template.
        // Without allowing overlap of template and MTree, this block run only once
        String[] mountedPathNodes = Arrays.copyOfRange(fullPath.getNodes(), 0, index + 1);
        IMNode mountedNode = getDeviceNodeWithAutoCreate(new PartialPath(mountedPathNodes));
        if (!mountedNode.isUseTemplate()) {
          setUsingSchemaTemplate(mountedNode);
        }
        mountedNodeFound = true;
      }
    }
    // get logical device node, may be in template. will be multiple if overlap is allowed.
    IMNode deviceMNode = getDeviceNodeWithAutoCreate(devicePath);

    // check insert non-aligned InsertPlan for aligned timeseries
    if (deviceMNode.isEntity()) {
      if (plan.isAligned()) {
        if (!deviceMNode.getAsEntityMNode().isAligned()) {
          throw new MetadataException(
              String.format(
                  "Timeseries under path [%s] is not aligned , please set InsertPlan.isAligned() = false",
                  plan.getDevicePath()));
        }
      } else {
        if (deviceMNode.getAsEntityMNode().isAligned()) {
          throw new MetadataException(
              String.format(
                  "Timeseries under path [%s] is aligned , please set InsertPlan.isAligned() = true",
                  plan.getDevicePath()));
        }
      }
    }

    // 2. get schema of each measurement
    IMeasurementMNode measurementMNode;
    for (int i = 0; i < measurementList.length; i++) {
      try {
        // get MeasurementMNode, auto create if absent
        Pair<IMNode, IMeasurementMNode> pair =
            getMeasurementMNodeForInsertPlan(plan, i, deviceMNode);
        deviceMNode = pair.left;
        measurementMNode = pair.right;

        // check type is match
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          try {
            checkDataTypeMatch(plan, i, measurementMNode.getSchema().getType());
          } catch (DataTypeMismatchException mismatchException) {
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

  private Pair<IMNode, IMeasurementMNode> getMeasurementMNodeForInsertPlan(
      InsertPlan plan, int loc, IMNode deviceMNode) throws MetadataException {
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    String measurement = measurementList[loc];
    IMeasurementMNode measurementMNode = getMeasurementMNode(deviceMNode, measurement);
    if (measurementMNode == null) {
      measurementMNode = findMeasurementInTemplate(deviceMNode, measurement);
    }
    if (measurementMNode == null) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw new PathNotExistException(devicePath + PATH_SEPARATOR + measurement);
      } else {
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          if (!plan.isAligned()) {
            internalCreateTimeseries(devicePath.concatNode(measurement), plan.getDataTypes()[loc]);
          } else {
            internalAlignedCreateTimeseries(
                devicePath,
                Collections.singletonList(measurement),
                Collections.singletonList(plan.getDataTypes()[loc]));
          }
          // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
          deviceMNode = mtree.getNodeByPath(devicePath);
          measurementMNode = deviceMNode.getChild(measurement).getAsMeasurementMNode();
        } else {
          throw new MetadataException(
              String.format(
                  "Only support insertRow and insertTablet, plan is [%s]", plan.getOperatorType()));
        }
      }
    }
    return new Pair<>(deviceMNode, measurementMNode);
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

  private IMeasurementMNode findMeasurementInTemplate(IMNode deviceMNode, String measurement)
      throws MetadataException {
    Template curTemplate = deviceMNode.getUpperTemplate();
    if (curTemplate != null) {
      IMeasurementSchema schema = curTemplate.getSchema(measurement);
      if (!deviceMNode.isUseTemplate()) {
        deviceMNode = setUsingSchemaTemplate(deviceMNode);
      }

      if (schema != null) {
        return MeasurementMNode.getMeasurementMNode(
            deviceMNode.getAsEntityMNode(), measurement, schema, null);
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
    List<CompressionType> compressors = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      encodings.add(getDefaultEncoding(dataType));
      compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
    }
    createAlignedTimeSeries(prefixPath, measurements, dataTypes, encodings, compressors);
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    try {

      List<List<TSDataType>> dataTypes = plan.getDataTypes();
      List<List<TSEncoding>> encodings = plan.getEncodings();
      for (int i = 0; i < dataTypes.size(); i++) {
        for (int j = 0; j < dataTypes.get(i).size(); j++) {
          SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i).get(j), encodings.get(i).get(j));
        }
      }

      templateManager.createSchemaTemplate(plan);
      // write wal
      if (!isRecovering) {
        logWriter.createSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    if (templateManager.getTemplate(plan.getName()) == null) {
      throw new MetadataException(String.format("Template [%s] does not exist.", plan.getName()));
    }

    if (!mtree.isTemplateAppendable(
        templateManager.getTemplate(plan.getName()), plan.getMeasurements())) {
      throw new MetadataException(
          String.format(
              "Template [%s] cannot be appended for overlapping of new measurement and MTree",
              plan.getName()));
    }

    try {
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();
      for (int idx = 0; idx < dataTypes.size(); idx++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(idx), encodings.get(idx));
      }

      templateManager.appendSchemaTemplate(plan);
      // write wal
      if (!isRecovering) {
        logWriter.appendSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void pruneSchemaTemplate(PruneTemplatePlan plan) throws MetadataException {
    if (templateManager.getTemplate(plan.getName()) == null) {
      throw new MetadataException(String.format("Template [%s] does not exist.", plan.getName()));
    }

    if (templateManager.getTemplate(plan.getName()).getRelatedStorageGroup().size() > 0) {
      throw new MetadataException(
          String.format(
              "Template [%s] cannot be pruned since had been set before.", plan.getName()));
    }
    try {
      templateManager.pruneSchemaTemplate(plan);
      // write wal
      if (!isRecovering) {
        logWriter.pruneSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public int countMeasurementsInTemplate(String templateName) throws MetadataException {
    try {
      return templateManager.getTemplate(templateName).getMeasurementsCount();
    } catch (UndefinedTemplateException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * @param templateName name of template to check
   * @param path full path to check
   * @return if path correspond to a measurement in template
   * @throws MetadataException
   */
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws MetadataException {
    return templateManager.getTemplate(templateName).isPathMeasurement(path);
  }

  public boolean isPathExistsInTemplate(String templateName, String path) throws MetadataException {
    return templateManager.getTemplate(templateName).isPathExistInTemplate(path);
  }

  public List<String> getMeasurementsInTemplate(String templateName, String path)
      throws MetadataException {
    return templateManager.getTemplate(templateName).getMeasurementsUnderPath(path);
  }

  public List<Pair<String, IMeasurementSchema>> getSchemasInTemplate(
      String templateName, String path) throws MetadataException {
    Set<Map.Entry<String, IMeasurementSchema>> rawSchemas =
        templateManager.getTemplate(templateName).getSchemaMap().entrySet();
    return rawSchemas.stream()
        .filter(e -> e.getKey().startsWith(path))
        .collect(
            ArrayList::new,
            (res, elem) -> res.add(new Pair<>(elem.getKey(), elem.getValue())),
            ArrayList::addAll);
  }

  public Set<String> getAllTemplates() {
    return templateManager.getAllTemplateName();
  }

  /**
   * Get all paths set designated template
   *
   * @param templateName designated template name, blank string for any template exists
   * @return paths set
   */
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    return templateName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
        ? new HashSet<>(mtree.getPathsSetOnTemplate(null))
        : new HashSet<>(mtree.getPathsSetOnTemplate(templateManager.getTemplate(templateName)));
  }

  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    return templateName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
        ? new HashSet<>(mtree.getPathsUsingTemplate(null))
        : new HashSet<>(mtree.getPathsUsingTemplate(templateManager.getTemplate(templateName)));
  }

  public void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException {
    try {
      String templateName = plan.getName();
      // check whether template exists
      if (!templateManager.getAllTemplateName().contains(templateName)) {
        throw new UndefinedTemplateException(templateName);
      }

      if (templateManager.getTemplate(plan.getName()).getRelatedStorageGroup().size() > 0) {
        throw new MetadataException(
            String.format(
                "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
      }

      templateManager.dropSchemaTemplate(plan);
      if (!isRecovering) {
        logWriter.dropSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public synchronized void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    // get mnode and update template should be atomic
    Template template = templateManager.getTemplate(plan.getTemplateName());

    try {
      PartialPath path = new PartialPath(plan.getPrefixPath());

      mtree.checkTemplateOnPath(path);

      IMNode node = getDeviceNodeWithAutoCreate(path);

      templateManager.checkTemplateCompatible(template, node);

      node.setSchemaTemplate(template);

      template.markStorageGroup(node);

      // write wal
      if (!isRecovering) {
        logWriter.setSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public synchronized void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
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
      templateManager.getTemplate(plan.getTemplateName()).unmarkStorageGroup(node);
      // write wal
      if (!isRecovering) {
        logWriter.unsetSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    // check whether any template has been set on designated path
    if (mtree.getTemplateOnPath(plan.getPrefixPath()) == null) {
      throw new MetadataException(
          String.format(
              "Path [%s] has not been set any template.", plan.getPrefixPath().toString()));
    }

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

  IMNode setUsingSchemaTemplate(IMNode node) throws MetadataException {
    // check whether any template has been set on designated path
    if (node.getUpperTemplate() == null) {
      throw new MetadataException(
          String.format("Path [%s] has not been set any template.", node.getFullPath()));
    }

    // this operation may change mtree structure and node type
    // invoke mnode.setUseTemplate is invalid

    // check alignment of template and mounted node
    // if direct measurement exists, node will be replaced
    IMNode mountedMNode =
        mtree.checkTemplateAlignmentWithMountedNode(node, node.getUpperTemplate());

    // if has direct measurement (be a EntityNode), to ensure alignment adapt with former node or
    // template
    if (mountedMNode.isEntity()) {
      mountedMNode
          .getAsEntityMNode()
          .setAligned(
              node.isEntity()
                  ? node.getAsEntityMNode().isAligned()
                  : node.getUpperTemplate().isDirectAligned());
    }
    mountedMNode.setUseTemplate(true);

    if (node != mountedMNode) {
      mNodeCache.invalidate(mountedMNode.getPartialPath());
    }
    if (!isRecovering) {
      try {
        logWriter.setUsingSchemaTemplate(node.getPartialPath());
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
    return mountedMNode;
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

  @TestOnly
  public Template getTemplate(String templateName) throws MetadataException {
    try {
      return templateManager.getTemplate(templateName);
    } catch (UndefinedTemplateException e) {
      throw new MetadataException(e);
    }
  }
  // endregion
}
