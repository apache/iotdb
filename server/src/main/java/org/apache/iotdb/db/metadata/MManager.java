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
import org.apache.iotdb.db.exception.metadata.*;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.*;
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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
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

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
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

  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final MManager INSTANCE = new MManager();
  }

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

  /** we should not use this function in other place, but only in IoTDB class */
  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
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

  /** @return line number of the logFile */
  @SuppressWarnings("squid:S3776")
  private int initFromLog(File logFile) throws IOException {
    long time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      int idx = 0;
      try (MLogReader mLogReader =
          new MLogReader(config.getSchemaDir(), MetadataConstant.METADATA_LOG); ) {
        idx = applyMlog(mLogReader);
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

  private int applyMlog(MLogReader mLogReader) {
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
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }

  public void createContinuousQuery(CreateContinuousQueryPlan plan) throws IOException {
    logWriter.createContinuousQuery(plan);
  }

  public void dropContinuousQuery(DropContinuousQueryPlan plan) throws IOException {
    logWriter.dropContinuousQuery(plan);
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  private void ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      mtree.getStorageGroupPath(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      setStorageGroup(storageGroupPath);
    }
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

  /**
   * Delete all timeseries under the given path, may cross different storage group
   *
   * @param prefixPath path to be deleted, could be root or a prefix path or a full path
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath prefixPath) throws MetadataException {
    if (isStorageGroup(prefixPath)) {
      mNodeCache.clear();
    }
    try {
      List<PartialPath> allTimeseries = mtree.getAllTimeseriesPath(prefixPath);
      if (allTimeseries.isEmpty()) {
        throw new PathNotExistException(prefixPath.getFullPath());
      }

      // for not support deleting part of aligned timeseies
      // should be removed after partial deletion is supported
      IMNode lastNode = getNodeByPath(allTimeseries.get(0));
      if (lastNode.isMeasurement()) {
        IMeasurementSchema schema = ((IMeasurementMNode) lastNode).getSchema();
        if (schema instanceof VectorMeasurementSchema) {
          if (schema.getValueMeasurementIdList().size() != allTimeseries.size()) {
            throw new AlignedTimeseriesException(
                "Not support deleting part of aligned timeseies!", prefixPath.getFullPath());
          } else {
            allTimeseries.add(lastNode.getPartialPath());
          }
        }
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

  /** remove the node from the tag inverted index */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(IMeasurementMNode node) throws IOException {
    tagManager.removeFromTagInvertedIndex(node);
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
    IMeasurementSchema schema = pair.right.getSchema();
    int timeseriesNum = 0;
    if (schema instanceof MeasurementSchema) {
      removeFromTagInvertedIndex(pair.right);
      timeseriesNum = 1;
    } else if (schema instanceof VectorMeasurementSchema) {
      timeseriesNum += schema.getValueTSDataTypeList().size();
    }
    PartialPath storageGroupPath = pair.left;

    // drop trigger with no exceptions
    TriggerEngine.drop(pair.right);

    // TODO: delete the path node and all its ancestors
    mNodeCache.clear();
    totalSeriesNumber.addAndGet(-timeseriesNum);
    if (!allowToCreateNewSeries
        && totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
      logger.info("Current series number {} come back to normal level", totalSeriesNumber);
      allowToCreateNewSeries = true;
    }
    return storageGroupPath;
  }

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
        totalSeriesNumber.addAndGet(-mtree.getAllTimeseriesCount(storageGroup));
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

  /**
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    return mtree.isStorageGroup(path);
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param path full path
   */
  public TSDataType getSeriesType(PartialPath path) throws MetadataException {
    if (path.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }

    if (path instanceof VectorPartialPath) {
      if (((VectorPartialPath) path).getSubSensorsPathList().size() != 1) {
        return TSDataType.VECTOR;
      } else {
        path = ((VectorPartialPath) path).getSubSensorsPathList().get(0);
      }
    }
    IMeasurementSchema schema = mtree.getSchema(path);
    if (schema instanceof MeasurementSchema) {
      return schema.getType();
    } else {
      List<String> measurements = schema.getValueMeasurementIdList();
      return schema.getValueTSDataTypeList().get(measurements.indexOf(path.getMeasurement()));
    }
  }

  public IMeasurementMNode[] getMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    IMeasurementMNode[] mNodes = new IMeasurementMNode[measurements.length];
    for (int i = 0; i < mNodes.length; i++) {
      try {
        mNodes[i] = (IMeasurementMNode) getNodeByPath(deviceId.concatNode(measurements[i]));
      } catch (PathNotExistException ignored) {
        logger.warn("{} does not exist in {}", measurements[i], deviceId);
      }
      if (mNodes[i] == null && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
        throw new MetadataException(measurements[i] + " does not exist in " + deviceId);
      }
    }
    return mNodes;
  }

  /**
   * Get all devices under given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   * @return A HashSet instance which stores devices paths with given prefixPath.
   */
  public Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevices(prefixPath);
  }

  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    return mtree.getDevices(plan);
  }

  /**
   * Get all nodes from the given level
   *
   * @param prefixPath can be a prefix of a full path. Can not be a full path. can not have
   *     wildcard. But, the level of the prefixPath can be smaller than the given level, e.g.,
   *     prefixPath = root.a while the given level is 5
   * @param nodeLevel the level can not be smaller than the level of the prefixPath
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesList(PartialPath prefixPath, int nodeLevel)
      throws MetadataException {
    return getNodesList(prefixPath, nodeLevel, null);
  }

  public List<PartialPath> getNodesList(
      PartialPath prefixPath, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
    return mtree.getNodesList(prefixPath, nodeLevel, filter);
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    return mtree.getStorageGroupPath(path);
  }

  /** Get all storage group paths */
  public List<PartialPath> getAllStorageGroupPaths() {
    return mtree.getAllStorageGroupPaths();
  }

  public List<PartialPath> searchAllRelatedStorageGroups(PartialPath path)
      throws MetadataException {
    return mtree.searchAllRelatedStorageGroups(path);
  }

  /**
   * Get all storage group under given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   * @return A ArrayList instance which stores storage group paths with given prefixPath.
   */
  public List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupPaths(prefixPath);
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  /**
   * Return all paths for given path if the path is abstract. Or return the path itself. Regular
   * expression in this method is formed by the amalgamation of seriesPath and the character '*'.
   *
   * @param prefixPath can be a prefix or a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesPath(prefixPath);
  }

  /** Similar to method getAllTimeseriesPath(), but return Path with alias alias. */
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath prefixPath, int limit, int offset) throws MetadataException {
    return mtree.getAllTimeseriesPathWithAlias(prefixPath, limit, offset);
  }

  /** To calculate the count of timeseries for given prefix path. */
  public int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesCount(prefixPath);
  }

  /** To calculate the count of devices for given prefix path. */
  public int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevicesNum(prefixPath);
  }

  /** To calculate the count of storage group for given prefix path. */
  public int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupNum(prefixPath);
  }

  /**
   * To calculate the count of nodes in the given level for given prefix path.
   *
   * @param prefixPath a prefix path or a full path, can not contain '*'
   * @param level the level can not be smaller than the level of the prefixPath
   */
  public int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    return mtree.getNodesCountInGivenLevel(prefixPath, level);
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
    String[] prefixNodes = plan.getPath().getNodes();
    int curOffset = -1;
    int count = 0;
    int limit = plan.getLimit();
    int offset = plan.getOffset();
    for (IMeasurementMNode leaf : allMatchedNodes) {
      if (match(leaf.getPartialPath(), prefixNodes)) {
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
                  getStorageGroupPath(leaf.getPartialPath()).getFullPath(),
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

  /** whether the full path has the prefixNodes */
  private boolean match(PartialPath fullPath, String[] prefixNodes) {
    String[] nodes = fullPath.getNodes();
    if (nodes.length < prefixNodes.length) {
      return false;
    }
    for (int i = 0; i < prefixNodes.length; i++) {
      if (!"*".equals(prefixNodes[i]) && !prefixNodes[i].equals(nodes[i])) {
        return false;
      }
    }
    return true;
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
   * get MeasurementSchema or VectorMeasurementSchema which contains the measurement
   *
   * @param device device path
   * @param measurement measurement name, could be vector name
   * @return MeasurementSchema or VectorMeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath device, String measurement)
      throws MetadataException {
    IMNode deviceIMNode = getDeviceNode(device);
    IMeasurementMNode measurementMNode = (IMeasurementMNode) deviceIMNode.getChild(measurement);
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
    IMeasurementMNode leaf = (IMeasurementMNode) mtree.getNodeByPath(fullPath);
    return getSeriesSchema(fullPath, leaf);
  }

  protected IMeasurementSchema getSeriesSchema(PartialPath fullPath, IMeasurementMNode leaf) {
    IMeasurementSchema schema = leaf.getSchema();

    if (schema == null || schema.getType() != TSDataType.VECTOR) {
      return schema;
    }
    List<String> measurementsInLeaf = schema.getValueMeasurementIdList();
    List<PartialPath> measurements = ((VectorPartialPath) fullPath).getSubSensorsPathList();
    TSDataType[] types = new TSDataType[measurements.size()];
    TSEncoding[] encodings = new TSEncoding[measurements.size()];

    for (int i = 0; i < measurements.size(); i++) {
      int index = measurementsInLeaf.indexOf(measurements.get(i).getMeasurement());
      types[i] = schema.getValueTSDataTypeList().get(index);
      encodings[i] = schema.getValueTSEncodingList().get(index);
    }
    String[] array = new String[measurements.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = measurements.get(i).getMeasurement();
    }
    return new VectorMeasurementSchema(
        schema.getMeasurementId(), array, types, encodings, schema.getCompressor());
  }

  /**
   * Transform the PartialPath to VectorPartialPath if it is a sub sensor of one vector. otherwise,
   * we don't change it.
   */
  public PartialPath transformPath(PartialPath partialPath) throws MetadataException {
    IMeasurementMNode node = (IMeasurementMNode) getNodeByPath(partialPath);
    if (node.getSchema() instanceof MeasurementSchema) {
      return partialPath;
    } else {
      return toVectorPath(partialPath);
    }
  }

  /** Convert the PartialPath to VectorPartialPath. */
  protected VectorPartialPath toVectorPath(PartialPath partialPath) throws MetadataException {
    List<PartialPath> subSensorsPathList = new ArrayList<>();
    subSensorsPathList.add(partialPath);
    return new VectorPartialPath(partialPath.getDevice(), subSensorsPathList);
  }

  /**
   * Get schema of partialPaths, in which aligned timeseries should only organized to one schema.
   * This method should be called when logical plan converts to physical plan.
   *
   * @param fullPaths full path list without pointing out which timeseries are aligned. For example,
   *     maybe (s1,s2) are aligned, but the input could be [root.sg1.d1.s1, root.sg1.d1.s2]
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     VectorMeasurementSchema (s1,s2) would be returned once; Size of integer list must equal to
   *     the input list size. It indicates the index of elements of original list in the result list
   */
  public Pair<List<PartialPath>, Map<String, Integer>> getSeriesSchemas(List<PartialPath> fullPaths)
      throws MetadataException {
    Map<IMNode, PartialPath> nodeToPartialPath = new LinkedHashMap<>();
    Map<IMNode, List<Integer>> nodeToIndex = new LinkedHashMap<>();
    for (int i = 0; i < fullPaths.size(); i++) {
      PartialPath path = fullPaths.get(i);
      // use dfs to collect paths
      IMeasurementMNode node = (IMeasurementMNode) getNodeByPath(path);
      getNodeToPartialPath(node, nodeToPartialPath, nodeToIndex, path, i);
    }
    return getPair(fullPaths, nodeToPartialPath, nodeToIndex);
  }

  protected void getNodeToPartialPath(
      IMeasurementMNode node,
      Map<IMNode, PartialPath> nodeToPartialPath,
      Map<IMNode, List<Integer>> nodeToIndex,
      PartialPath path,
      int index)
      throws MetadataException {
    if (!nodeToPartialPath.containsKey(node)) {
      if (node.getSchema() instanceof MeasurementSchema) {
        nodeToPartialPath.put(node, path);
      } else {
        List<PartialPath> subSensorsPathList = new ArrayList<>();
        subSensorsPathList.add(path);
        nodeToPartialPath.put(node, new VectorPartialPath(node.getFullPath(), subSensorsPathList));
      }
      nodeToIndex.computeIfAbsent(node, k -> new ArrayList<>()).add(index);
    } else {
      // if nodeToPartialPath contains node
      String existPath = nodeToPartialPath.get(node).getFullPath();
      if (existPath.equals(path.getFullPath())) {
        // could be the same path in different aggregate functions
        nodeToIndex.get(node).add(index);
      } else {
        // could be VectorPartialPath
        ((VectorPartialPath) nodeToPartialPath.get(node)).addSubSensor(path);
        nodeToIndex.get(node).add(index);
      }
    }
  }

  protected Pair<List<PartialPath>, Map<String, Integer>> getPair(
      List<PartialPath> fullPaths,
      Map<IMNode, PartialPath> nodeToPartialPath,
      Map<IMNode, List<Integer>> nodeToIndex)
      throws MetadataException {
    Map<String, Integer> indexMap = new HashMap<>();
    int i = 0;
    for (List<Integer> indexList : nodeToIndex.values()) {
      for (int index : indexList) {
        PartialPath partialPath = fullPaths.get(i);
        if (indexMap.containsKey(partialPath.getFullPath())) {
          throw new MetadataException(
              "Query for measurement and its alias at the same time!", true);
        }
        indexMap.put(partialPath.getFullPath(), index);
        if (partialPath.isMeasurementAliasExists()) {
          indexMap.put(partialPath.getFullPathWithAlias(), index);
        }
        i++;
      }
    }
    return new Pair<>(new ArrayList<>(nodeToPartialPath.values()), indexMap);
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param path The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    return mtree.getChildNodePathInNextLevel(path);
  }

  /**
   * Get child node in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2] given path = root.sg.d1 return [s1,s2]
   *
   * @return All child nodes of given seriesPath.
   */
  public Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException {
    return mtree.getChildNodeInNextLevel(path);
  }

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    return mtree.isPathExist(path);
  }

  /** Get node by path */
  public IMNode getNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getNodeByPath(path);
  }

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
  public IMNode getDeviceNodeWithAutoCreate(
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

  /** !!!!!!Attention!!!!! must call the return node's readUnlock() if you call this method. */
  public IMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws MetadataException, IOException {
    return getDeviceNodeWithAutoCreate(
        path, config.isAutoCreateSchemaEnabled(), true, config.getDefaultStorageGroupLevel());
  }

  // attention: this path must be a device node
  public List<IMeasurementSchema> getAllMeasurementByDevicePath(PartialPath path)
      throws PathNotExistException {
    Set<IMeasurementSchema> res = new HashSet<>();
    try {
      IMNode node = mNodeCache.get(path);
      Template template = node.getUpperTemplate();

      for (IMNode child : node.getChildren().values()) {
        IMeasurementMNode measurementMNode = (IMeasurementMNode) child;
        res.add(measurementMNode.getSchema());
      }

      // template
      if (node.isUseTemplate() && template != null) {
        res.addAll(template.getSchemaMap().values());
      }
    } catch (CacheException e) {
      throw new PathNotExistException(path.getFullPath());
    }

    return new ArrayList<>(res);
  }

  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    IMNode node;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (CacheException e) {
      throw new PathNotExistException(path.getFullPath());
    }
  }

  /**
   * To reduce the String number in memory, use the deviceId from MManager instead of the deviceId
   * read from disk
   *
   * @param path read from disk
   * @return deviceId
   */
  public String getDeviceId(PartialPath path) {
    String device = null;
    try {
      IMNode deviceNode = getDeviceNode(path);
      device = deviceNode.getFullPath();
    } catch (MetadataException | NullPointerException e) {
      // Cannot get deviceId from MManager, return the input deviceId
    }
    return device;
  }

  /** Get metadata in string */
  public String getMetadataInString() {
    return TIME_SERIES_TREE_HEADER + mtree;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
    if (!isRecovering) {
      logWriter.setTTL(storageGroup, dataTTL);
    }
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

  /**
   * Check whether the given path contains a storage group change or set the new offset of a
   * timeseries
   *
   * @param path timeseries
   * @param offset offset in the tag file
   */
  public void changeOffset(PartialPath path, long offset) throws MetadataException {
    ((IMeasurementMNode) mtree.getNodeByPath(path)).setOffset(offset);
  }

  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = (IMeasurementMNode) mtree.getNodeByPath(path);
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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
    IMNode node = mtree.getNodeByPath(fullPath);
    if (!(node.isMeasurement())) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    IMeasurementMNode leafMNode = (IMeasurementMNode) node;
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey),
          true);
    }
    // tags, attributes
    tagManager.renameTagOrAttributeKey(oldKey, newKey, fullPath, leafMNode);
  }

  /** Check whether the given path contains a storage group */
  boolean checkStorageGroupByPath(PartialPath path) {
    return mtree.checkStorageGroupByPath(path);
  }

  /**
   * Get all storage groups under the given path
   *
   * @return List of String represented all storage group names
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    try {
      return mtree.getStorageGroupByPath(path);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    }
  }

  public void collectTimeseriesSchema(
      IMNode startingNode, Collection<TimeseriesSchema> timeseriesSchemas) {
    Deque<IMNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      IMNode node = nodeDeque.removeFirst();
      if (node.isMeasurement()) {
        IMeasurementSchema nodeSchema = ((IMeasurementMNode) node).getSchema();
        timeseriesSchemas.add(
            new TimeseriesSchema(
                node.getFullPath(),
                nodeSchema.getType(),
                nodeSchema.getEncodingType(),
                nodeSchema.getCompressor()));
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  public void collectTimeseriesSchema(
      String prefixPath, Collection<TimeseriesSchema> timeseriesSchemas) throws MetadataException {
    collectTimeseriesSchema(getNodeByPath(new PartialPath(prefixPath)), timeseriesSchemas);
  }

  public void collectMeasurementSchema(
      IMNode startingNode, Collection<IMeasurementSchema> measurementSchemas) {
    Deque<IMNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      IMNode node = nodeDeque.removeFirst();
      if (node.isMeasurement()) {
        IMeasurementSchema nodeSchema = ((IMeasurementMNode) node).getSchema();
        measurementSchemas.add(nodeSchema);
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  /** Collect the timeseries schemas under "startingPath". */
  public void collectSeries(PartialPath startingPath, List<IMeasurementSchema> measurementSchemas) {
    IMNode node;
    try {
      node = getNodeByPath(startingPath);
    } catch (MetadataException e) {
      return;
    }
    collectMeasurementSchema(node, measurementSchemas);
  }

  /**
   * For a path, infer all storage groups it may belong to. The path can have wildcards.
   *
   * <p>Consider the path into two parts: (1) the sub path which can not contain a storage group
   * name and (2) the sub path which is substring that begin after the storage group name.
   *
   * <p>(1) Suppose the part of the path can not contain a storage group name (e.g.,
   * "root".contains("root.sg") == false), then: If the wildcard is not at the tail, then for each
   * wildcard, only one level will be inferred and the wildcard will be removed. If the wildcard is
   * at the tail, then the inference will go on until the storage groups are found and the wildcard
   * will be kept. (2) Suppose the part of the path is a substring that begin after the storage
   * group name. (e.g., For "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part
   * is "a.*.b.*"). For this part, keep what it is.
   *
   * <p>Assuming we have three SGs: root.group1, root.group2, root.area1.group3 Eg1: for input
   * "root.*", returns ("root.group1", "root.group1.*"), ("root.group2", "root.group2.*")
   * ("root.area1.group3", "root.area1.group3.*") Eg2: for input "root.*.s1", returns
   * ("root.group1", "root.group1.s1"), ("root.group2", "root.group2.s1")
   *
   * <p>Eg3: for input "root.area1.*", returns ("root.area1.group3", "root.area1.group3.*")
   *
   * @param path can be a prefix or a full path.
   * @return StorageGroupName-FullPath pairs
   */
  public Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException {
    Map<String, String> sgPathMap = mtree.determineStorageGroup(path);
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
   * Update the last cache value of time series of given seriesPath.
   *
   * <p>MManager will use the seriesPath to search the node first and then process the lastCache in
   * the MeasurementMNode
   *
   * <p>Invoking scenario: (1) after executing insertPlan (2) after reading last value from file
   * during last Query
   *
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
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
      node = (IMeasurementMNode) mtree.getNodeByPath(seriesPath);
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
    if (node.getSchema() instanceof VectorMeasurementSchema) {
      return;
    }
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
      IMeasurementMNode node,
      String subMeasurement,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {
    if (!(node.getSchema() instanceof VectorMeasurementSchema)) {
      return;
    }
    LastCacheManager.updateLastCache(
        node.getPartialPath().concatNode(subMeasurement),
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
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
   * @return the last cache value
   */
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = (IMeasurementMNode) mtree.getNodeByPath(seriesPath);
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
   * @param node the measurementMNode holding the lastCache, must be unary timeseries
   * @return the last cache value
   */
  public TimeValuePair getLastCache(IMeasurementMNode node) {
    if (node.getSchema() instanceof VectorMeasurementSchema) {
      return null;
    }
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
  public TimeValuePair getLastCache(IMeasurementMNode node, String subMeasurement) {
    if (!(node.getSchema() instanceof VectorMeasurementSchema)) {
      return null;
    }
    return LastCacheManager.getLastCache(node.getPartialPath().concatNode(subMeasurement), node);
  }

  /**
   * Reset the last cache value of time series of given seriesPath. MManager will use the seriesPath
   * to search the node.
   *
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
   */
  public void resetLastCache(PartialPath seriesPath) {
    IMeasurementMNode node;
    try {
      node = (IMeasurementMNode) mtree.getNodeByPath(seriesPath);
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
      LastCacheManager.deleteLastCacheByDevice((IEntityMNode) node);
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
          (IEntityMNode) node, originalPath, startTime, endTime);
    }
  }

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
        && ((IMeasurementMNode) deviceMNode).getSchema() instanceof VectorMeasurementSchema
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
        IMNode child = getMNode(deviceMNode, plan.isAligned() ? vectorId : measurement);
        if (child != null && child.isMeasurement()) {
          measurementMNode = (IMeasurementMNode) child;
        } else if (child != null && child.isStorageGroup()) {
          throw new PathAlreadyExistException(deviceId + PATH_SEPARATOR + measurement);
        } else if ((measurementMNode = findTemplate(deviceMNode, measurement, vectorId)) != null) {
          // empty
        } else {
          if (!config.isAutoCreateSchemaEnabled()) {
            throw new PathNotExistException(deviceId + PATH_SEPARATOR + measurement);
          } else {
            if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
              if (!plan.isAligned()) {
                internalCreateTimeseries(
                    prefixPath.concatNode(measurement), plan.getDataTypes()[i]);
                // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
                deviceMNode = mtree.getNodeByPath(deviceId);
                measurementMNode = (IMeasurementMNode) deviceMNode.getChild(measurement);
              } else {
                internalAlignedCreateTimeseries(
                    prefixPath, Arrays.asList(measurementList), Arrays.asList(plan.getDataTypes()));
                // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
                deviceMNode = mtree.getNodeByPath(deviceId);
                measurementMNode = (IMeasurementMNode) deviceMNode.getChild(vectorId);
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
                measurementMNode.getSchema().getValueTSDataTypeList().get(i);
            insertDataType = plan.getDataTypes()[i];
            if (insertDataType == null) {
              insertDataType = dataTypeInNode;
            }
            if (dataTypeInNode != insertDataType) {
              logger.warn(
                  "DataType mismatch, Insert measurement {} in {} type {}, metadata tree type {}",
                  measurementMNode.getSchema().getValueMeasurementIdList().get(i),
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

  public IMNode getMNode(IMNode deviceMNode, String measurementName) {
    return deviceMNode.getChild(measurementName);
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
        try {
          logWriter.setUsingSchemaTemplate(deviceMNode.getPartialPath());
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }

      if (schema != null) {
        if (schema instanceof MeasurementSchema) {
          return new MeasurementMNode(deviceMNode, measurement, schema, null);
        } else if (schema instanceof VectorMeasurementSchema) {
          return new MeasurementMNode(deviceMNode, vectorId, schema, null);
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

  /**
   * StorageGroupFilter filters unsatisfied storage groups in metadata queries to speed up and
   * deduplicate.
   */
  @FunctionalInterface
  public interface StorageGroupFilter {

    boolean satisfy(String storageGroup);
  }

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

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath(), config.getDefaultStorageGroupLevel());
  }

  private void setUsingSchemaTemplate(SetUsingSchemaTemplatePlan plan) throws MetadataException {
    try {
      setUsingSchemaTemplate(getDeviceNode(plan.getPrefixPath()));
    } catch (PathNotExistException e) {
      // the order of SetUsingSchemaTemplatePlan and AutoCreateDeviceMNodePlan cannot be guaranteed
      // when writing concurrently, so we need a auto-create mechanism here
      mtree.getDeviceNodeWithAutoCreating(
          plan.getPrefixPath(), config.getDefaultStorageGroupLevel());
      setUsingSchemaTemplate(getDeviceNode(plan.getPrefixPath()));
    }
  }

  IEntityMNode setUsingSchemaTemplate(IMNode node) {
    // this operation may change mtree structure and node type
    // invoke mnode.setUseTemplate is invalid
    IEntityMNode entityMNode = mtree.setToEntity(node);
    entityMNode.setUseTemplate(true);
    if (node != entityMNode) {
      mNodeCache.removeObject(entityMNode.getPartialPath());
    }
    return entityMNode;
  }

  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }

  @TestOnly
  public void flushAllMlogForTest() throws IOException {
    logWriter.close();
  }
}
