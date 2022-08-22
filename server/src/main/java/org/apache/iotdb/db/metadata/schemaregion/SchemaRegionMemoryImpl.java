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
package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEngine;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.SeriesOverflowException;
import org.apache.iotdb.db.exception.metadata.template.DifferentTemplateException;
import org.apache.iotdb.db.exception.metadata.template.NoTemplateOnMNodeException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSGMemoryImpl;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;
import org.apache.iotdb.db.metadata.rescon.SchemaStatisticsManager;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplateInClusterPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.sync.sender.manager.SchemaSyncManager;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_TEMPLATE;
import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info of one certain
 * schema region and persistent it into files. This class contains the interfaces to modify the
 * metadata in schema region for delta system. All the operations will be inserted into the logs
 * temporary in case the downtime of the delta system.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>Interfaces and Implementation for initialization、recover and clear
 *   <li>Interfaces and Implementation for schema region Info query and operation
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces for auto create device
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for metadata count
 *         <li>Interfaces for level Node info Query
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces and methods for MNode query
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces and Implementation for InsertPlan process
 *   <li>Interfaces and Implementation for Template operations
 *   <li>Interfaces for Trigger
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
public class SchemaRegionMemoryImpl implements ISchemaRegion {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionMemoryImpl.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean isRecovering = true;
  private volatile boolean initialized = false;

  private String storageGroupDirPath;
  private String schemaRegionDirPath;
  private String storageGroupFullPath;
  private SchemaRegionId schemaRegionId;

  private int templateId = NON_TEMPLATE;

  // the log file writer
  private boolean usingMLog = true;
  private MLogWriter logWriter;

  private SchemaStatisticsManager schemaStatisticsManager = SchemaStatisticsManager.getInstance();
  private MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private final IStorageGroupMNode storageGroupMNode;
  private MTreeBelowSGMemoryImpl mtree;
  // device -> DeviceMNode
  private LoadingCache<PartialPath, IMNode> mNodeCache;
  private TagManager tagManager;
  private SchemaSyncManager syncManager = SchemaSyncManager.getInstance();

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionMemoryImpl(
      PartialPath storageGroup, SchemaRegionId schemaRegionId, IStorageGroupMNode storageGroupMNode)
      throws MetadataException {

    storageGroupFullPath = storageGroup.getFullPath();
    this.schemaRegionId = schemaRegionId;

    storageGroupDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + schemaRegionId.getId();

    int cacheSize = config.getSchemaRegionDeviceNodeCacheSize();
    mNodeCache =
        Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .build(
                new com.github.benmanes.caffeine.cache.CacheLoader<PartialPath, IMNode>() {
                  @Override
                  public @Nullable IMNode load(@NonNull PartialPath partialPath)
                      throws MetadataException {
                    return mtree.getNodeByPath(partialPath);
                  }
                });

    this.storageGroupMNode = storageGroupMNode;

    // In ratis mode, no matter create schemaRegion or recover schemaRegion, the working dir should
    // be clear first
    if (config.isClusterMode()
        && config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RatisConsensus)) {
      File schemaRegionDir = new File(schemaRegionDirPath);
      if (schemaRegionDir.exists()) {
        FileUtils.deleteDirectory(schemaRegionDir);
      }
    }

    init();
  }

  @SuppressWarnings("squid:S2093")
  public synchronized void init() throws MetadataException {
    if (initialized) {
      return;
    }

    templateId = NON_TEMPLATE;

    initDir();

    try {
      // do not write log when recover
      isRecovering = true;

      tagManager = new TagManager(schemaRegionDirPath);
      mtree = new MTreeBelowSGMemoryImpl(storageGroupMNode, schemaRegionId.getId());

      if (!(config.isClusterMode()
          && config
              .getSchemaRegionConsensusProtocolClass()
              .equals(ConsensusFactory.RatisConsensus))) {
        usingMLog = true;
        initMLog();
      } else {
        usingMLog = false;
      }

      isRecovering = false;
    } catch (IOException e) {
      logger.error(
          "Cannot recover all schema info from {}, we try to recover as possible as we can",
          schemaRegionDirPath,
          e);
    }
    initialized = true;
  }

  private void initDir() throws SchemaDirCreationFailureException {
    File sgSchemaFolder = SystemFileFactory.INSTANCE.getFile(storageGroupDirPath);
    if (!sgSchemaFolder.exists()) {
      if (sgSchemaFolder.mkdirs()) {
        logger.info("create storage group schema folder {}", storageGroupDirPath);
      } else {
        if (!sgSchemaFolder.exists()) {
          logger.error("create storage group schema folder {} failed.", storageGroupDirPath);
          throw new SchemaDirCreationFailureException(storageGroupDirPath);
        }
      }
    }

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
  }

  private void initMLog() throws IOException {
    int lineNumber = initFromLog();

    logWriter = new MLogWriter(schemaRegionDirPath, MetadataConstant.METADATA_LOG);
    logWriter.setLogNum(lineNumber);
  }

  public void writeToMLog(PhysicalPlan plan) throws IOException {
    if (usingMLog && !isRecovering) {
      logWriter.putLog(plan);
    }
  }

  public void forceMlog() {
    if (!initialized) {
      return;
    }
    if (usingMLog) {
      try {
        logWriter.force();
      } catch (IOException e) {
        logger.error("Cannot force {} mlog to the schema region", schemaRegionId, e);
      }
    }
  }

  /**
   * Init from metadata log file.
   *
   * @return line number of the logFile
   */
  @SuppressWarnings("squid:S3776")
  private int initFromLog() throws IOException {
    File logFile =
        SystemFileFactory.INSTANCE.getFile(
            schemaRegionDirPath + File.separator + MetadataConstant.METADATA_LOG);

    long time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      int idx = 0;
      try (MLogReader mLogReader =
          new MLogReader(schemaRegionDirPath, MetadataConstant.METADATA_LOG); ) {
        idx = applyMLog(mLogReader);
        logger.debug(
            "spend {} ms to deserialize {} mtree from mlog.bin",
            System.currentTimeMillis() - time,
            storageGroupFullPath);
        return idx;
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException("Failed to parse " + storageGroupFullPath + " mlog.bin for err:" + e);
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

  /** function for clearing metadata components of one schema region */
  @Override
  public synchronized void clear() {
    try {

      templateId = NON_TEMPLATE;

      if (this.mNodeCache != null) {
        this.mNodeCache.invalidateAll();
      }
      if (this.mtree != null) {
        this.mtree.clear();
      }
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      tagManager.clear();

      isRecovering = true;
      initialized = false;
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
  }

  // this method is mainly used for recover
  private void operation(PhysicalPlan plan) throws IOException, MetadataException {
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
        deleteOneTimeseriesUpdateStatisticsAndDropTrigger(deleteTimeSeriesPlan.getPaths().get(0));
        break;
      case CHANGE_ALIAS:
        ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        break;
      case CHANGE_TAG_OFFSET:
        ChangeTagOffsetPlan changeTagOffsetPlan = (ChangeTagOffsetPlan) plan;
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
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
      case ACTIVATE_TEMPLATE_IN_CLUSTER:
        ActivateTemplateInClusterPlan activateTemplateInClusterPlan =
            (ActivateTemplateInClusterPlan) plan;
        recoverActivatingSchemaTemplate(activateTemplateInClusterPlan);
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces for schema region Info query and operation

  public String getStorageGroupFullPath() {
    return storageGroupFullPath;
  }

  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  public synchronized void deleteSchemaRegion() throws MetadataException {
    // collect all the LeafMNode in this schema region
    List<IMeasurementMNode> leafMNodes = mtree.getAllMeasurementMNode();

    schemaStatisticsManager.deleteTimeseries(leafMNodes.size());

    // drop triggers with no exceptions
    TriggerEngine.drop(leafMNodes);

    // clear all the components and release all the file handlers
    clear();

    // delete all the schema region files
    SchemaRegionUtils.deleteSchemaRegionFolder(schemaRegionDirPath, logger);
  }

  // currently, this method is only used for cluster-ratis mode
  @Override
  public synchronized boolean createSnapshot(File snapshotDir) {
    logger.info("Start create snapshot of schemaRegion {}", schemaRegionId);
    boolean isSuccess = true;
    long startTime = System.currentTimeMillis();

    long mtreeSnapshotStartTime = System.currentTimeMillis();
    isSuccess = isSuccess && mtree.createSnapshot(snapshotDir);
    logger.info(
        "MTree snapshot creation of schemaRegion {} costs {}ms.",
        schemaRegionId,
        System.currentTimeMillis() - mtreeSnapshotStartTime);

    long tagSnapshotStartTime = System.currentTimeMillis();
    isSuccess = isSuccess && tagManager.createSnapshot(snapshotDir);
    logger.info(
        "Tag snapshot creation of schemaRegion {} costs {}ms.",
        schemaRegionId,
        System.currentTimeMillis() - tagSnapshotStartTime);

    logger.info(
        "Snapshot creation of schemaRegion {} costs {}ms.",
        schemaRegionId,
        System.currentTimeMillis() - startTime);
    logger.info("Successfully create snapshot of schemaRegion {}", schemaRegionId);

    return isSuccess;
  }

  // currently, this method is only used for cluster-ratis mode
  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    clear();

    logger.info("Start loading snapshot of schemaRegion {}", schemaRegionId);
    long startTime = System.currentTimeMillis();

    try {
      usingMLog = false;

      isRecovering = true;

      long tagSnapshotStartTime = System.currentTimeMillis();
      tagManager = TagManager.loadFromSnapshot(latestSnapshotRootDir, schemaRegionDirPath);
      logger.info(
          "Tag snapshot loading of schemaRegion {} costs {}ms.",
          schemaRegionId,
          System.currentTimeMillis() - tagSnapshotStartTime);

      long mtreeSnapshotStartTime = System.currentTimeMillis();
      mtree =
          MTreeBelowSGMemoryImpl.loadFromSnapshot(
              latestSnapshotRootDir,
              storageGroupMNode,
              schemaRegionId.getId(),
              measurementMNode -> {
                if (measurementMNode.getOffset() == -1) {
                  return;
                }
                try {
                  tagManager.recoverIndex(measurementMNode.getOffset(), measurementMNode);
                } catch (IOException e) {
                  logger.error(
                      "Failed to recover tagIndex for {} in schemaRegion {}.",
                      storageGroupFullPath + PATH_SEPARATOR + measurementMNode.getFullPath(),
                      schemaRegionId);
                }
              });
      logger.info(
          "MTree snapshot loading of schemaRegion {} costs {}ms.",
          schemaRegionId,
          System.currentTimeMillis() - mtreeSnapshotStartTime);

      isRecovering = false;
      initialized = true;

      logger.info(
          "Snapshot loading of schemaRegion {} costs {}ms.",
          schemaRegionId,
          System.currentTimeMillis() - startTime);
      logger.info("Successfully load snapshot of schemaRegion {}", schemaRegionId);
    } catch (IOException e) {
      logger.error(
          "Failed to load snapshot for schemaRegion {}  due to {}. Use empty schemaRegion",
          schemaRegionId,
          e.getMessage(),
          e);
      try {
        initialized = false;
        isRecovering = true;
        init();
      } catch (MetadataException metadataException) {
        logger.error(
            "Error occurred during initializing schemaRegion {}",
            schemaRegionId,
            metadataException);
      }
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
    if (!memoryStatistics.isAllowToCreateNewSeries()) {
      throw new SeriesOverflowException();
    }

    try {
      PartialPath path = plan.getPath();
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

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

      // update statistics and schemaDataTypeNumMap
      schemaStatisticsManager.addTimeseries(1);

      // update tag index
      if (offset != -1 && isRecovering) {
        // the timeseries has already been created and now system is recovering, using the tag
        // info
        // in tagFile to recover index directly
        tagManager.recoverIndex(offset, leafMNode);
      } else if (plan.getTags() != null) {
        // tag key, tag value
        tagManager.addIndex(plan.getTags(), leafMNode);
      }

      // write log
      if (!isRecovering) {
        // either tags or attributes is not empty
        if ((plan.getTags() != null && !plan.getTags().isEmpty())
            || (plan.getAttributes() != null && !plan.getAttributes().isEmpty())) {
          offset = tagManager.writeTagFile(plan.getTags(), plan.getAttributes());
        }
        plan.setTagOffset(offset);
        writeToMLog(plan);
        if (syncManager.isEnableSync()) {
          syncManager.syncMetadataPlan(plan);
        }
      }
      if (offset != -1) {
        leafMNode.setOffset(offset);
      }

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
  private void createTimeseries(
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
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    if (!memoryStatistics.isAllowToCreateNewSeries()) {
      throw new SeriesOverflowException();
    }

    try {
      PartialPath prefixPath = plan.getPrefixPath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();
      List<Map<String, String>> tagsList = plan.getTagsList();
      List<Map<String, String>> attributesList = plan.getAttributesList();

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      // create time series in MTree
      List<IMeasurementMNode> measurementMNodeList =
          mtree.createAlignedTimeseries(
              prefixPath,
              measurements,
              plan.getDataTypes(),
              plan.getEncodings(),
              plan.getCompressors(),
              plan.getAliasList());

      // the cached mNode may be replaced by new entityMNode in mtree
      mNodeCache.invalidate(prefixPath);

      // update statistics and schemaDataTypeNumMap
      schemaStatisticsManager.addTimeseries(plan.getMeasurements().size());

      List<Long> tagOffsets = plan.getTagOffsets();
      for (int i = 0; i < measurements.size(); i++) {
        if (tagOffsets != null && !plan.getTagOffsets().isEmpty() && isRecovering) {
          if (tagOffsets.get(i) != -1) {
            tagManager.recoverIndex(plan.getTagOffsets().get(i), measurementMNodeList.get(i));
          }
        } else if (tagsList != null && !tagsList.isEmpty()) {
          if (tagsList.get(i) != null) {
            // tag key, tag value
            tagManager.addIndex(tagsList.get(i), measurementMNodeList.get(i));
          }
        }
      }

      // write log
      tagOffsets = new ArrayList<>();
      if (!isRecovering) {
        if ((tagsList != null && !tagsList.isEmpty())
            || (attributesList != null && !attributesList.isEmpty())) {
          Map<String, String> tags;
          Map<String, String> attributes;
          for (int i = 0; i < measurements.size(); i++) {
            tags = tagsList == null ? null : tagsList.get(i);
            attributes = attributesList == null ? null : attributesList.get(i);
            if (tags == null && attributes == null) {
              tagOffsets.add(-1L);
            } else {
              tagOffsets.add(tagManager.writeTagFile(tags, attributes));
            }
          }
        } else {
          for (int i = 0; i < measurements.size(); i++) {
            tagOffsets.add(-1L);
          }
        }
        plan.setTagOffsets(tagOffsets);
        writeToMLog(plan);
        if (syncManager.isEnableSync()) {
          syncManager.syncMetadataPlan(plan);
        }
      }
      tagOffsets = plan.getTagOffsets();
      for (int i = 0; i < measurements.size(); i++) {
        if (tagOffsets.get(i) != -1) {
          measurementMNodeList.get(i).setOffset(tagOffsets.get(i));
        }
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

  /**
   * Delete all timeseries matching the given path pattern. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * deleted.
   *
   * @param pathPattern path to be deleted
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return deletion failed Timeseries
   */
  public synchronized Pair<Integer, Set<String>> deleteTimeseries(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    try {
      List<MeasurementPath> allTimeseries = mtree.getMeasurementPaths(pathPattern, isPrefixMatch);

      Set<String> failedNames = new HashSet<>();
      int deletedNum = 0;
      for (PartialPath p : allTimeseries) {
        deleteSingleTimeseriesInternal(p, failedNames);
        deletedNum++;
      }
      return new Pair<>(deletedNum, failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Delete all timeseries matching the given path pattern
   *
   * @param pathPattern path to be deleted
   * @return deletion failed Timeseries
   */
  public Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern)
      throws MetadataException {
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
        }
        deleteTimeSeriesPlan.setDeletePathList(Collections.singletonList(p));
        writeToMLog(deleteTimeSeriesPlan);
        if (syncManager.isEnableSync()) {
          syncManager.syncMetadataPlan(deleteTimeSeriesPlan);
        }
      }
    } catch (DeleteFailedException e) {
      failedNames.add(e.getName());
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return After delete if the schema region is empty, return its path, otherwise return null
   */
  private PartialPath deleteOneTimeseriesUpdateStatisticsAndDropTrigger(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, IMeasurementMNode> pair =
        mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);

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

    mNodeCache.invalidate(node.getPartialPath());

    schemaStatisticsManager.deleteTimeseries(1);
    return storageGroupPath;
  }
  // endregion

  // region Interfaces for get and auto create device
  /**
   * get device node, if the schema region is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param path path
   */
  private IMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws IOException, MetadataException {
    IMNode node;
    try {
      return mNodeCache.get(path);
    } catch (Exception e) {
      if (!(e.getCause() instanceof MetadataException)) {
        throw e;
      }
    }

    node = mtree.getDeviceNodeWithAutoCreating(path);
    writeToMLog(new AutoCreateDeviceMNodePlan(node.getPartialPath()));
    return node;
  }

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath());
    try {
      writeToMLog(plan);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for metadata info Query
  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    try {
      return mtree.isPathExist(path);
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return false;
    }
  }

  // region Interfaces for metadata count

  /**
   * To calculate the count of timeseries matching given path. The path could be a pattern of a full
   * path, may contain wildcard. If using prefix match, the path pattern is used to match prefix
   * path. All timeseries start with the matched prefix path will be counted.
   */
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, isPrefixMatch);
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, String key, String value, boolean isContains)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(
        pathPattern,
        isPrefixMatch,
        tagManager.getMatchedTimeseriesInIndex(key, value, isContains),
        true);
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
   * To calculate the count of nodes in the given level for given path pattern. If using prefix
   * match, the path pattern is used to match prefix path. All nodes start with the matched prefix
   * path will be counted.
   *
   * @param pathPattern a path pattern or a full path
   * @param level the level should match the level of the path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
  }

  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    return mtree.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      String key,
      String value,
      boolean isContains)
      throws MetadataException {
    return mtree.getMeasurementCountGroupByLevel(
        pathPattern,
        level,
        isPrefixMatch,
        tagManager.getMatchedTimeseriesInIndex(key, value, isContains),
        true);
  }

  // endregion

  // region Interfaces for level Node info Query
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException {
    return mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch, filter);
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
  public Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
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
   * @return ShowDevicesResult and the current offset of this region after traverse.
   */
  public Pair<List<ShowDevicesResult>, Integer> getMatchedDevices(ShowDevicesPlan plan)
      throws MetadataException {
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

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap) throws MetadataException {
    return mtree.fetchSchema(pathPattern, templateMap);
  }

  public Pair<List<ShowTimeSeriesResult>, Integer> showTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    // show timeseries with index
    if (plan.getKey() != null && plan.getValue() != null) {
      return showTimeseriesWithIndex(plan, context);
    } else {
      return showTimeseriesWithoutIndex(plan, context);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Pair<List<ShowTimeSeriesResult>, Integer> showTimeseriesWithIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {

    List<IMeasurementMNode> allMatchedNodes = tagManager.getMatchedTimeseriesInIndex(plan, context);

    List<ShowTimeSeriesResult> res = new LinkedList<>();
    PartialPath pathPattern = plan.getPath();
    boolean needLast = plan.isOrderByHeat();
    int curOffset = -1;
    int count = 0;
    int limit = needLast ? 0 : plan.getLimit();
    int offset = needLast ? 0 : plan.getOffset();

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
                  storageGroupFullPath,
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

    if (needLast) {
      Stream<ShowTimeSeriesResult> stream = res.stream();

      limit = plan.getLimit();
      offset = plan.getOffset();

      stream =
          stream.sorted(
              Comparator.comparingLong(ShowTimeSeriesResult::getLastTime)
                  .reversed()
                  .thenComparing(ShowTimeSeriesResult::getName));

      if (limit != 0) {
        stream = stream.skip(offset).limit(limit);
      }

      res = stream.collect(toList());
    }

    return new Pair<>(res, curOffset + 1);
  }

  /**
   * Get the result of ShowTimeseriesPlan
   *
   * @param plan show time series query plan
   */
  private Pair<List<ShowTimeSeriesResult>, Integer> showTimeseriesWithoutIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    Pair<List<Pair<PartialPath, String[]>>, Integer> ans =
        mtree.getAllMeasurementSchema(plan, context);
    List<ShowTimeSeriesResult> res = new LinkedList<>();
    for (Pair<PartialPath, String[]> ansString : ans.left) {
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
    return new Pair<>(res, ans.right);
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
  // endregion
  // endregion

  // region Interfaces and methods for MNode query
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

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    return mtree.getMeasurementMNode(fullPath);
  }

  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws MetadataException {
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
   * Set the new offset of a timeseries. Only used for Recover. When creating tags/attributes for a
   * timeseries, if is first time, the file offset where the tags/attributes stored will be stored
   * in measurementMNode.
   *
   * @param path timeseries
   * @param offset offset in the tag file
   */
  private void changeOffset(PartialPath path, long offset) throws MetadataException {
    IMeasurementMNode measurementMNode = mtree.getMeasurementMNode(path);
    measurementMNode.setOffset(offset);

    if (isRecovering) {
      try {
        tagManager.recoverIndex(offset, measurementMNode);
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
  }

  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(path);
    if (leafMNode.getAlias() != null) {
      leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
    }
    leafMNode.getParent().addAlias(alias, leafMNode);
    mtree.setAlias(leafMNode, alias);

    try {
      writeToMLog(new ChangeAliasPlan(path, alias));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
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
      writeToMLog(new ChangeTagOffsetPlan(fullPath, offset));
      leafMNode.setOffset(offset);
      // update inverted Index map
      if (tagsMap != null && !tagsMap.isEmpty()) {
        tagManager.addIndex(tagsMap, leafMNode);
      }
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

      mtree.setAlias(leafMNode, alias);
      // persist to WAL
      writeToMLog(new ChangeAliasPlan(fullPath, alias));
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
      writeToMLog(new ChangeTagOffsetPlan(fullPath, offset));
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
      writeToMLog(new ChangeTagOffsetPlan(fullPath, offset));
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
    if (leafMNode.getOffset() != -1) {
      tagManager.dropTagsOrAttributes(keySet, fullPath, leafMNode);
    }
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

  // region Interfaces and Implementation for InsertPlan process
  /** get schema for device. Attention!!! Only support insertPlan */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    // devicePath is a logical path which is parent of measurement, whether in template or not
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();
    IMNode deviceMNode;

    // 1. get device node, set using template if accessed.
    deviceMNode = getDeviceInTemplateIfUsingTemplate(devicePath, measurementList);
    boolean isDeviceInTemplate = deviceMNode != null;

    // get logical device node, may be in template. will be multiple if overlap is allowed.
    if (!isDeviceInTemplate) {
      deviceMNode = getDeviceNodeWithAutoCreate(devicePath);
    }

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
            getMeasurementMNodeForInsertPlan(plan, i, deviceMNode, isDeviceInTemplate);
        deviceMNode = pair.left;
        measurementMNode = pair.right;

        // check type is match
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          try {
            SchemaRegionUtils.checkDataTypeMatch(plan, i, measurementMNode.getSchema().getType());
          } catch (DataTypeMismatchException mismatchException) {
            logger.warn(mismatchException.getMessage());
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
        if (config.isClusterMode()) {
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

  private IMNode getDeviceInTemplateIfUsingTemplate(
      PartialPath devicePath, String[] measurementList) throws MetadataException, IOException {
    // 1. get device node, set using template if accessed.
    IMNode deviceMNode = null;

    // check every measurement path
    int index = mtree.getMountedNodeIndexOnMeasurementPath(devicePath, measurementList);
    if (index == devicePath.getNodeLength()) {
      return null;
    }

    // this measurement is in template, need to assure mounted node exists and set using
    // template.
    // Without allowing overlap of template and MTree, this block run only once
    String[] mountedPathNodes = Arrays.copyOfRange(devicePath.getNodes(), 0, index + 1);
    IMNode mountedNode = getDeviceNodeWithAutoCreate(new PartialPath(mountedPathNodes));
    if (!mountedNode.isUseTemplate()) {
      mountedNode = setUsingSchemaTemplate(mountedNode);
    }
    if (index < devicePath.getNodeLength() - 1) {
      deviceMNode =
          mountedNode
              .getUpperTemplate()
              .getPathNodeInTemplate(
                  new PartialPath(
                      Arrays.copyOfRange(
                          devicePath.getNodes(), index + 1, devicePath.getNodeLength())));
    }

    return deviceMNode;
  }

  private Pair<IMNode, IMeasurementMNode> getMeasurementMNodeForInsertPlan(
      InsertPlan plan, int loc, IMNode deviceMNode, boolean isDeviceInTemplate)
      throws MetadataException {
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    String measurement = measurementList[loc];
    IMeasurementMNode measurementMNode = null;
    if (isDeviceInTemplate) {
      measurementMNode = deviceMNode.getChild(measurement).getAsMeasurementMNode();
    } else {
      measurementMNode = getMeasurementMNode(deviceMNode, measurement);
      if (measurementMNode == null) {
        measurementMNode = findMeasurementInTemplate(deviceMNode, measurement);
      }
    }
    if (measurementMNode == null) {
      if (!config.isAutoCreateSchemaEnabled() || isDeviceInTemplate) {
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
          measurementMNode = getMeasurementMNode(deviceMNode, measurement);
        } else {
          throw new MetadataException(
              String.format(
                  "Only support insertRow and insertTablet, plan is [%s]", plan.getOperatorType()));
        }
      }
    }
    return new Pair<>(deviceMNode, measurementMNode);
  }

  /** get dataType of plan, in loc measurements only support InsertRowPlan and InsertTabletPlan */
  private IMeasurementMNode findMeasurementInTemplate(IMNode deviceMNode, String measurement)
      throws MetadataException {
    Template curTemplate = deviceMNode.getUpperTemplate();

    if (curTemplate == null) {
      return null;
    }

    IMeasurementSchema schema = curTemplate.getSchema(measurement);

    if (schema == null) {
      return null;
    }

    if (!deviceMNode.isUseTemplate()) {
      deviceMNode = setUsingSchemaTemplate(deviceMNode);
    }

    return MeasurementMNode.getMeasurementMNode(
        deviceMNode.getAsEntityMNode(), measurement, schema, null);
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

  @Override
  public DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean aligned)
      throws MetadataException {
    try {
      List<MeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>(measurements.length);
      IMNode deviceMNode = getDeviceNodeWithAutoCreate(devicePath);
      IMeasurementMNode measurementMNode;
      for (int i = 0; i < measurements.length; i++) {
        measurementMNode = getMeasurementMNode(deviceMNode, measurements[i]);
        if (measurementMNode == null) {
          if (config.isAutoCreateSchemaEnabled()) {
            if (aligned) {
              internalAlignedCreateTimeseries(
                  devicePath,
                  Collections.singletonList(measurements[i]),
                  Collections.singletonList(getDataType.apply(i)));

            } else {
              internalCreateTimeseries(
                  devicePath.concatNode(measurements[i]), getDataType.apply(i));
            }
            // after creating timeseries, the deviceMNode has been replaced by a new entityMNode
            deviceMNode = mtree.getNodeByPath(devicePath);
            measurementMNode = getMeasurementMNode(deviceMNode, measurements[i]);
          } else {
            throw new PathNotExistException(devicePath + PATH_SEPARATOR + measurements[i]);
          }
        }
        measurementSchemaInfoList.add(
            new MeasurementSchemaInfo(
                measurementMNode.getName(),
                (MeasurementSchema) measurementMNode.getSchema(),
                measurementMNode.getAlias()));
      }

      return new DeviceSchemaInfo(
          devicePath, deviceMNode.getAsEntityMNode().isAligned(), measurementSchemaInfoList);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  /**
   * Get all paths set designated template
   *
   * @param templateName designated template name, blank string for any template exists
   * @return paths set
   */
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    return new HashSet<>(mtree.getPathsSetOnTemplate(templateName));
  }

  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    return new HashSet<>(mtree.getPathsUsingTemplate(templateName));
  }

  public boolean isTemplateAppendable(Template template, List<String> measurements)
      throws MetadataException {
    return mtree.isTemplateAppendable(template, measurements);
  }

  public synchronized void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    // get mnode and update template should be atomic
    Template template = TemplateManager.getInstance().getTemplate(plan.getTemplateName());

    try {
      PartialPath path = new PartialPath(plan.getPrefixPath());

      mtree.checkTemplateOnPath(path);

      IMNode node = getDeviceNodeWithAutoCreate(path);

      TemplateManager.getInstance().checkIsTemplateCompatible(template, node);
      mtree.checkIsTemplateCompatibleWithChild(node, template);
      node.setSchemaTemplate(template);

      TemplateManager.getInstance()
          .markSchemaRegion(template, storageGroupFullPath, schemaRegionId);

      // write wal
      writeToMLog(plan);
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
      Template template = node.getSchemaTemplate();
      node.setSchemaTemplate(null);
      TemplateManager.getInstance()
          .unmarkSchemaRegion(template, storageGroupFullPath, schemaRegionId);
      // write wal
      writeToMLog(plan);
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

    IMNode node;
    // the order of SetUsingSchemaTemplatePlan and AutoCreateDeviceMNodePlan cannot be guaranteed
    // when writing concurrently, so we need a auto-create mechanism here
    try {
      node = getDeviceNodeWithAutoCreate(plan.getPrefixPath());
    } catch (IOException ioException) {
      throw new MetadataException(ioException);
    }
    node = setUsingSchemaTemplate(node);
  }

  public IMNode setUsingSchemaTemplate(IMNode node) throws MetadataException {
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
    try {
      writeToMLog(new ActivateTemplatePlan(node.getPartialPath()));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
    return mountedMNode;
  }

  @Override
  public void activateSchemaTemplate(ActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {
    try {
      if (plan.getPathSetTemplate().getFullPath().length() <= storageGroupFullPath.length()) {
        templateId = plan.getTemplateId();
      }

      getDeviceNodeWithAutoCreate(plan.getActivatePath());

      mtree.activateTemplate(plan.getActivatePath(), plan.getTemplateSetLevel(), template);
      writeToMLog(plan);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new MetadataException(e);
    }
  }

  private void recoverActivatingSchemaTemplate(ActivateTemplateInClusterPlan plan) {
    if (plan.getPathSetTemplate().getFullPath().length() <= storageGroupFullPath.length()) {
      templateId = plan.getTemplateId();
    }
    mtree.activateTemplateWithoutCheck(
        plan.getActivatePath(), plan.getTemplateSetLevel(), plan.getTemplateId(), plan.isAligned());
  }

  @Override
  public List<String> getPathsUsingTemplate(int templateId) throws MetadataException {
    if (this.templateId != NON_TEMPLATE
        && templateId != ALL_TEMPLATE
        && this.templateId != templateId) {
      return Collections.emptyList();
    }
    return mtree.getPathsUsingTemplate(templateId);
  }

  // endregion

  // region Interfaces for Trigger

  public IMNode getMNodeForTrigger(PartialPath fullPath) throws MetadataException {
    return mtree.getNodeByPath(fullPath);
  }

  public void releaseMNodeAfterDropTrigger(IMNode node) throws MetadataException {
    // do nothing
  }

  // endregion
}
