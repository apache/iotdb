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

package org.apache.iotdb.db.schemaengine.schemaregion.impl;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.schemaengine.metric.ISchemaRegionMetric;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.rescon.DataNodeSchemaQuotaManager;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionParams;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.FakeCRC32Deserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.FakeCRC32Serializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.MLogDescriptionReader;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.MLogDescriptionWriter;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.SchemaLogReader;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.SchemaLogWriter;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.visitor.SchemaRegionPlanDeserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.visitor.SchemaRegionPlanSerializer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.MTreeBelowSGCachedImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.CacheMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowDevicesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowNodesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.tag.TagManager;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.filter.FilterContainsVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IChangeAliasPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IChangeTagOffsetPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IRollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IAlterLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces and Implementation for Template operations
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
@SchemaRegion(mode = "PBTree")
public class SchemaRegionPBTreeImpl implements ISchemaRegion {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionPBTreeImpl.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean isRecovering = true;
  private volatile boolean initialized = false;
  private boolean isClearing = false;

  private final String storageGroupDirPath;
  private final String schemaRegionDirPath;
  private final String storageGroupFullPath;
  private final SchemaRegionId schemaRegionId;

  // the log file writer
  private boolean usingMLog = true;

  private SchemaLogWriter<ISchemaRegionPlan> logWriter;
  private MLogDescriptionWriter logDescriptionWriter;

  private final CachedSchemaRegionStatistics regionStatistics;
  private final DataNodeSchemaQuotaManager schemaQuotaManager =
      DataNodeSchemaQuotaManager.getInstance();

  private MTreeBelowSGCachedImpl mtree;
  private TagManager tagManager;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionPBTreeImpl(ISchemaRegionParams schemaRegionParams) throws MetadataException {

    storageGroupFullPath = schemaRegionParams.getDatabase().getFullPath();
    this.schemaRegionId = schemaRegionParams.getSchemaRegionId();

    storageGroupDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + schemaRegionId.getId();
    this.regionStatistics =
        new CachedSchemaRegionStatistics(
            schemaRegionId.getId(), schemaRegionParams.getSchemaEngineStatistics());
    init();
  }

  @Override
  @SuppressWarnings("squid:S2093")
  public synchronized void init() throws MetadataException {
    if (initialized) {
      return;
    }

    initDir();

    try {
      // do not write log when recover
      isRecovering = true;

      tagManager = new TagManager(schemaRegionDirPath);
      mtree =
          new MTreeBelowSGCachedImpl(
              new PartialPath(storageGroupFullPath),
              tagManager::readTags,
              this::flushCallback,
              measurementInitProcess(),
              deviceInitProcess(),
              schemaRegionId.getId(),
              regionStatistics);

      if (!(config.isClusterMode()
          && config
              .getSchemaRegionConsensusProtocolClass()
              .equals(ConsensusFactory.RATIS_CONSENSUS))) {
        usingMLog = true;
        initMLog();
      } else {
        usingMLog = false;
      }

      isRecovering = false;
    } catch (IOException e) {
      logger.error(
          "Cannot recover all MTree from {} file, we try to recover as possible as we can",
          storageGroupFullPath,
          e);
    }
    initialized = true;
  }

  private Consumer<IMeasurementMNode<ICachedMNode>> measurementInitProcess() {
    return measurementMNode -> {
      regionStatistics.addTimeseries(1L);
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
    };
  }

  private Consumer<IDeviceMNode<ICachedMNode>> deviceInitProcess() {
    return deviceMNode -> {
      regionStatistics.addDevice();
      if (deviceMNode.getSchemaTemplateIdWithState() >= 0) {
        regionStatistics.activateTemplate(deviceMNode.getSchemaTemplateId());
      }
    };
  }

  private void flushCallback() {
    if (usingMLog && !isRecovering) {
      try {
        logDescriptionWriter.updateCheckPoint(logWriter.position());
        regionStatistics.setMlogCheckPoint(logWriter.position());
      } catch (IOException e) {
        logger.warn(
            "Update {} failed because {}",
            SchemaConstant.METADATA_LOG_DESCRIPTION,
            e.getMessage(),
            e);
      }
    }
  }

  private void initDir() throws SchemaDirCreationFailureException {
    File sgSchemaFolder = SystemFileFactory.INSTANCE.getFile(storageGroupDirPath);
    if (!sgSchemaFolder.exists()) {
      if (sgSchemaFolder.mkdirs()) {
        logger.info("create database schema folder {}", storageGroupDirPath);
      } else {
        if (!sgSchemaFolder.exists()) {
          logger.error("create database schema folder {} failed.", storageGroupDirPath);
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
    initFromLog();

    logWriter =
        new SchemaLogWriter<>(
            schemaRegionDirPath,
            SchemaConstant.METADATA_LOG,
            new FakeCRC32Serializer<>(new SchemaRegionPlanSerializer()),
            config.getSyncMlogPeriodInMs() == 0);
    logDescriptionWriter =
        new MLogDescriptionWriter(schemaRegionDirPath, SchemaConstant.METADATA_LOG_DESCRIPTION);
  }

  public void writeToMLog(ISchemaRegionPlan schemaRegionPlan) throws IOException {
    if (usingMLog && !isRecovering) {
      logWriter.write(schemaRegionPlan);
      regionStatistics.setMlogLength(logWriter.position());
    }
  }

  @Override
  public void forceMlog() {
    if (!initialized) {
      return;
    }
    if (usingMLog) {
      try {
        SchemaLogWriter<ISchemaRegionPlan> logWriter = this.logWriter;
        if (logWriter != null) {
          logWriter.force();
        }
      } catch (IOException e) {
        logger.error("Cannot force {} mlog to the schema region", schemaRegionId, e);
      }
    }
  }

  @Override
  public MemSchemaRegionStatistics getSchemaRegionStatistics() {
    return regionStatistics;
  }

  @Override
  public ISchemaRegionMetric createSchemaRegionMetric() {
    return new SchemaRegionCachedMetric(regionStatistics);
  }

  /** Init from metadata log file. */
  @SuppressWarnings("squid:S3776")
  private void initFromLog() throws IOException {
    File logFile =
        SystemFileFactory.INSTANCE.getFile(
            schemaRegionDirPath + File.separator + SchemaConstant.METADATA_LOG);
    File logDescriptionFile =
        SystemFileFactory.INSTANCE.getFile(
            schemaRegionDirPath + File.separator + SchemaConstant.METADATA_LOG_DESCRIPTION);

    long time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      long mLogOffset = 0;
      try {
        MLogDescriptionReader mLogDescriptionReader =
            new MLogDescriptionReader(schemaRegionDirPath, SchemaConstant.METADATA_LOG_DESCRIPTION);
        mLogOffset = mLogDescriptionReader.readCheckPoint();
        logger.info("MLog recovery check point: {}", mLogOffset);
      } catch (IOException e) {
        logger.warn(
            "Can not get check point in MLogDescription file because {}, use default value 0.",
            e.getMessage());
      }
      try (SchemaLogReader<ISchemaRegionPlan> mLogReader =
          new SchemaLogReader<>(
              schemaRegionDirPath,
              SchemaConstant.METADATA_LOG,
              new FakeCRC32Deserializer<>(new SchemaRegionPlanDeserializer()))) {
        applyMLog(mLogReader, mLogOffset);
        logger.debug(
            "spend {} ms to deserialize {} mtree from mlog.bin",
            System.currentTimeMillis() - time,
            storageGroupFullPath);
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException("Failed to parse " + storageGroupFullPath + " mlog.bin for err:" + e);
      }
    }
  }

  /**
   * Redo metadata log file.
   *
   * @param offset start position to redo MLog
   */
  private void applyMLog(SchemaLogReader<ISchemaRegionPlan> mLogReader, long offset) {
    ISchemaRegionPlan plan;
    RecoverPlanOperator recoverPlanOperator = new RecoverPlanOperator();
    RecoverOperationResult operationResult;
    try {
      mLogReader.skip(offset);
    } catch (IOException e) {
      e.printStackTrace();
    }
    while (mLogReader.hasNext()) {
      plan = mLogReader.next();
      operationResult = plan.accept(recoverPlanOperator, this);
      if (operationResult.isFailed()) {
        logger.error(
            "Can not operate cmd {} for err:",
            plan.getPlanType().name(),
            operationResult.getException());
      }
    }

    if (mLogReader.isFileCorrupted()) {
      throw new IllegalStateException(
          "The mlog.bin has been corrupted. Please remove it or fix it, and then restart IoTDB");
    }
  }

  /** function for clearing metadata components of one schema region */
  @Override
  public synchronized void clear() {
    isClearing = true;
    try {
      if (this.mtree != null) {
        this.mtree.clear();
      }
      this.regionStatistics.clear();
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      if (logDescriptionWriter != null) {
        logDescriptionWriter.close();
        logDescriptionWriter = null;
      }
      tagManager.clear();

      isRecovering = true;
      initialized = false;
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
    isClearing = false;
  }

  // endregion

  // region Interfaces for schema region Info query and operation

  @Override
  public String getDatabaseFullPath() {
    return storageGroupFullPath;
  }

  @Override
  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  @Override
  public synchronized void deleteSchemaRegion() throws MetadataException {
    // clear all the components and release all the file handlers
    clear();

    // delete all the schema region files
    SchemaRegionUtils.deleteSchemaRegionFolder(schemaRegionDirPath, logger);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    if (!initialized) {
      logger.warn(
          "Failed to create snapshot of schemaRegion {}, because the schemaRegion has not been initialized.",
          schemaRegionId);
      return false;
    }
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
          MTreeBelowSGCachedImpl.loadFromSnapshot(
              latestSnapshotRootDir,
              storageGroupFullPath,
              schemaRegionId.getId(),
              regionStatistics,
              measurementInitProcess(),
              deviceInitProcess(),
              tagManager::readTags,
              this::flushCallback);
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
    } catch (IOException | MetadataException e) {
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

  public void createTimeseries(ICreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  public void recoverTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    boolean done = false;
    while (!done) {
      try {
        createTimeseries(plan, offset);
        done = true;
      } catch (AliasAlreadyExistException | PathAlreadyExistException e) {
        // skip
        done = true;
      }
    }
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      CacheMemoryManager.getInstance().waitIfReleasing();
    }

    PartialPath path = plan.getPath();
    IMeasurementMNode<ICachedMNode> leafMNode;
    try {
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

      TSDataType type = plan.getDataType();
      // create time series in MTree
      leafMNode =
          mtree.createTimeseriesWithPinnedReturn(
              path,
              type,
              plan.getEncoding(),
              plan.getCompressor(),
              plan.getProps(),
              plan.getAlias());

      try {
        // update statistics and schemaDataTypeNumMap
        regionStatistics.addTimeseries(1L);

        // update tag index
        if (offset != -1 && isRecovering) {
          // the timeseries has already been created and now system is recovering, using the tag
          // info
          // in tagFile to recover index directly
          tagManager.recoverIndex(offset, leafMNode);
          mtree.pinMNode(leafMNode.getAsMNode());
        } else if (plan.getTags() != null) {
          // tag key, tag value
          tagManager.addIndex(plan.getTags(), leafMNode);
          mtree.pinMNode(leafMNode.getAsMNode());
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
        }
        if (offset != -1) {
          leafMNode.setOffset(offset);
          mtree.updateMNode(leafMNode.getAsMNode());
        }

      } finally {
        mtree.unPinMNode(leafMNode.getAsMNode());
      }

    } catch (IOException e) {
      throw new MetadataException(e);
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
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  public void recoverAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException {
    createAlignedTimeSeries(plan);
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  @Override
  public void createAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException {
    int seriesCount = plan.getMeasurements().size();
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      CacheMemoryManager.getInstance().waitIfReleasing();
    }

    try {
      PartialPath prefixPath = plan.getDevicePath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();
      List<Map<String, String>> tagsList = plan.getTagsList();
      List<Map<String, String>> attributesList = plan.getAttributesList();
      List<IMeasurementMNode<ICachedMNode>> measurementMNodeList;

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      // create time series in MTree
      measurementMNodeList =
          mtree.createAlignedTimeseries(
              prefixPath,
              measurements,
              plan.getDataTypes(),
              plan.getEncodings(),
              plan.getCompressors(),
              plan.getAliasList());

      try {

        // update statistics and schemaDataTypeNumMap
        regionStatistics.addTimeseries(seriesCount);

        List<Long> tagOffsets = plan.getTagOffsets();
        for (int i = 0; i < measurements.size(); i++) {
          if (tagOffsets != null && !plan.getTagOffsets().isEmpty() && isRecovering) {
            if (tagOffsets.get(i) != -1) {
              tagManager.recoverIndex(plan.getTagOffsets().get(i), measurementMNodeList.get(i));
              mtree.pinMNode(measurementMNodeList.get(i).getAsMNode());
            }
          } else if (tagsList != null && !tagsList.isEmpty()) {
            if (tagsList.get(i) != null) {
              // tag key, tag value
              tagManager.addIndex(tagsList.get(i), measurementMNodeList.get(i));
              mtree.pinMNode(measurementMNodeList.get(i).getAsMNode());
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
        }
        tagOffsets = plan.getTagOffsets();
        for (int i = 0; i < measurements.size(); i++) {
          if (tagOffsets.get(i) != -1) {
            measurementMNodeList.get(i).setOffset(tagOffsets.get(i));
            mtree.updateMNode(measurementMNodeList.get(i).getAsMNode());
          }
        }
      } finally {
        for (IMeasurementMNode<ICachedMNode> measurementMNode : measurementMNodeList) {
          mtree.unPinMNode(measurementMNode.getAsMNode());
        }
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    return mtree.checkMeasurementExistence(devicePath, measurementList, aliasList);
  }

  @Override
  public void checkSchemaQuota(PartialPath devicePath, int timeSeriesNum)
      throws SchemaQuotaExceededException {
    if (schemaQuotaManager.getLevel().equals(ClusterSchemaQuotaLevel.TIMESERIES)) {
      schemaQuotaManager.checkMeasurementLevel(timeSeriesNum);
    } else if (schemaQuotaManager.getLevel().equals(ClusterSchemaQuotaLevel.DEVICE)) {
      if (!mtree.checkDeviceNodeExists(devicePath)) {
        schemaQuotaManager.checkDeviceLevel();
      }
    }
  }

  @Override
  public long constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    long preDeletedNum = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      // Given pathPatterns may match one timeseries multi times, which may results in the
      // preDeletedNum larger than the actual num of timeseries. It doesn't matter since the main
      // purpose is to check whether there's timeseries to be deleted.
      List<PartialPath> paths = mtree.constructSchemaBlackList(pathPattern);
      preDeletedNum += paths.size();
      for (PartialPath path : paths) {
        try {
          writeToMLog(SchemaRegionWritePlanFactory.getPreDeleteTimeSeriesPlan(path));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
    return preDeletedNum;
  }

  private void recoverPreDeleteTimeseries(PartialPath path) throws MetadataException {
    mtree.constructSchemaBlackList(path);
  }

  @Override
  public void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      List<PartialPath> paths = mtree.rollbackSchemaBlackList(pathPattern);
      for (PartialPath path : paths) {
        try {
          writeToMLog(SchemaRegionWritePlanFactory.getRollbackPreDeleteTimeSeriesPlan(path));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public Set<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree)
      throws MetadataException {
    Set<PartialPath> deviceBasedPathPatternSet = new HashSet<>();
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      for (PartialPath devicePath : mtree.getDevicesOfPreDeletedTimeseries(pathPattern)) {
        deviceBasedPathPatternSet.addAll(pathPattern.alterPrefixPath(devicePath));
      }
    }
    return deviceBasedPathPatternSet;
  }

  @Override
  public void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException {
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      for (PartialPath path : mtree.getPreDeletedTimeseries(pathPattern)) {
        try {
          deleteSingleTimeseriesInBlackList(path);
          writeToMLog(
              SchemaRegionWritePlanFactory.getDeleteTimeSeriesPlan(
                  Collections.singletonList(path)));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public void createLogicalView(ICreateLogicalViewPlan createLogicalViewPlan)
      throws MetadataException {
    throw new UnsupportedOperationException("createLogicalView is unsupported.");
  }

  @Override
  public long constructLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteLogicalView(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterLogicalView(IAlterLogicalViewPlan alterLogicalViewPlan)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  private void deleteSingleTimeseriesInBlackList(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);

    regionStatistics.deleteTimeseries(1L);
  }

  private void recoverRollbackPreDeleteTimeseries(PartialPath path) throws MetadataException {
    mtree.rollbackSchemaBlackList(path);
  }

  /** @param path full path from root to leaf node */
  private void deleteOneTimeseriesUpdateStatistics(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);

    regionStatistics.deleteTimeseries(1L);
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
  private ICachedMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws IOException, MetadataException {
    ICachedMNode node = mtree.getDeviceNodeWithAutoCreating(path);
    writeToMLog(SchemaRegionWritePlanFactory.getAutoCreateDeviceMNodePlan(node.getPartialPath()));
    return node;
  }

  private void autoCreateDeviceMNode(IAutoCreateDeviceMNodePlan plan) throws MetadataException {
    ICachedMNode node = mtree.getDeviceNodeWithAutoCreating(plan.getPath());
    mtree.unPinMNode(node);
    try {
      writeToMLog(plan);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for metadata info Query

  // region Interfaces for timeseries, measurement and schema info Query

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    return mtree.fetchSchema(pathPattern, templateMap, withTags);
  }

  // endregion
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
    IMeasurementMNode<ICachedMNode> measurementMNode = mtree.getMeasurementMNode(path);
    try {
      measurementMNode.setOffset(offset);
      mtree.updateMNode(measurementMNode.getAsMNode());

      if (isRecovering) {
        try {
          if (tagManager.recoverIndex(offset, measurementMNode)) {
            mtree.pinMNode(measurementMNode.getAsMNode());
          }
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(measurementMNode.getAsMNode());
    }
  }

  /**
   * Upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upsertAliasAndTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    // upsert alias
    upsertAlias(alias, fullPath);
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      if (tagsMap == null && attributesMap == null) {
        return;
      }
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(tagsMap, attributesMap);
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        leafMNode.setOffset(offset);
        mtree.updateMNode(leafMNode.getAsMNode());
        // update inverted Index map
        if (tagsMap != null && !tagsMap.isEmpty()) {
          tagManager.addIndex(tagsMap, leafMNode);
          mtree.pinMNode(leafMNode.getAsMNode());
        }
        return;
      }

      tagManager.updateTagsAndAttributes(tagsMap, attributesMap, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  private void upsertAlias(String alias, PartialPath fullPath)
      throws MetadataException, IOException {
    if (mtree.changeAlias(alias, fullPath)) {
      // persist to WAL
      writeToMLog(SchemaRegionWritePlanFactory.getChangeAliasPlan(fullPath, alias));
    }
  }

  /**
   * Add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or attributes already exist
   */
  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        leafMNode.setOffset(offset);
        mtree.updateMNode(leafMNode.getAsMNode());
        return;
      }

      tagManager.addAttributes(attributesMap, fullPath, leafMNode);
    } finally {
      mtree.updateMNode(leafMNode.getAsMNode());
    }
  }

  /**
   * Add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags already exist
   */
  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        leafMNode.setOffset(offset);
        mtree.updateMNode(leafMNode.getAsMNode());
        // update inverted Index map
        tagManager.addIndex(tagsMap, leafMNode);
        mtree.pinMNode(leafMNode.getAsMNode());
        return;
      }

      tagManager.addTags(tagsMap, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  /**
   * Drop tags or attributes of the timeseries. It will not throw exception even if the key does not
   * exist.
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // no tag or attribute, just do nothing.
      if (leafMNode.getOffset() != -1) {
        tagManager.dropTagsOrAttributes(keySet, fullPath, leafMNode);
        // when the measurementMNode was added to tagIndex, it was pinned
        mtree.unPinMNode(leafMNode.getAsMNode());
      }
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  /**
   * Set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags/attributes do not exist
   */
  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
      }

      // tags, attributes
      tagManager.setTagsOrAttributesValue(alterMap, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  /**
   * Rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or does not have tag/attribute or already has
   *     a tag/attribute named newKey
   */
  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey),
            true);
      }
      // tags, attributes
      tagManager.renameTagOrAttributeKey(oldKey, newKey, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  /** remove the node from the tag inverted index */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(IMeasurementMNode<ICachedMNode> node) throws IOException {
    tagManager.removeFromTagInvertedIndex(node);
  }
  // endregion

  // region Interfaces and Implementation for Template operations
  @Override
  public void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      CacheMemoryManager.getInstance().waitIfReleasing();
    }
    try {
      ICachedMNode deviceNode = getDeviceNodeWithAutoCreate(plan.getActivatePath());
      try {
        mtree.activateTemplate(plan.getActivatePath(), template);
        writeToMLog(plan);
      } finally {
        mtree.unPinMNode(deviceNode);
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new MetadataException(e);
    }
  }

  private void recoverActivatingSchemaTemplate(IActivateTemplateInClusterPlan plan)
      throws MetadataException {
    mtree.activateTemplateWithoutCheck(
        plan.getActivatePath(), plan.getTemplateId(), plan.isAligned());
  }

  @Override
  public long constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo =
        mtree.constructSchemaBlackListWithTemplate(plan.getTemplateSetInfo());
    try {
      writeToMLog(SchemaRegionWritePlanFactory.getPreDeactivateTemplatePlan(resultTemplateSetInfo));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
    return resultTemplateSetInfo.size();
  }

  @Override
  public void rollbackSchemaBlackListWithTemplate(IRollbackPreDeactivateTemplatePlan plan)
      throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo =
        mtree.rollbackSchemaBlackListWithTemplate(plan.getTemplateSetInfo());
    try {
      writeToMLog(
          SchemaRegionWritePlanFactory.getRollbackPreDeactivateTemplatePlan(resultTemplateSetInfo));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo =
        mtree.deactivateTemplateInBlackList(plan.getTemplateSetInfo());
    try {
      writeToMLog(SchemaRegionWritePlanFactory.getDeactivateTemplatePlan(resultTemplateSetInfo));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public long countPathsUsingTemplate(int templateId, PathPatternTree patternTree)
      throws MetadataException {
    long result = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      result += mtree.countPathsUsingTemplate(pathPattern, templateId);
    }
    return result;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    return mtree.getDeviceReader(showDevicesPlan);
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      IShowTimeSeriesPlan showTimeSeriesPlan) throws MetadataException {
    if (showTimeSeriesPlan.getSchemaFilter() != null
        && new FilterContainsVisitor()
            .process(showTimeSeriesPlan.getSchemaFilter(), SchemaFilterType.TAGS_FILTER)) {
      return tagManager.getTimeSeriesReaderWithIndex(showTimeSeriesPlan);
    } else {
      return mtree.getTimeSeriesReader(
          showTimeSeriesPlan,
          offset -> {
            try {
              return tagManager.readTagFile(offset);
            } catch (IOException e) {
              logger.error("Failed to read tag and attribute info because {}", e.getMessage(), e);
              return new Pair<>(Collections.emptyMap(), Collections.emptyMap());
            }
          });
    }
  }

  @Override
  public ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException {
    return mtree.getNodeReader(showNodesPlan);
  }
  // endregion

  private static class RecoverOperationResult {

    private static final RecoverOperationResult SUCCESS = new RecoverOperationResult(null);

    private final Exception e;

    private RecoverOperationResult(Exception e) {
      this.e = e;
    }

    private boolean isFailed() {
      return e != null;
    }

    private Exception getException() {
      return e;
    }
  }

  private class RecoverPlanOperator
      extends SchemaRegionPlanVisitor<RecoverOperationResult, SchemaRegionPBTreeImpl> {

    @Override
    public RecoverOperationResult visitSchemaRegionPlan(
        ISchemaRegionPlan plan, SchemaRegionPBTreeImpl context) {
      throw new UnsupportedOperationException(
          String.format(
              "SchemaRegionPlan of type %s doesn't support recover operation in SchemaRegionPBTreeImpl.",
              plan.getPlanType().name()));
    }

    @Override
    public RecoverOperationResult visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, SchemaRegionPBTreeImpl context) {
      try {
        recoverTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, SchemaRegionPBTreeImpl context) {
      try {
        recoverAlignedTimeSeries(createAlignedTimeSeriesPlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDeleteTimeSeries(
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, SchemaRegionPBTreeImpl context) {
      try {
        // since we only has one path for one DeleteTimeSeriesPlan
        deleteOneTimeseriesUpdateStatistics(deleteTimeSeriesPlan.getDeletePathList().get(0));
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException | IOException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitChangeAlias(
        IChangeAliasPlan changeAliasPlan, SchemaRegionPBTreeImpl context) {
      try {
        upsertAlias(changeAliasPlan.getAlias(), changeAliasPlan.getPath());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException | IOException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, SchemaRegionPBTreeImpl context) {
      try {
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitAutoCreateDeviceMNode(
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, SchemaRegionPBTreeImpl context) {
      try {
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitActivateTemplateInCluster(
        IActivateTemplateInClusterPlan activateTemplateInClusterPlan,
        SchemaRegionPBTreeImpl context) {
      try {
        recoverActivatingSchemaTemplate(activateTemplateInClusterPlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitPreDeleteTimeSeries(
        IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, SchemaRegionPBTreeImpl context) {
      try {
        recoverPreDeleteTimeseries(preDeleteTimeSeriesPlan.getPath());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitRollbackPreDeleteTimeSeries(
        IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan,
        SchemaRegionPBTreeImpl context) {
      try {
        recoverRollbackPreDeleteTimeseries(rollbackPreDeleteTimeSeriesPlan.getPath());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitPreDeactivateTemplate(
        IPreDeactivateTemplatePlan preDeactivateTemplatePlan, SchemaRegionPBTreeImpl context) {
      try {
        constructSchemaBlackListWithTemplate(preDeactivateTemplatePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitRollbackPreDeactivateTemplate(
        IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan,
        SchemaRegionPBTreeImpl context) {
      try {
        rollbackSchemaBlackListWithTemplate(rollbackPreDeactivateTemplatePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDeactivateTemplate(
        IDeactivateTemplatePlan deactivateTemplatePlan, SchemaRegionPBTreeImpl context) {
      try {
        deactivateTemplateInBlackList(deactivateTemplatePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }
  }
}
