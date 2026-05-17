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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterEncodingCompressorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAliasSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DropAliasSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.LockAliasNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MarkSeriesEnabledNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MarkSeriesInvalidNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.UnlockForAliasNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.UpdatePhysicalAliasRefNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.ConstructTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDevicesInBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.RollbackTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableAttributeColumnDropNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeCommitUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableNodeLocationAddNode;
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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
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
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateAlignedTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IAlterLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.resp.ConstructSchemaBlackListResult;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TTimeSeriesInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

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

  private static final String MSG_CANNOT_ALTER_INVALID_TIME_SERIES =
      "Cannot alter invalid time series [%s]. Use the alias series instead.";
  private static final String MSG_CANNOT_ALTER_RENAMING_TIME_SERIES =
      "Cannot alter time series [%s] that is currently being renamed.";

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
  private final SchemaRegionCachedMetric metric;
  private final DataNodeSchemaQuotaManager schemaQuotaManager =
      DataNodeSchemaQuotaManager.getInstance();

  private MTreeBelowSGCachedImpl mtree;
  private TagManager tagManager;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionPBTreeImpl(ISchemaRegionParams schemaRegionParams) throws MetadataException {

    storageGroupFullPath = schemaRegionParams.getDatabase();
    this.schemaRegionId = schemaRegionParams.getSchemaRegionId();

    storageGroupDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + schemaRegionId.getId();
    this.regionStatistics =
        new CachedSchemaRegionStatistics(
            schemaRegionId.getId(), schemaRegionParams.getSchemaEngineStatistics());
    this.metric = new SchemaRegionCachedMetric(regionStatistics, schemaRegionParams.getDatabase());
    init();
  }

  @Override
  @SuppressWarnings("squid:S2093")
  public synchronized void init() throws MetadataException {
    if (initialized) {
      return;
    }

    if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      long memCost = config.getSchemaRatisConsensusLogAppenderBufferSizeMax();
      if (!SystemInfo.getInstance().addDirectBufferMemoryCost(memCost)) {
        throw new MetadataException(
            DataNodeSchemaMessages.DIRECT_BUFFER_MEMORY_EXCEEDED
                + (SystemInfo.getInstance().getDirectBufferMemoryCost() + memCost)
                + DataNodeSchemaMessages.DIRECT_BUFFER_MEMORY_LIMIT
                + SystemInfo.getInstance().getTotalDirectBufferMemorySizeLimit());
      }
    }

    initDir();

    try {
      // do not write log when recover
      isRecovering = true;

      tagManager = new TagManager(schemaRegionDirPath, regionStatistics);
      mtree =
          new MTreeBelowSGCachedImpl(
              new PartialPath(storageGroupFullPath),
              tagManager::readTags,
              tagManager::readAttributes,
              this::flushCallback,
              measurementInitProcess(),
              deviceInitProcess(),
              schemaRegionId.getId(),
              regionStatistics,
              metric);

      if (!config
          .getSchemaRegionConsensusProtocolClass()
          .equals(ConsensusFactory.RATIS_CONSENSUS)) {
        usingMLog = true;
        initMLog();
      } else {
        usingMLog = false;
      }

      isRecovering = false;
    } catch (IOException e) {
      logger.error(DataNodeSchemaMessages.CANNOT_RECOVER_ALL_MTREE, storageGroupFullPath, e);
    }
    initialized = true;
  }

  private Consumer<IMeasurementMNode<ICachedMNode>> measurementInitProcess() {
    return measurementMNode -> {
      if (measurementMNode.isLogicalView()) {
        regionStatistics.addView(1L);
      } else {
        // Count invalid series during recovery
        if (measurementMNode.isInvalid()) {
          regionStatistics.addInvalidSeries(1L);
        } else {
          regionStatistics.addMeasurement(1L);
        }
      }
      if (measurementMNode.getOffset() == -1) {
        return;
      }
      try {
        tagManager.recoverIndex(measurementMNode.getOffset(), measurementMNode);
      } catch (IOException e) {
        logger.error(
            DataNodeSchemaMessages.FAILED_TO_RECOVER_TAG_INDEX,
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
            DataNodeSchemaMessages.UPDATE_MLOG_DESCRIPTION_FAILED,
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
        logger.info(DataNodeSchemaMessages.CREATE_DATABASE_SCHEMA_FOLDER, storageGroupDirPath);
      } else {
        if (!sgSchemaFolder.exists()) {
          logger.error(
              DataNodeSchemaMessages.CREATE_DATABASE_SCHEMA_FOLDER_FAILED, storageGroupDirPath);
          throw new SchemaDirCreationFailureException(storageGroupDirPath);
        }
      }
    }

    File schemaRegionFolder = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    if (!schemaRegionFolder.exists()) {
      if (schemaRegionFolder.mkdirs()) {
        logger.info(DataNodeSchemaMessages.CREATE_SCHEMA_REGION_FOLDER, schemaRegionDirPath);
      } else {
        if (!schemaRegionFolder.exists()) {
          logger.error(
              DataNodeSchemaMessages.CREATE_SCHEMA_REGION_FOLDER_FAILED, schemaRegionDirPath);
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
      regionStatistics.setMLogLength(logWriter.position());
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
        logger.error(DataNodeSchemaMessages.CANNOT_FORCE_MLOG, schemaRegionId, e);
      }
    }
  }

  @Override
  public MemSchemaRegionStatistics getSchemaRegionStatistics() {
    return regionStatistics;
  }

  @Override
  public ISchemaRegionMetric getSchemaRegionMetric() {
    return metric;
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
    // Init the metadata from the operation log
    if (logFile.exists()) {
      long mLogOffset = 0;
      try {
        MLogDescriptionReader mLogDescriptionReader =
            new MLogDescriptionReader(schemaRegionDirPath, SchemaConstant.METADATA_LOG_DESCRIPTION);
        mLogOffset = mLogDescriptionReader.readCheckPoint();
        logger.info(DataNodeSchemaMessages.MLOG_RECOVERY_CHECK_POINT, mLogOffset);
      } catch (IOException e) {
        logger.warn(DataNodeSchemaMessages.CANNOT_GET_MLOG_CHECKPOINT, e.getMessage());
      }
      try (SchemaLogReader<ISchemaRegionPlan> mLogReader =
          new SchemaLogReader<>(
              schemaRegionDirPath,
              SchemaConstant.METADATA_LOG,
              new FakeCRC32Deserializer<>(new SchemaRegionPlanDeserializer()))) {
        applyMLog(mLogReader, mLogOffset);
        logger.debug(
            DataNodeSchemaMessages.SPEND_TIME_DESERIALIZE_MTREE,
            System.currentTimeMillis() - time,
            storageGroupFullPath);
      } catch (Exception e) {
        throw new IOException(
            DataNodeSchemaMessages.FAILED_TO_PARSE_MLOG
                + storageGroupFullPath
                + DataNodeSchemaMessages.MLOG_BIN_SUFFIX,
            e);
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
      logger.error(DataNodeSchemaMessages.FAILED_TO_SKIP_MLOG, offset, schemaRegionDirPath, e);
    }
    while (mLogReader.hasNext()) {
      plan = mLogReader.next();
      operationResult = plan.accept(recoverPlanOperator, this);
      if (operationResult.isFailed()) {
        logger.error(
            DataNodeSchemaMessages.CANNOT_OPERATE_CMD,
            plan.getPlanType().name(),
            operationResult.getException());
      }
    }

    if (mLogReader.isFileCorrupted()) {
      throw new IllegalStateException(DataNodeSchemaMessages.MLOG_BIN_CORRUPTED);
    }
  }

  /** Function for clearing metadata components of one schema region */
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
      logger.error(DataNodeSchemaMessages.CANNOT_CLOSE_METADATA_LOG_WRITER, e);
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

    if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      SystemInfo.getInstance()
          .decreaseDirectBufferMemoryCost(config.getSchemaRatisConsensusLogAppenderBufferSizeMax());
    }
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    if (!initialized) {
      logger.warn(DataNodeSchemaMessages.FAILED_TO_CREATE_SNAPSHOT_NOT_INITIALIZED, schemaRegionId);
      return false;
    }
    logger.info(DataNodeSchemaMessages.START_CREATE_SNAPSHOT, schemaRegionId);
    boolean isSuccess = true;
    long startTime = System.currentTimeMillis();

    long mtreeSnapshotStartTime = System.currentTimeMillis();
    isSuccess = isSuccess && mtree.createSnapshot(snapshotDir);
    logger.info(
        DataNodeSchemaMessages.MTREE_SNAPSHOT_CREATION_COST,
        schemaRegionId,
        System.currentTimeMillis() - mtreeSnapshotStartTime);

    long tagSnapshotStartTime = System.currentTimeMillis();
    isSuccess = isSuccess && tagManager.createSnapshot(snapshotDir);
    logger.info(
        DataNodeSchemaMessages.TAG_SNAPSHOT_CREATION_COST,
        schemaRegionId,
        System.currentTimeMillis() - tagSnapshotStartTime);

    logger.info(
        DataNodeSchemaMessages.SNAPSHOT_CREATION_COST,
        schemaRegionId,
        System.currentTimeMillis() - startTime);
    logger.info(DataNodeSchemaMessages.SUCCESSFULLY_CREATE_SNAPSHOT, schemaRegionId);

    return isSuccess;
  }

  // currently, this method is only used for cluster-ratis mode
  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    clear();

    logger.info(DataNodeSchemaMessages.START_LOADING_SNAPSHOT, schemaRegionId);
    long startTime = System.currentTimeMillis();

    try {
      usingMLog = false;

      isRecovering = true;

      long tagSnapshotStartTime = System.currentTimeMillis();
      tagManager =
          TagManager.loadFromSnapshot(latestSnapshotRootDir, schemaRegionDirPath, regionStatistics);
      logger.info(
          DataNodeSchemaMessages.TAG_SNAPSHOT_LOADING_COST,
          schemaRegionId,
          System.currentTimeMillis() - tagSnapshotStartTime);

      long mtreeSnapshotStartTime = System.currentTimeMillis();
      mtree =
          MTreeBelowSGCachedImpl.loadFromSnapshot(
              latestSnapshotRootDir,
              storageGroupFullPath,
              schemaRegionId.getId(),
              regionStatistics,
              metric,
              measurementInitProcess(),
              deviceInitProcess(),
              tagManager::readTags,
              tagManager::readAttributes,
              this::flushCallback);
      logger.info(
          DataNodeSchemaMessages.MTREE_SNAPSHOT_LOADING_COST,
          schemaRegionId,
          System.currentTimeMillis() - mtreeSnapshotStartTime);

      isRecovering = false;
      initialized = true;

      logger.info(
          DataNodeSchemaMessages.SNAPSHOT_LOADING_COST,
          schemaRegionId,
          System.currentTimeMillis() - startTime);
      logger.info(DataNodeSchemaMessages.SUCCESSFULLY_LOAD_SNAPSHOT, schemaRegionId);
    } catch (IOException | MetadataException e) {
      logger.error(
          DataNodeSchemaMessages.FAILED_TO_LOAD_SNAPSHOT, schemaRegionId, e.getMessage(), e);
      try {
        initialized = false;
        isRecovering = true;
        init();
      } catch (MetadataException metadataException) {
        logger.error(
            DataNodeSchemaMessages.ERROR_DURING_INIT_SCHEMA_REGION,
            schemaRegionId,
            metadataException);
      }
    }
  }

  // endregion

  // region Interfaces and Implementation for Time series operation
  // including create and delete

  public void createTimeSeries(ICreateTimeSeriesPlan plan) throws MetadataException {
    createTimeSeries(plan, -1);
  }

  public void recoverTimeSeries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    boolean done = false;
    while (!done) {
      try {
        createTimeSeries(plan, offset);
        done = true;
      } catch (AliasAlreadyExistException | PathAlreadyExistException e) {
        // skip
        done = true;
      }
    }
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeSeries(final ICreateTimeSeriesPlan plan, long offset)
      throws MetadataException {
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }

    final PartialPath path = plan.getPath();
    final IMeasurementMNode<ICachedMNode> leafMNode;
    try {
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

      final TSDataType type = plan.getDataType();
      // Create time series in MTree
      leafMNode =
          mtree.createTimeSeriesWithPinnedReturn(
              path,
              type,
              plan.getEncoding(),
              plan.getCompressor(),
              plan.getProps(),
              plan.getAlias(),
              (plan instanceof CreateTimeSeriesPlanImpl
                  && ((CreateTimeSeriesPlanImpl) plan).isWithMerge()),
              plan instanceof CreateTimeSeriesPlanImpl
                  ? ((CreateTimeSeriesPlanImpl) plan).getAligned()
                  : null);

      try {
        // Should merge
        if (Objects.isNull(leafMNode)) {
          // Write an upsert plan directly
          // Note that the "pin" and "unpin" is reentrant
          upsertAliasAndTagsAndAttributes(
              plan.getAlias(), plan.getTags(), plan.getAttributes(), path);
          return;
        }

        // Update statistics and schemaDataTypeNumMap
        regionStatistics.addMeasurement(1L);

        // Update tag index
        if (offset != -1 && isRecovering) {
          // The time series has already been created and now system is recovering, using the tag
          // info in tagFile to recover index directly
          tagManager.recoverIndex(offset, leafMNode);
          mtree.pinMNode(leafMNode.getAsMNode());
        } else if (plan.getTags() != null) {
          // Tag key, tag value
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
          long finalOffset = offset;
          mtree.updateMNode(
              leafMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(finalOffset));
        }

      } finally {
        if (Objects.nonNull(leafMNode)) {
          mtree.unPinMNode(leafMNode.getAsMNode());
        }
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
   * Create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  @Override
  public void createAlignedTimeSeries(final ICreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }

    try {
      final PartialPath prefixPath = plan.getDevicePath();
      final List<String> measurements = plan.getMeasurements();
      final List<TSDataType> dataTypes = plan.getDataTypes();
      final List<TSEncoding> encodings = plan.getEncodings();
      final List<CompressionType> compressors = plan.getCompressors();
      final List<String> aliasList = plan.getAliasList();
      final List<Map<String, String>> tagsList = plan.getTagsList();
      final List<Map<String, String>> attributesList = plan.getAttributesList();
      final List<IMeasurementMNode<ICachedMNode>> measurementMNodeList;

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      // Used iff with merge
      final Set<Integer> existingMeasurementIndexes = new HashSet<>();

      // Create time series in MTree
      measurementMNodeList =
          mtree.createAlignedTimeSeries(
              prefixPath,
              measurements,
              dataTypes,
              encodings,
              compressors,
              aliasList,
              (plan instanceof CreateAlignedTimeSeriesPlanImpl
                  && ((CreateAlignedTimeSeriesPlanImpl) plan).isWithMerge()),
              existingMeasurementIndexes,
              (plan instanceof CreateAlignedTimeSeriesPlanImpl
                  ? ((CreateAlignedTimeSeriesPlanImpl) plan).getAligned()
                  : null));

      try {
        // Update statistics and schemaDataTypeNumMap
        regionStatistics.addMeasurement(measurementMNodeList.size());

        List<Long> tagOffsets = plan.getTagOffsets();

        // Merge the existing ones
        // The existing measurements are written into the "upsert" but not written
        // to the "createSeries" in mLog
        for (int i = measurements.size() - 1; i >= 0; --i) {
          if (existingMeasurementIndexes.isEmpty()) {
            break;
          }
          if (!existingMeasurementIndexes.remove(i)) {
            continue;
          }
          // WARNING: The input lists can not be immutable when the "withMerge" is set.
          upsertAliasAndTagsAndAttributes(
              Objects.nonNull(aliasList) ? aliasList.remove(i) : null,
              Objects.nonNull(tagsList) ? tagsList.remove(i) : null,
              Objects.nonNull(attributesList) ? attributesList.remove(i) : null,
              prefixPath.concatAsMeasurementPath(measurements.get(i)));
          if (Objects.nonNull(tagOffsets) && !tagOffsets.isEmpty()) {
            tagOffsets.remove(i);
          }
          // Nonnull
          measurements.remove(i);
          dataTypes.remove(i);
          encodings.remove(i);
          compressors.remove(i);
        }

        if (measurementMNodeList.isEmpty()) {
          return;
        }

        for (int i = 0; i < measurements.size(); i++) {
          if (tagOffsets != null && !plan.getTagOffsets().isEmpty() && isRecovering) {
            if (tagOffsets.get(i) != -1) {
              tagManager.recoverIndex(plan.getTagOffsets().get(i), measurementMNodeList.get(i));
              mtree.pinMNode(measurementMNodeList.get(i).getAsMNode());
            }
          } else if (tagsList != null && !tagsList.isEmpty()) {
            if (tagsList.get(i) != null) {
              // Tag key, tag value
              tagManager.addIndex(tagsList.get(i), measurementMNodeList.get(i));
              mtree.pinMNode(measurementMNodeList.get(i).getAsMNode());
            }
          }
        }

        // Write log
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
            final long offset = tagOffsets.get(i);
            mtree.updateMNode(
                measurementMNodeList.get(i).getAsMNode(),
                o -> o.getAsMeasurementMNode().setOffset(offset));
          }
        }
      } finally {
        for (final IMeasurementMNode<ICachedMNode> measurementMNode : measurementMNodeList) {
          mtree.unPinMNode(measurementMNode.getAsMNode());
        }
      }
    } catch (final IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    try {
      return mtree.checkMeasurementExistence(devicePath, measurementList, aliasList);
    } catch (final Exception e) {
      return Collections.emptyMap();
    }
  }

  @Override
  public void checkSchemaQuota(final PartialPath devicePath, final int timeSeriesNum)
      throws SchemaQuotaExceededException {
    if (!mtree.checkDeviceNodeExists(devicePath)) {
      schemaQuotaManager.check(timeSeriesNum, 1);
    } else {
      schemaQuotaManager.check(timeSeriesNum, 0);
    }
  }

  @Override
  public void checkSchemaQuota(final String tableName, final List<Object[]> deviceIdList) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public Pair<Long, Boolean> constructSchemaBlackList(final PathPatternTree patternTree)
      throws MetadataException {
    return constructSchemaBlackListWithAliasInfo(patternTree).toPair();
  }

  @Override
  public org.apache.iotdb.db.schemaengine.schemaregion.write.resp.ConstructSchemaBlackListResult
      constructSchemaBlackListWithAliasInfo(final PathPatternTree patternTree)
          throws MetadataException {
    long preDeletedNum = 0;
    final AtomicBoolean isAllLogicalView = new AtomicBoolean(true);
    final AtomicBoolean isAllInvalidSeries = new AtomicBoolean(true);
    final AtomicBoolean hasInvalidSeries = new AtomicBoolean(false);
    final AtomicBoolean hasNonInvalidSeries = new AtomicBoolean(false);
    final List<
            org.apache.iotdb.db.schemaengine.schemaregion.write.resp.ConstructSchemaBlackListResult
                .ReferencedInvalidPathInfo>
        referencedInvalidPaths = new ArrayList<>();
    final List<PartialPath> preDeletedPaths = new ArrayList<>();

    for (final PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      // Given pathPatterns may match one time series multi times, which may results in the
      // preDeletedNum larger than the actual num of time series. It doesn't matter since the main
      // purpose is to check whether there's time series to be deleted.
      final AtomicBoolean patternIsAllInvalid = new AtomicBoolean(true);
      final AtomicBoolean patternHasInvalidSeries = new AtomicBoolean(false);
      final List<PartialPath> paths =
          mtree.constructSchemaBlackListWithAliasInfo(
              pathPattern,
              isAllLogicalView,
              patternIsAllInvalid,
              patternHasInvalidSeries,
              referencedInvalidPaths);
      preDeletedNum += paths.size();
      if (!paths.isEmpty()) {
        hasNonInvalidSeries.set(true);
        preDeletedPaths.addAll(paths);
      }
      // If this pattern has invalid series, set the global flag
      if (patternHasInvalidSeries.get()) {
        hasInvalidSeries.set(true);
      }
      // If this pattern matched non-invalid series, then overall isAllInvalidSeries is false
      if (!patternIsAllInvalid.get()) {
        isAllInvalidSeries.set(false);
      }
      for (final PartialPath path : paths) {
        try {
          writeToMLog(SchemaRegionWritePlanFactory.getPreDeleteTimeSeriesPlan(path));
        } catch (final IOException e) {
          throw new MetadataException(e);
        }
      }
    }

    // Set isAllInvalidSeries to true only if no non-invalid series were found
    // and there are invalid series
    if (!hasNonInvalidSeries.get() && hasInvalidSeries.get()) {
      isAllInvalidSeries.set(true);
    }

    return new ConstructSchemaBlackListResult(
        preDeletedNum,
        isAllLogicalView.get(),
        isAllInvalidSeries.get(),
        hasInvalidSeries.get(),
        referencedInvalidPaths,
        preDeletedPaths);
  }

  private void recoverPreDeleteTimeseries(final PartialPath path) throws MetadataException {
    mtree.constructSchemaBlackList(path, new AtomicBoolean(true));
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
  public void alterEncodingCompressor(final AlterEncodingCompressorNode node)
      throws MetadataException {
    throw new UnsupportedOperationException(
        DataNodeSchemaMessages.PBTREE_NOT_SUPPORT_ALTER_ENCODING);
  }

  @Override
  public void validateAndPrepareForAliasSeries(final LockAliasNode node) throws MetadataException {
    final PartialPath oldPath = node.getOldPath();
    final PartialPath newPath = node.getNewPath();
    final IMeasurementMNode<ICachedMNode> oldPathNode = mtree.getMeasurementMNode(oldPath);

    if (Objects.equals(oldPath, newPath)) {
      return;
    }

    try {
      // Check if oldPath is pre-deleted (in deletion blacklist)
      if (oldPathNode.isPreDeleted()) {
        throw new MeasurementInBlackListException(oldPath);
      }

      // Check if oldPath is already being renamed (prevent concurrent rename operations)
      if (oldPathNode.isRenaming()) {
        throw new MetadataException(
            String.format(
                "Timeseries [%s] is already being renamed, cannot perform another rename operation",
                oldPath.getFullPath()),
            TSStatusCode.TIMESERIES_ALREADY_RENAMING.getStatusCode(),
            true);
      }

      // Check if oldPath is a logical view (logical views may not support alias series)
      if (oldPathNode.isLogicalView()) {
        throw new MetadataException(
            String.format(
                "Cannot create alias series for logical view [%s]", oldPath.getFullPath()),
            TSStatusCode.VIEW_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode(),
            true);
      }

      // Check if oldPath is invalid (invalid series cannot be renamed/aliased)
      if (oldPathNode.isInvalid()) {
        throw new MetadataException(
            String.format(
                "Cannot create alias series for invalid timeseries [%s]", oldPath.getFullPath()),
            TSStatusCode.INVALID_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode(),
            true);
      }

      // Check if oldPath is from template (template-based time series cannot be renamed/aliased)
      final ICachedMNode parentNode = oldPathNode.getParent();
      if (parentNode != null && parentNode.isDevice()) {
        final int templateId = parentNode.getAsDeviceMNode().getSchemaTemplateId();
        if (templateId != SchemaConstant.NON_TEMPLATE) {
          throw new MetadataException(
              String.format(
                  "Cannot create alias series for template-based timeseries [%s]. "
                      + "Template-based time series cannot be renamed.",
                  oldPath.getFullPath()),
              TSStatusCode.TEMPLATE_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode(),
              true);
        }
      }

      // Mark oldPath as isRenaming to prevent concurrent operations
      mtree.updateMNode(
          oldPathNode.getAsMNode(), o -> o.getAsMeasurementMNode().setIsRenaming(true));

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(oldPathNode.getAsMNode());
    }
  }

  /**
   * Rollback createAliasSeries operation by deleting the alias series at newPath.
   *
   * @param node the CreateAliasSeriesNode containing rollback information
   * @param newPath the alias path to delete
   * @throws MetadataException if other errors occur
   */
  private void rollbackCreateAliasSeries(
      final CreateAliasSeriesNode node, final PartialPath newPath) throws MetadataException {
    IMeasurementMNode<ICachedMNode> aliasNode = null;
    try {
      // Get the alias measurement node (verify it exists)
      aliasNode = mtree.getMeasurementMNode(newPath);
      // Rollback must be idempotent. If alias creation never took effect and the target path is
      // still occupied by a physical/invalid series, leave it untouched.
      if (!aliasNode.isRenamed()) {
        mtree.unPinMNode(aliasNode.getAsMNode());
        return;
      }
      // Physically delete the alias node
      IMeasurementMNode<ICachedMNode> deletedNode = mtree.deleteTimeseries(newPath);
      removeFromTagInvertedIndex(deletedNode);
      regionStatistics.deleteMeasurement(1L);
      // Write log
      if (!isRecovering) {
        writeToMLog(node);
      }
    } catch (PathNotExistException e) {
      // Alias series doesn't exist, which is fine for rollback - just return
    } catch (IOException e) {
      // If any operation throws IOException, we need to unpin the node if it was pinned
      try {
        mtree.unPinMNode(aliasNode.getAsMNode());
      } catch (Exception ignored) {
        // Ignore exceptions during unpin
      }
      throw new MetadataException(e);
    }
  }

  @Override
  public void createAliasSeries(final CreateAliasSeriesNode node) throws MetadataException {
    final PartialPath oldPath = node.getOldPath(); // Physical path to be aliased
    final PartialPath newPath = node.getNewPath(); // New alias path

    // If this is a rollback operation, delete the alias series at newPath
    if (node.isRollback()) {
      rollbackCreateAliasSeries(node, newPath);
      return;
    }

    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }

    final TTimeSeriesInfo timeSeriesInfo = node.getTimeSeriesInfo();

    // Validate that newPath can be used for alias creation
    validateAliasPathForCreation(newPath, oldPath);

    if (timeSeriesInfo == null) {
      throw new MetadataException("TTimeSeriesInfo is required for createAliasSeries but is null");
    }

    // Extract schema information from TTimeSeriesInfo
    final TSDataType dataType = TSDataType.values()[timeSeriesInfo.getDataType()];
    final TSEncoding encoding = TSEncoding.values()[timeSeriesInfo.getEncoding()];
    final CompressionType compressor = CompressionType.values()[timeSeriesInfo.getCompressor()];
    final String alias = timeSeriesInfo.getMeasurementAlias();

    // Build props map with alias series properties
    Map<String, String> props =
        MeasurementPropsUtils.buildAliasSeriesProps(timeSeriesInfo.getProps(), oldPath);

    try {
      SchemaUtils.checkDataTypeWithEncoding(dataType, encoding);

      // Create alias measurement node at newPath using mtree.createTimeSeriesWithPinnedReturn
      final IMeasurementMNode<ICachedMNode> aliasMNode =
          mtree.createTimeSeriesWithPinnedReturn(
              newPath, dataType, encoding, compressor, props, alias, false, null, false);

      processCreatedAliasMNode(aliasMNode, timeSeriesInfo, node, newPath);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Validate that the newPath can be used for alias creation.
   *
   * @param newPath the new alias path to validate
   * @param oldPath the physical path to be aliased
   * @throws MetadataException if the path cannot be used for alias creation
   */
  private void validateAliasPathForCreation(final PartialPath newPath, final PartialPath oldPath)
      throws MetadataException {
    try {
      IMeasurementMNode<ICachedMNode> existingNode = mtree.getMeasurementMNode(newPath);
      if (existingNode == null) {
        // Node doesn't exist, continue with creation
        return;
      }

      // Note: This check should not be reached in normal flow, as pre-deleted nodes should be
      // handled earlier. This is a defensive check.
      if (existingNode.isPreDeleted()) {
        mtree.unPinMNode(existingNode.getAsMNode());
        throw new MetadataException(
            "Node " + newPath.getFullPath() + " is currently being deleted", true);
      }

      // Path exists, check if it's invalid, an alias or physical path
      Map<String, String> existingProps = existingNode.getSchema().getProps();
      boolean isInvalid = MeasurementPropsUtils.isInvalid(existingProps);
      boolean isRenamed = MeasurementPropsUtils.isRenamed(existingProps);

      if (isRenamed) {
        validateExistingAlias(existingNode, existingProps, newPath, oldPath);
      } else {
        validateExistingPhysicalPath(existingNode, isInvalid, newPath);
      }
    } catch (PathNotExistException e) {
      // Path doesn't exist, which is expected - continue with creation
    }
  }

  /**
   * Validate an existing alias node.
   *
   * @param existingNode the existing alias node
   * @param existingProps the properties of the existing node
   * @param newPath the new alias path
   * @param oldPath the physical path to be aliased
   * @throws MetadataException if validation fails
   * @throws PathAlreadyExistException if alias exists but points to different physical path
   */
  private void validateExistingAlias(
      final IMeasurementMNode<ICachedMNode> existingNode,
      final Map<String, String> existingProps,
      final PartialPath newPath,
      final PartialPath oldPath)
      throws MetadataException {
    PartialPath existingPhysicalPath = MeasurementPropsUtils.getOriginalPath(existingProps);
    if (existingPhysicalPath.equals(oldPath)) {
      // Same alias already exists pointing to the same physical path
      mtree.unPinMNode(existingNode.getAsMNode());
      // Even if it's invalid, we consider it as already existing and return successfully
      return;
    }
    // Alias exists but points to different physical path, this is an error
    mtree.unPinMNode(existingNode.getAsMNode());
    throw new PathAlreadyExistException(
        newPath.getFullPath() + " already exists as an alias pointing to a different path");
  }

  /**
   * Validate an existing physical path.
   *
   * @param existingNode the existing physical node
   * @param isInvalid whether the node is invalid
   * @param newPath the new alias path
   * @throws MetadataException if path is invalid and cannot be used to create alias
   * @throws PathAlreadyExistException if path already exists as a physical path
   */
  private void validateExistingPhysicalPath(
      final IMeasurementMNode<ICachedMNode> existingNode,
      final boolean isInvalid,
      final PartialPath newPath)
      throws MetadataException {
    mtree.unPinMNode(existingNode.getAsMNode());
    // It's a physical path, cannot create alias with the same name
    // If it's invalid, we also cannot create alias for it
    if (isInvalid) {
      throw new MetadataException(
          String.format(
              "Path [%s] is already an invalid series and cannot be used to create an alias "
                  + "series",
              newPath.getFullPath()),
          true);
    }
    throw new PathAlreadyExistException(
        newPath.getFullPath() + " already exists as a physical path");
  }

  /**
   * Process the created alias MNode: update statistics, tags, and write logs.
   *
   * @param aliasMNode the created alias MNode
   * @param timeSeriesInfo the time series information
   * @param node the CreateAliasSeriesNode
   * @param newPath the new alias path
   * @throws MetadataException if errors occur during processing
   * @throws IOException if I/O errors occur during tag/attribute writing
   */
  private void processCreatedAliasMNode(
      final IMeasurementMNode<ICachedMNode> aliasMNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final CreateAliasSeriesNode node,
      final PartialPath newPath)
      throws MetadataException, IOException {
    try {
      // Should merge
      if (Objects.isNull(aliasMNode)) {
        // Write an upsert plan directly
        // Note that the "pin" and "unpin" is reentrant
        upsertAliasAndTagsAndAttributes(
            timeSeriesInfo.getMeasurementAlias(),
            timeSeriesInfo.getTags() != null ? timeSeriesInfo.getTags() : null,
            timeSeriesInfo.getAttributes() != null ? timeSeriesInfo.getAttributes() : null,
            newPath);
        return;
      }

      // Update statistics and schemaDataTypeNumMap
      regionStatistics.addMeasurement(1L);

      // Update tag index
      updateTagIndexForAlias(aliasMNode, timeSeriesInfo);

      // write log
      long offset = writeTagsAndAttributesIfNeeded(timeSeriesInfo, node);
      if (offset != -1) {
        long finalOffset = offset;
        mtree.updateMNode(
            aliasMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(finalOffset));
      }
    } finally {
      if (Objects.nonNull(aliasMNode)) {
        mtree.unPinMNode(aliasMNode.getAsMNode());
      }
    }
  }

  /**
   * Update tag index for alias MNode.
   *
   * @param aliasMNode the alias MNode
   * @param timeSeriesInfo the time series information
   * @throws MetadataException if errors occur
   */
  private void updateTagIndexForAlias(
      final IMeasurementMNode<ICachedMNode> aliasMNode, final TTimeSeriesInfo timeSeriesInfo)
      throws MetadataException {
    // Note: offset is not used in this context, so we skip the recovery case
    if (timeSeriesInfo.getTags() != null) {
      // Tag key, tag value
      tagManager.addIndex(timeSeriesInfo.getTags(), aliasMNode);
      mtree.pinMNode(aliasMNode.getAsMNode());
    }
  }

  /**
   * Write tags and attributes to tag file if needed and write log.
   *
   * @param timeSeriesInfo the time series information
   * @param node the CreateAliasSeriesNode
   * @return the offset if tags/attributes were written, -1 otherwise
   * @throws MetadataException if errors occur
   * @throws IOException if I/O errors occur during tag/attribute writing
   */
  private long writeTagsAndAttributesIfNeeded(
      final TTimeSeriesInfo timeSeriesInfo, final CreateAliasSeriesNode node)
      throws MetadataException, IOException {
    if (isRecovering) {
      return -1;
    }

    long offset = -1;
    // either tags or attributes is not empty
    if ((timeSeriesInfo.getTags() != null && !timeSeriesInfo.getTags().isEmpty())
        || (timeSeriesInfo.getAttributes() != null && !timeSeriesInfo.getAttributes().isEmpty())) {
      try {
        offset = tagManager.writeTagFile(timeSeriesInfo.getTags(), timeSeriesInfo.getAttributes());
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
    writeToMLog(node);
    return offset;
  }

  /**
   * Rollback markSeriesInvalid operation by removing INVALID flag and clearing ALIAS_PATH.
   *
   * @param node the MarkSeriesInvalidNode containing rollback information
   * @param oldPath the physical path to enable
   * @throws MetadataException if errors occur during rollback
   */
  private void rollbackMarkSeriesInvalid(
      final MarkSeriesInvalidNode node, final PartialPath oldPath) throws MetadataException {
    // Get the physical measurement node (verify it exists)
    final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(oldPath);
    try {
      // Validate that the physical node is not a logical view
      if (physicalNode.isLogicalView()) {
        throw new MetadataException(
            String.format(
                "Cannot rollback markSeriesInvalid for logical view [%s]", oldPath.getFullPath()));
      }

      // Remove INVALID flag (set INVALID=false) and clear ALIAS_PATH (set to null)
      mtree.updateMNode(
          physicalNode.getAsMNode(),
          o -> {
            o.getAsMeasurementMNode().setInvalid(false);
            o.getAsMeasurementMNode().setAliasPath(null);
          });

      // Update statistics (decrease invalid series count)
      regionStatistics.deleteInvalidSeries(1L);
      regionStatistics.addMeasurement(1L);

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(physicalNode.getAsMNode());
    }
  }

  @Override
  public void markSeriesInvalid(final MarkSeriesInvalidNode node) throws MetadataException {
    final PartialPath oldPath = node.getOldPath();
    final PartialPath newPath = node.getNewPath();

    // If this is a rollback operation, remove INVALID flag, clear ALIAS_PATH, and delete alias
    // series
    if (node.isRollback()) {
      rollbackMarkSeriesInvalid(node, oldPath);
      return;
    }

    // Get the physical measurement node (verify it exists)
    final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(oldPath);
    try {
      // Validate that the physical node is not a logical view
      if (physicalNode.isLogicalView()) {
        throw new MetadataException(
            String.format("Cannot mark logical view [%s] as invalid", oldPath.getFullPath()));
      }

      // Check if already invalid and ALIAS_PATH is already set to newPath
      if (physicalNode.isInvalid()
          && physicalNode.getAliasPath() != null
          && physicalNode.getAliasPath().equals(newPath)) {
        // Already marked as invalid with the same alias path, return successfully
        return;
      }

      // Mark physical node as invalid (INVALID=true) and set ALIAS_PATH
      mtree.updateMNode(
          physicalNode.getAsMNode(),
          o -> {
            o.getAsMeasurementMNode().setInvalid(true);
            o.getAsMeasurementMNode().setAliasPath(newPath);
          });

      // Update statistics
      regionStatistics.addInvalidSeries(1L);
      regionStatistics.deleteMeasurement(1L);

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(physicalNode.getAsMNode());
    }
  }

  @Override
  public void updatePhysicalAliasRef(final UpdatePhysicalAliasRefNode node)
      throws MetadataException {
    final PartialPath physicalPath = node.getPhysicalPath();
    final PartialPath newAliasPath = node.getNewAliasPath();
    final PartialPath oldAliasPath = node.getOldAliasPath();

    // If this is a rollback operation, restore ALIAS_PATH to oldAliasPath
    if (node.isRollback()) {
      rollbackUpdatePhysicalAliasRef(node, physicalPath, oldAliasPath);
      return;
    }

    // Get the physical measurement node (verify it exists)
    final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(physicalPath);
    try {
      // Validate that the physical node is not a logical view
      if (physicalNode.isLogicalView()) {
        throw new MetadataException(
            String.format(
                "Cannot update alias reference for logical view [%s]", physicalPath.getFullPath()));
      }

      if (!physicalNode.isInvalid()) {
        throw new MetadataException(
            String.format(
                "Cannot update alias reference for physical path [%s]: "
                    + "physical node is not invalid (only invalid nodes can update alias "
                    + "reference)",
                physicalPath.getFullPath()));
      }
      if (!Objects.equals(physicalNode.getAliasPath(), oldAliasPath)) {
        String currentAliasPathStr =
            physicalNode.getAliasPath() != null
                ? physicalNode.getAliasPath().getFullPath()
                : "null";
        throw new MetadataException(
            String.format(
                "Cannot update alias reference for physical path [%s]: "
                    + "current alias path [%s] does not match expected old alias path [%s]",
                physicalPath.getFullPath(), currentAliasPathStr, oldAliasPath.getFullPath()));
      }

      // Update ALIAS_PATH to newAliasPath
      mtree.updateMNode(
          physicalNode.getAsMNode(), o -> o.getAsMeasurementMNode().setAliasPath(newAliasPath));

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(physicalNode.getAsMNode());
    }
  }

  /**
   * Rollback updatePhysicalAliasRef operation by restoring ALIAS_PATH to the original alias path.
   *
   * @param node the UpdatePhysicalAliasRefNode containing rollback information
   * @param physicalPath the physical path to update
   * @param oldAliasPath the current alias path (before rollback, used for logging purposes)
   * @throws MetadataException if errors occur during rollback
   */
  private void rollbackUpdatePhysicalAliasRef(
      final UpdatePhysicalAliasRefNode node,
      final PartialPath physicalPath,
      final PartialPath oldAliasPath)
      throws MetadataException {
    try {
      // Get the physical measurement node (verify it exists)
      final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(physicalPath);
      try {
        // Validate that the physical node is not a logical view
        if (physicalNode.isLogicalView()) {
          throw new MetadataException(
              String.format(
                  "Cannot rollback update alias reference for logical view [%s]",
                  physicalPath.getFullPath()));
        }

        mtree.updateMNode(
            physicalNode.getAsMNode(), o -> o.getAsMeasurementMNode().setAliasPath(oldAliasPath));

        // Write log
        if (!isRecovering) {
          try {
            writeToMLog(node);
          } catch (IOException e) {
            throw new MetadataException(e);
          }
        }
      } finally {
        mtree.unPinMNode(physicalNode.getAsMNode());
      }
    } catch (PathNotExistException e) {
      // Physical series doesn't exist, which is fine for rollback - just return
    }
  }

  @Override
  public void dropAliasSeries(final DropAliasSeriesNode node) throws MetadataException {
    final PartialPath aliasPath = node.getAliasPath();

    // If this is a rollback operation, recreate the alias series
    if (node.isRollback()) {
      rollbackDropAliasSeries(node, aliasPath);
      return;
    }

    // Get the alias measurement node (verify it exists)
    IMeasurementMNode<ICachedMNode> aliasNode;
    try {
      aliasNode = mtree.getMeasurementMNode(aliasPath);
    } catch (PathNotExistException e) {
      // Alias series doesn't exist, which is fine - just return successfully
      return;
    }

    try {
      // Verify it's an alias series (IS_RENAMED=true)
      if (!aliasNode.isRenamed()) {
        mtree.unPinMNode(aliasNode.getAsMNode());
        throw new MetadataException(
            String.format(
                "Timeseries [%s] is not an alias series, cannot drop as alias",
                aliasPath.getFullPath()));
      }

      // Validate that the alias node is not a logical view (logical views are not alias series)
      if (aliasNode.isLogicalView()) {
        mtree.unPinMNode(aliasNode.getAsMNode());
        throw new MetadataException(
            String.format(
                "Logical view [%s] cannot be dropped as an alias series", aliasPath.getFullPath()));
      }

      // Physically delete the alias node
      // Note: deleteTimeseries will return the deleted node and handle unpin internally
      IMeasurementMNode<ICachedMNode> deletedNode = mtree.deleteTimeseries(aliasPath);
      removeFromTagInvertedIndex(deletedNode);
      regionStatistics.deleteMeasurement(1L);

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } catch (IOException e) {
      // If deleteTimeseries throws IOException, we need to unpin the node
      try {
        mtree.unPinMNode(aliasNode.getAsMNode());
      } catch (Exception ignored) {
        // Ignore exceptions during unpin
      }
      throw new MetadataException(e);
    }
  }

  /**
   * Rollback dropAliasSeries operation by recreating the alias series from saved TTimeSeriesInfo.
   *
   * @param node the DropAliasSeriesNode containing rollback information
   * @param aliasPath the alias path to recreate
   * @throws MetadataException if errors occur during rollback
   */
  private void rollbackDropAliasSeries(final DropAliasSeriesNode node, final PartialPath aliasPath)
      throws MetadataException {
    // Check if alias series already exists (may have been recreated by another operation)
    if (checkAndHandleExistingAlias(aliasPath)) {
      return;
    }

    // Alias series doesn't exist, need to recreate it using saved TTimeSeriesInfo
    TTimeSeriesInfo timeSeriesInfo = node.getTimeSeriesInfo();
    PartialPath physicalPath = node.getPhysicalPath();

    if (timeSeriesInfo == null || physicalPath == null) {
      throw new MetadataException(
          String.format(
              "Cannot rollback dropAliasSeries: missing rollback information for [%s]",
              aliasPath.getFullPath()));
    }

    // Recreate alias series
    IMeasurementMNode<ICachedMNode> aliasMNode =
        recreateAliasSeries(aliasPath, timeSeriesInfo, physicalPath);

    if (aliasMNode != null) {
      processRecreatedAliasMNode(aliasMNode, timeSeriesInfo, node);
    }
  }

  /**
   * Check if alias series already exists and handle it.
   *
   * @param aliasPath the alias path to check
   * @return true if alias already exists and was handled, false otherwise
   * @throws MetadataException if the path exists but is not an alias series
   */
  private boolean checkAndHandleExistingAlias(final PartialPath aliasPath)
      throws MetadataException {
    try {
      IMeasurementMNode<ICachedMNode> existingNode = mtree.getMeasurementMNode(aliasPath);
      if (existingNode.isRenamed()) {
        // Alias series already exists, rollback is successful
        mtree.unPinMNode(existingNode.getAsMNode());
        return true;
      }
      mtree.unPinMNode(existingNode.getAsMNode());
      throw new MetadataException(
          String.format(
              "Cannot rollback dropAliasSeries: path [%s] exists but is not an alias series",
              aliasPath.getFullPath()));
    } catch (PathNotExistException e) {
      // Alias series doesn't exist, need to recreate
      return false;
    }
  }

  /**
   * Recreate alias series from saved TTimeSeriesInfo.
   *
   * @param aliasPath the alias path to recreate
   * @param timeSeriesInfo the time series information
   * @param physicalPath the physical path
   * @return the created alias MNode
   * @throws MetadataException if errors occur
   */
  private IMeasurementMNode<ICachedMNode> recreateAliasSeries(
      final PartialPath aliasPath,
      final TTimeSeriesInfo timeSeriesInfo,
      final PartialPath physicalPath)
      throws MetadataException {
    // Extract schema information from TTimeSeriesInfo
    final TSDataType dataType = TSDataType.values()[timeSeriesInfo.getDataType()];
    final TSEncoding encoding = TSEncoding.values()[timeSeriesInfo.getEncoding()];
    final CompressionType compressor = CompressionType.values()[timeSeriesInfo.getCompressor()];
    final String alias = timeSeriesInfo.getMeasurementAlias();

    // Build props map with alias series properties
    Map<String, String> props =
        MeasurementPropsUtils.buildAliasSeriesProps(timeSeriesInfo.getProps(), physicalPath);

    // Recreate alias series
    return mtree.createTimeSeriesWithPinnedReturn(
        aliasPath, dataType, encoding, compressor, props, alias, false, null, false);
  }

  /**
   * Process the recreated alias MNode: update statistics, tags, and write logs.
   *
   * @param aliasMNode the recreated alias MNode
   * @param timeSeriesInfo the time series information
   * @param node the DropAliasSeriesNode
   * @throws MetadataException if errors occur during processing
   */
  private void processRecreatedAliasMNode(
      final IMeasurementMNode<ICachedMNode> aliasMNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final DropAliasSeriesNode node)
      throws MetadataException {
    try {
      regionStatistics.addMeasurement(1L);

      // Update tag index if timeSeriesInfo has tags
      if (timeSeriesInfo.getTags() != null && !timeSeriesInfo.getTags().isEmpty()) {
        tagManager.addIndex(timeSeriesInfo.getTags(), aliasMNode);
      }

      // Write tags and attributes if available
      writeTagsAndAttributesForRollback(aliasMNode, timeSeriesInfo);

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException i) {
          throw new MetadataException(i);
        }
      }
    } finally {
      mtree.unPinMNode(aliasMNode.getAsMNode());
    }
  }

  /**
   * Write tags and attributes to tag file for rollback operation.
   *
   * @param aliasMNode the alias MNode
   * @param timeSeriesInfo the time series information
   * @throws MetadataException if errors occur
   */
  private void writeTagsAndAttributesForRollback(
      final IMeasurementMNode<ICachedMNode> aliasMNode, final TTimeSeriesInfo timeSeriesInfo)
      throws MetadataException {
    if ((timeSeriesInfo.getTags() != null && !timeSeriesInfo.getTags().isEmpty())
        || (timeSeriesInfo.getAttributes() != null && !timeSeriesInfo.getAttributes().isEmpty())) {
      try {
        long offset =
            tagManager.writeTagFile(timeSeriesInfo.getTags(), timeSeriesInfo.getAttributes());
        mtree.updateMNode(
            aliasMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));
      } catch (IOException i) {
        throw new MetadataException(i);
      }
    }
  }

  @Override
  public void markSeriesEnabled(final MarkSeriesEnabledNode node) throws MetadataException {
    final PartialPath physicalPath = node.getPhysicalPath();
    final PartialPath aliasPath = node.getAliasPath();
    final TTimeSeriesInfo timeSeriesInfo = node.getTimeSeriesInfo();

    // If this is a rollback operation, re-mark as invalid and restore ALIAS_PATH
    if (node.isRollback()) {
      rollbackMarkSeriesEnabled(node, physicalPath, aliasPath);
      return;
    }

    // Get the physical measurement node (verify it exists)
    final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(physicalPath);
    try {
      validatePhysicalNodeForEnable(physicalNode, physicalPath);
      if (isAlreadyEnabled(physicalNode)) {
        return;
      }

      updatePhysicalNodeForEnable(physicalNode, timeSeriesInfo, physicalPath);
      regionStatistics.deleteInvalidSeries(1L);
      regionStatistics.addMeasurement(1L);
      writeLogIfNotRecovering(node);
    } finally {
      mtree.unPinMNode(physicalNode.getAsMNode());
    }
  }

  /**
   * Validate that the physical node can be marked as enabled.
   *
   * @param physicalNode the physical measurement node
   * @param physicalPath the physical path
   * @throws MetadataException if validation fails
   */
  private void validatePhysicalNodeForEnable(
      final IMeasurementMNode<ICachedMNode> physicalNode, final PartialPath physicalPath)
      throws MetadataException {
    if (physicalNode.isLogicalView()) {
      throw new MetadataException(
          String.format("Cannot mark logical view [%s] as enabled", physicalPath.getFullPath()));
    }
  }

  /**
   * Check if the physical node is already enabled.
   *
   * @param physicalNode the physical measurement node
   * @return true if already enabled, false otherwise
   */
  private boolean isAlreadyEnabled(final IMeasurementMNode<ICachedMNode> physicalNode) {
    return !physicalNode.isInvalid() && physicalNode.getAliasPath() == null;
  }

  /**
   * Update physical node to mark it as enabled.
   *
   * @param physicalNode the physical measurement node
   * @param timeSeriesInfo the time series information
   * @param physicalPath the physical path
   * @throws MetadataException if errors occur
   */
  private void updatePhysicalNodeForEnable(
      final IMeasurementMNode<ICachedMNode> physicalNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final PartialPath physicalPath)
      throws MetadataException {
    if (timeSeriesInfo != null) {
      updatePhysicalNodeWithSchema(physicalNode, timeSeriesInfo, physicalPath);
    } else {
      updatePhysicalNodeWithoutSchema(physicalNode);
    }
  }

  /**
   * Update physical node with schema information from timeSeriesInfo.
   *
   * @param physicalNode the physical measurement node
   * @param timeSeriesInfo the time series information
   * @param physicalPath the physical path
   * @throws MetadataException if errors occur
   */
  private void updatePhysicalNodeWithSchema(
      final IMeasurementMNode<ICachedMNode> physicalNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final PartialPath physicalPath)
      throws MetadataException {
    MeasurementSchema newSchema = createMeasurementSchema(physicalNode, timeSeriesInfo);

    // Update INVALID, ALIAS_PATH, and schema in one operation
    mtree.updateMNode(
        physicalNode.getAsMNode(),
        o -> {
          o.getAsMeasurementMNode().setInvalid(false);
          o.getAsMeasurementMNode().setAliasPath(null);
          o.getAsMeasurementMNode().setSchema(newSchema);
        });

    updateTagsAndAttributes(physicalNode, timeSeriesInfo, physicalPath);
  }

  /**
   * Create MeasurementSchema from timeSeriesInfo.
   *
   * @param physicalNode the physical measurement node
   * @param timeSeriesInfo the time series information
   * @return the created MeasurementSchema
   */
  private MeasurementSchema createMeasurementSchema(
      final IMeasurementMNode<ICachedMNode> physicalNode, final TTimeSeriesInfo timeSeriesInfo) {
    final TSDataType dataType = TSDataType.values()[timeSeriesInfo.getDataType()];
    final TSEncoding encoding = TSEncoding.values()[timeSeriesInfo.getEncoding()];
    final CompressionType compressor = CompressionType.values()[timeSeriesInfo.getCompressor()];

    Map<String, String> props = new HashMap<>();
    if (timeSeriesInfo.getProps() != null && !timeSeriesInfo.getProps().isEmpty()) {
      props.putAll(timeSeriesInfo.getProps());
    }

    return new MeasurementSchema(physicalNode.getName(), dataType, encoding, compressor, props);
  }

  /**
   * Update tags and attributes for the physical node.
   *
   * @param physicalNode the physical measurement node
   * @param timeSeriesInfo the time series information
   * @param physicalPath the physical path
   * @throws MetadataException if errors occur
   */
  private void updateTagsAndAttributes(
      final IMeasurementMNode<ICachedMNode> physicalNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final PartialPath physicalPath)
      throws MetadataException {
    if (isRecovering) {
      return;
    }

    if (timeSeriesInfo.getTags() != null && !timeSeriesInfo.getTags().isEmpty()) {
      tagManager.addIndex(timeSeriesInfo.getTags(), physicalNode);
    }

    writeTagsAndAttributesToFile(physicalNode, timeSeriesInfo, physicalPath);
  }

  /**
   * Write tags and attributes to tag file if needed.
   *
   * @param physicalNode the physical measurement node
   * @param timeSeriesInfo the time series information
   * @param physicalPath the physical path
   * @throws MetadataException if errors occur
   */
  private void writeTagsAndAttributesToFile(
      final IMeasurementMNode<ICachedMNode> physicalNode,
      final TTimeSeriesInfo timeSeriesInfo,
      final PartialPath physicalPath)
      throws MetadataException {
    boolean hasTags = timeSeriesInfo.getTags() != null && !timeSeriesInfo.getTags().isEmpty();
    boolean hasAttributes =
        timeSeriesInfo.getAttributes() != null && !timeSeriesInfo.getAttributes().isEmpty();

    if (!hasTags && !hasAttributes) {
      return;
    }

    try {
      long offset =
          tagManager.writeTagFile(timeSeriesInfo.getTags(), timeSeriesInfo.getAttributes());
      if (offset != -1) {
        mtree.updateMNode(
            physicalNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));
      }
    } catch (IOException e) {
      throw new MetadataException(
          "Failed to write tags and attributes to tag file for " + physicalPath.getFullPath(), e);
    }
  }

  /**
   * Update physical node without schema information (only update INVALID and ALIAS_PATH).
   *
   * @param physicalNode the physical measurement node
   */
  private void updatePhysicalNodeWithoutSchema(final IMeasurementMNode<ICachedMNode> physicalNode) {
    mtree.updateMNode(
        physicalNode.getAsMNode(),
        o -> {
          o.getAsMeasurementMNode().setInvalid(false);
          o.getAsMeasurementMNode().setAliasPath(null);
        });
  }

  /**
   * Write log if not recovering.
   *
   * @param node the MarkSeriesEnabledNode
   * @throws MetadataException if errors occur
   */
  private void writeLogIfNotRecovering(final MarkSeriesEnabledNode node) throws MetadataException {
    if (!isRecovering) {
      try {
        writeToMLog(node);
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
  }

  /**
   * Rollback markSeriesEnabled operation by re-marking as invalid and restoring ALIAS_PATH.
   *
   * @param node the MarkSeriesEnabledNode containing rollback information
   * @param physicalPath the physical path to mark as invalid
   * @param aliasPath the alias path to restore in ALIAS_PATH
   * @throws MetadataException if errors occur during rollback
   */
  private void rollbackMarkSeriesEnabled(
      final MarkSeriesEnabledNode node, final PartialPath physicalPath, final PartialPath aliasPath)
      throws MetadataException {
    try {
      // Get the physical measurement node (verify it exists)
      final IMeasurementMNode<ICachedMNode> physicalNode = mtree.getMeasurementMNode(physicalPath);
      try {
        // Validate that the physical node is not a logical view
        if (physicalNode.isLogicalView()) {
          throw new MetadataException(
              String.format(
                  "Cannot rollback mark logical view [%s] as enabled", physicalPath.getFullPath()));
        }

        // Re-mark as invalid (set INVALID=true) and restore ALIAS_PATH
        mtree.updateMNode(
            physicalNode.getAsMNode(),
            o -> {
              o.getAsMeasurementMNode().setInvalid(true);
              o.getAsMeasurementMNode().setAliasPath(aliasPath);
            });

        // Update statistics
        regionStatistics.addInvalidSeries(1L);
        regionStatistics.deleteMeasurement(1L);

        // Write log
        if (!isRecovering) {
          try {
            writeToMLog(node);
          } catch (IOException e) {
            throw new MetadataException(e);
          }
        }
      } finally {
        mtree.unPinMNode(physicalNode.getAsMNode());
      }
    } catch (PathNotExistException e) {
      // Physical series doesn't exist, which is fine for rollback - just return
    }
  }

  @Override
  public void unlockForAlias(final UnlockForAliasNode node) throws MetadataException {
    final PartialPath oldPath = node.getOldPath();

    IMeasurementMNode<ICachedMNode> oldPathNode;
    try {
      // Get oldPath node
      oldPathNode = mtree.getMeasurementMNode(oldPath);
    } catch (PathNotExistException e) {
      // If the node no longer exists (e.g., due to a rollback or concurrent deletion),
      // we can consider the unlock operation idempotent and log a warning.
      logger.warn(
          "Attempted to unlock non-existent path [{}]. It might have been deleted.",
          oldPath.getFullPath());
      // Still write to MLog if not recovering, to ensure the procedure state is consistent.
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException ioException) {
          throw new MetadataException(ioException);
        }
      }
      return;
    }

    try {
      if (oldPathNode.isMeasurement() && oldPathNode.isRenaming()) {
        // Clear isRenaming flag
        mtree.updateMNode(
            oldPathNode.getAsMNode(), o -> o.getAsMeasurementMNode().setIsRenaming(false));
      }

      // Write log
      if (!isRecovering) {
        try {
          writeToMLog(node);
        } catch (IOException ioException) {
          throw new MetadataException(ioException);
        }
      }
    } finally {
      mtree.unPinMNode(oldPathNode.getAsMNode());
    }
  }

  @Override
  public void createLogicalView(ICreateLogicalViewPlan plan) throws MetadataException {
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    try {
      List<PartialPath> pathList = plan.getViewPathList();
      Map<PartialPath, ViewExpression> viewPathToSourceMap =
          plan.getViewPathToSourceExpressionMap();
      for (PartialPath path : pathList) {
        ViewExpression viewExpression = viewPathToSourceMap.get(path);
        mtree.createLogicalView(path, viewExpression);
        // Write log
        if (!isRecovering) {
          writeToMLog(SchemaRegionWritePlanFactory.getCreateLogicalViewPlan(path, viewExpression));
        }
        regionStatistics.addView(1L);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public long constructLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException {
    long preDeletedNum = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      // Given pathPatterns may match one logical view multi times, which may results in the
      // preDeletedNum larger than the actual num of logical view. It doesn't matter since the main
      // purpose is to check whether there's logical view to be deleted.
      List<PartialPath> paths = mtree.constructLogicalViewBlackList(pathPattern);
      preDeletedNum += paths.size();
      for (PartialPath path : paths) {
        try {
          writeToMLog(SchemaRegionWritePlanFactory.getPreDeleteLogicalViewPlan(path));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
    return preDeletedNum;
  }

  @Override
  public void rollbackLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException {
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      List<PartialPath> paths = mtree.rollbackLogicalViewBlackList(pathPattern);
      for (PartialPath path : paths) {
        try {
          writeToMLog(SchemaRegionWritePlanFactory.getRollbackPreDeleteLogicalViewPlan(path));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public void deleteLogicalView(PathPatternTree patternTree) throws MetadataException {
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      for (PartialPath path : mtree.getPreDeletedLogicalView(pathPattern)) {
        try {
          deleteSingleTimeseriesInBlackList(path);
          writeToMLog(SchemaRegionWritePlanFactory.getDeleteLogicalViewPlan(path));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public void alterLogicalView(IAlterLogicalViewPlan alterLogicalViewPlan)
      throws MetadataException {
    mtree.alterLogicalView(
        alterLogicalViewPlan.getViewPath(), alterLogicalViewPlan.getSourceExpression());
    // Write log
    if (!isRecovering) {
      try {
        writeToMLog(alterLogicalViewPlan);
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
  }

  private void deleteSingleTimeseriesInBlackList(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);
    if (measurementMNode.isLogicalView()) {
      regionStatistics.deleteView(1L);
    } else {
      if (measurementMNode.isInvalid()) {
        regionStatistics.deleteInvalidSeries(1L);
      } else {
        regionStatistics.deleteMeasurement(1L);
      }
    }
  }

  private void recoverRollbackPreDeleteTimeseries(PartialPath path) throws MetadataException {
    mtree.rollbackSchemaBlackList(path);
  }

  /**
   * @param path full path from root to leaf node
   */
  private void deleteOneTimeseriesUpdateStatistics(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode<ICachedMNode> measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);
    if (measurementMNode.isLogicalView()) {
      regionStatistics.deleteView(1L);
    } else {
      if (measurementMNode.isInvalid()) {
        regionStatistics.deleteInvalidSeries(1L);
      } else {
        regionStatistics.deleteMeasurement(1L);
      }
    }
  }

  // endregion

  // region Interfaces for get and auto create device
  /**
   * Get device node, if the schema region is not set, create it when autoCreateSchema is true
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
  public MeasurementPath fetchMeasurementPath(PartialPath fullPath) throws MetadataException {
    IMeasurementMNode<ICachedMNode> node = mtree.getMeasurementMNode(fullPath);
    try {
      MeasurementPath res = new MeasurementPath(node.getPartialPath(), node.getSchema());
      res.setUnderAlignedEntity(node.getParent().getAsDeviceMNode().isAligned());
      return res;
    } finally {
      mtree.unPinMNode(node.getAsMNode());
    }
  }

  @Override
  public ClusterSchemaTree fetchSeriesSchema(
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      boolean withTags,
      boolean withAttributes,
      boolean withTemplate,
      boolean withAliasForcete)
      throws MetadataException {
    if (patternTree.isContainWildcard()) {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      for (PartialPath path : patternTree.getAllPathPatterns()) {
        schemaTree.mergeSchemaTree(
            mtree.fetchSchema(
                path, templateMap, withTags, withAttributes, withTemplate, withAliasForcete));
      }
      return schemaTree;
    } else {
      return mtree.fetchSchemaWithoutWildcard(
          patternTree, templateMap, withTags, withAttributes, withTemplate);
    }
  }

  @Override
  public ClusterSchemaTree fetchDeviceSchema(
      PathPatternTree patternTree, PathPatternTree authorityScope) throws MetadataException {
    return mtree.fetchDeviceSchema(patternTree, authorityScope);
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
      mtree.updateMNode(
          measurementMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));

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
    while (tagsMap != null && !regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

      if (tagsMap == null && attributesMap == null) {
        return;
      }
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(tagsMap, attributesMap);
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        mtree.updateMNode(leafMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));
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
    if (alias != null) {
      // Check if the time series is invalid (INVALID=true)
      IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
      try {
        if (leafMNode.isInvalid()) {
          throw new MetadataException(
              String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
        }

        // Check if the time series is being renamed (IS_RENAMING=true)
        if (leafMNode.isRenaming()) {
          throw new MetadataException(
              String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
        }
      } finally {
        mtree.unPinMNode(leafMNode.getAsMNode());
      }

      while (!regionStatistics.isAllowToCreateNewSeries()) {
        ReleaseFlushMonitor.getInstance().waitIfReleasing();
      }
      if (mtree.changeAlias(alias, fullPath)) {
        // persist to WAL
        writeToMLog(SchemaRegionWritePlanFactory.getChangeAliasPlan(fullPath, alias));
      }
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
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        mtree.updateMNode(leafMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));
        return;
      }

      tagManager.addAttributes(attributesMap, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
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
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
        writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        mtree.updateMNode(leafMNode.getAsMNode(), o -> o.getAsMeasurementMNode().setOffset(offset));
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
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

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
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format(DataNodeSchemaMessages.TIMESERIES_NO_TAG_ATTRIBUTE, fullPath));
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
    while (!regionStatistics.isAllowToCreateNewSeries()) {
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    IMeasurementMNode<ICachedMNode> leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // Check if the time series is invalid (INVALID=true)
      if (leafMNode.isInvalid()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_INVALID_TIME_SERIES, fullPath.getFullPath()), true);
      }

      // Check if the time series is being renamed (IS_RENAMING=true)
      if (leafMNode.isRenaming()) {
        throw new MetadataException(
            String.format(MSG_CANNOT_ALTER_RENAMING_TIME_SERIES, fullPath.getFullPath()), true);
      }

      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format(
                DataNodeSchemaMessages.TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE, fullPath, oldKey),
            true);
      }
      // tags, attributes
      tagManager.renameTagOrAttributeKey(oldKey, newKey, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode.getAsMNode());
    }
  }

  /**
   * Set/change the data type of measurement
   *
   * @param newDataType the new data type
   * @param fullPath timeseries
   * @throws MetadataException write error or data type do not exist
   */
  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void alterTimeSeriesDataType(final TSDataType newDataType, final PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException(
        DataNodeSchemaMessages.PBTREE_NOT_SUPPORT_ALTER_DATA_TYPE);
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
      ReleaseFlushMonitor.getInstance().waitIfReleasing();
    }
    try {
      ICachedMNode deviceNode = getDeviceNodeWithAutoCreate(plan.getActivatePath());
      try {
        plan.setAligned(template.isDirectAligned());
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
  public void createOrUpdateTableDevice(
      final CreateOrUpdateTableDeviceNode createOrUpdateTableDeviceNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public void updateTableDeviceAttribute(final TableDeviceAttributeUpdateNode updateNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public void deleteTableDevice(final DeleteTableDeviceNode deleteTableDeviceNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public void dropTableAttribute(final TableAttributeColumnDropNode dropTableAttributeNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public long constructTableDevicesBlackList(
      final ConstructTableDevicesBlackListNode constructTableDevicesBlackListNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public void rollbackTableDevicesBlackList(
      final RollbackTableDevicesBlackListNode rollbackTableDevicesBlackListNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public void deleteTableDevicesInBlackList(
      final DeleteTableDevicesInBlackListNode rollbackTableDevicesBlackListNode) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.TABLE_MODEL_NOT_SUPPORT_PBTREE);
  }

  @Override
  public int fillLastQueryMap(
      final PartialPath pattern,
      final Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> mapToFill,
      final PathPatternTree scope) {
    throw new UnsupportedOperationException(DataNodeSchemaMessages.NOT_IMPLEMENTED);
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
              logger.error(DataNodeSchemaMessages.FAILED_TO_READ_TAG_ATTRIBUTE, e.getMessage(), e);
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

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(final PartialPath pathPattern)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(
      String table, List<Object[]> devicePathList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<Long, Map<TDataNodeLocation, byte[]>> getAttributeUpdateInfo(
      final AtomicInteger limit, final AtomicBoolean hasRemaining) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitUpdateAttribute(final TableDeviceAttributeCommitUpdateNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addNodeLocation(final TableNodeLocationAddNode node) {
    throw new UnsupportedOperationException();
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
              DataNodeSchemaMessages.SCHEMA_REGION_PLAN_NOT_SUPPORT_RECOVER_PBTREE,
              plan.getPlanType().name()));
    }

    @Override
    public RecoverOperationResult visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, SchemaRegionPBTreeImpl context) {
      try {
        recoverTimeSeries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
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

    @Override
    public RecoverOperationResult visitCreateAliasSeries(
        final CreateAliasSeriesNode createAliasSeriesNode, final SchemaRegionPBTreeImpl context) {
      try {
        createAliasSeries(createAliasSeriesNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitLockAlias(
        final LockAliasNode lockAliasNode, final SchemaRegionPBTreeImpl context) {
      try {
        validateAndPrepareForAliasSeries(lockAliasNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitMarkSeriesInvalid(
        final MarkSeriesInvalidNode markSeriesInvalidNode, final SchemaRegionPBTreeImpl context) {
      try {
        markSeriesInvalid(markSeriesInvalidNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitUpdatePhysicalAliasRef(
        final UpdatePhysicalAliasRefNode updatePhysicalAliasRefNode,
        final SchemaRegionPBTreeImpl context) {
      try {
        updatePhysicalAliasRef(updatePhysicalAliasRefNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDropAliasSeries(
        final DropAliasSeriesNode dropAliasSeriesNode, final SchemaRegionPBTreeImpl context) {
      try {
        dropAliasSeries(dropAliasSeriesNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitMarkSeriesEnabled(
        final MarkSeriesEnabledNode markSeriesEnabledNode, final SchemaRegionPBTreeImpl context) {
      try {
        markSeriesEnabled(markSeriesEnabledNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitUnlockForAlias(
        final UnlockForAliasNode unlockForAliasNode, final SchemaRegionPBTreeImpl context) {
      try {
        unlockForAlias(unlockForAliasNode);
        return RecoverOperationResult.SUCCESS;
      } catch (final MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }
  }
}
