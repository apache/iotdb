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

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.SeriesNumberOverflowException;
import org.apache.iotdb.db.exception.metadata.SeriesOverflowException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.logfile.FakeCRC32Deserializer;
import org.apache.iotdb.db.metadata.logfile.FakeCRC32Serializer;
import org.apache.iotdb.db.metadata.logfile.SchemaLogReader;
import org.apache.iotdb.db.metadata.logfile.SchemaLogWriter;
import org.apache.iotdb.db.metadata.metric.ISchemaRegionMetric;
import org.apache.iotdb.db.metadata.metric.SchemaRegionMemMetric;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSGMemoryImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanDeserializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanSerializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeAliasPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeTagOffsetPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.external.api.ISeriesNumerMonitor;
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
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces and Implementation for Template operations
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
@SchemaRegion(mode = MetadataConstant.DEFAULT_SCHEMA_ENGINE_MODE)
public class SchemaRegionMemoryImpl implements ISchemaRegion {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionMemoryImpl.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean isRecovering = true;
  private volatile boolean initialized = false;

  private String storageGroupDirPath;
  private String schemaRegionDirPath;
  private String storageGroupFullPath;
  private SchemaRegionId schemaRegionId;

  // the log file writer
  private boolean usingMLog = true;
  private SchemaLogWriter<ISchemaRegionPlan> logWriter;

  private final MemSchemaRegionStatistics regionStatistics;

  private MTreeBelowSGMemoryImpl mtree;
  private TagManager tagManager;

  // seriesNumberMonitor may be null
  private final ISeriesNumerMonitor seriesNumerMonitor;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionMemoryImpl(ISchemaRegionParams schemaRegionParams) throws MetadataException {

    storageGroupFullPath = schemaRegionParams.getDatabase().getFullPath();
    this.schemaRegionId = schemaRegionParams.getSchemaRegionId();

    storageGroupDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + schemaRegionId.getId();

    // In ratis mode, no matter create schemaRegion or recover schemaRegion, the working dir should
    // be clear first
    if (config.isClusterMode()
        && config
            .getSchemaRegionConsensusProtocolClass()
            .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      File schemaRegionDir = new File(schemaRegionDirPath);
      if (schemaRegionDir.exists()) {
        FileUtils.deleteDirectory(schemaRegionDir);
      }
    }

    this.seriesNumerMonitor = schemaRegionParams.getSeriesNumberMonitor();
    this.regionStatistics =
        new MemSchemaRegionStatistics(
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
          new MTreeBelowSGMemoryImpl(
              new PartialPath(storageGroupFullPath), tagManager::readTags, regionStatistics);

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
    int lineNumber = initFromLog();

    logWriter =
        new SchemaLogWriter<>(
            schemaRegionDirPath,
            MetadataConstant.METADATA_LOG,
            new FakeCRC32Serializer<>(new SchemaRegionPlanSerializer()),
            config.getSyncMlogPeriodInMs() == 0);
  }

  public void writeToMLog(ISchemaRegionPlan schemaRegionPlan) throws IOException {
    if (usingMLog && !isRecovering) {
      logWriter.write(schemaRegionPlan);
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
    return new SchemaRegionMemMetric(regionStatistics);
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
      int idx;
      try (SchemaLogReader<ISchemaRegionPlan> mLogReader =
          new SchemaLogReader<>(
              schemaRegionDirPath,
              MetadataConstant.METADATA_LOG,
              new FakeCRC32Deserializer<>(new SchemaRegionPlanDeserializer()))) {
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

  private int applyMLog(SchemaLogReader<ISchemaRegionPlan> mLogReader) {
    int idx = 0;
    ISchemaRegionPlan plan;
    RecoverPlanOperator recoverPlanOperator = new RecoverPlanOperator();
    RecoverOperationResult operationResult;
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

    return idx;
  }

  /** function for clearing metadata components of one schema region */
  @Override
  public synchronized void clear() {
    try {
      if (this.mtree != null) {
        this.mtree.clear();
      }
      this.regionStatistics.clear();
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

  // endregion

  // region Interfaces for schema region Info query and operation

  @Override
  public String getStorageGroupFullPath() {
    return storageGroupFullPath;
  }

  @Override
  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  @Override
  public synchronized void deleteSchemaRegion() throws MetadataException {
    // collect all the LeafMNode in this schema region
    long seriesCount = regionStatistics.getSeriesNumber();
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries((int) seriesCount);
    }

    // clear all the components and release all the file handlers
    clear();

    // delete all the schema region files
    SchemaRegionUtils.deleteSchemaRegionFolder(schemaRegionDirPath, logger);
  }

  // currently, this method is only used for cluster-ratis mode
  @Override
  public synchronized boolean createSnapshot(File snapshotDir) {
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
    isSuccess = mtree.createSnapshot(snapshotDir);
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
              storageGroupFullPath,
              regionStatistics,
              measurementMNode -> {
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
              },
              tagManager::readTags);
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
    } catch (IOException | IllegalPathException e) {
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

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!regionStatistics.isAllowToCreateNewSeries()) {
      throw new SeriesOverflowException();
    }

    if (seriesNumerMonitor != null && !seriesNumerMonitor.addTimeSeries(1)) {
      throw new SeriesNumberOverflowException();
    }

    try {
      IMeasurementMNode leafMNode;

      // using try-catch to restore seriesNumberMonitor's state while create failed
      try {
        PartialPath path = plan.getPath();
        SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

        TSDataType type = plan.getDataType();
        // create time series in MTree
        leafMNode =
            mtree.createTimeseries(
                path,
                type,
                plan.getEncoding(),
                plan.getCompressor(),
                plan.getProps(),
                plan.getAlias());
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(1);
        }
        throw t;
      }

      // update statistics and schemaDataTypeNumMap
      regionStatistics.addTimeseries(1L);

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
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  @Override
  public void createAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException {
    int seriesCount = plan.getMeasurements().size();
    if (!regionStatistics.isAllowToCreateNewSeries()) {
      throw new SeriesOverflowException();
    }

    if (seriesNumerMonitor != null && !seriesNumerMonitor.addTimeSeries(seriesCount)) {
      throw new SeriesNumberOverflowException();
    }

    try {
      PartialPath prefixPath = plan.getDevicePath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();
      List<Map<String, String>> tagsList = plan.getTagsList();
      List<Map<String, String>> attributesList = plan.getAttributesList();
      List<IMeasurementMNode> measurementMNodeList;

      // using try-catch to restore seriesNumberMonitor's state while create failed
      try {
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
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(seriesCount);
        }
        throw t;
      }

      // update statistics and schemaDataTypeNumMap
      regionStatistics.addTimeseries(seriesCount);

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
      IDTable idTable = IDTableManager.getInstance().getIDTable(plan.getDevicePath());
      idTable.createAlignedTimeseries(plan);
    }
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    return mtree.checkMeasurementExistence(devicePath, measurementList, aliasList);
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
    IMeasurementMNode measurementMNode = mtree.getMeasurementMNode(path);
    measurementMNode.setPreDeleted(true);
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

  private void deleteSingleTimeseriesInBlackList(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);

    regionStatistics.deleteTimeseries(1L);
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries(1);
    }
  }

  private void recoverRollbackPreDeleteTimeseries(PartialPath path) throws MetadataException {
    IMeasurementMNode measurementMNode = mtree.getMeasurementMNode(path);
    measurementMNode.setPreDeleted(false);
  }

  /** @param path full path from root to leaf node */
  private void deleteOneTimeseriesUpdateStatistics(PartialPath path)
      throws MetadataException, IOException {
    IMeasurementMNode measurementMNode = mtree.deleteTimeseries(path);
    removeFromTagInvertedIndex(measurementMNode);

    regionStatistics.deleteTimeseries(1L);
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries(1);
    }
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
    IMNode node = mtree.getDeviceNodeWithAutoCreating(path);
    writeToMLog(SchemaRegionWritePlanFactory.getAutoCreateDeviceMNodePlan(node.getPartialPath()));
    return node;
  }

  private void autoCreateDeviceMNode(IAutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath());
    try {
      writeToMLog(plan);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }
  // endregion

  // region Interfaces for metadata info Query

  // region Interfaces for Entity/Device info Query

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

  private void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(path);
    if (leafMNode.getAlias() != null) {
      leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
    }
    leafMNode.getParent().addAlias(alias, leafMNode);
    mtree.setAlias(leafMNode, alias);

    try {
      writeToMLog(SchemaRegionWritePlanFactory.getChangeAliasPlan(path, alias));
    } catch (IOException e) {
      throw new MetadataException(e);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);

    // upsert alias
    upsertAlias(alias, fullPath, leafMNode);

    if (tagsMap == null && attributesMap == null) {
      return;
    }

    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(tagsMap, attributesMap);
      writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);

    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
      writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
      leafMNode.setOffset(offset);
      return;
    }

    tagManager.addAttributes(attributesMap, fullPath, leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
      writeToMLog(SchemaRegionWritePlanFactory.getChangeTagOffsetPlan(fullPath, offset));
      leafMNode.setOffset(offset);
      // update inverted Index map
      tagManager.addIndex(tagsMap, leafMNode);
      return;
    }

    tagManager.addTags(tagsMap, fullPath, leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, just do nothing.
    if (leafMNode.getOffset() != -1) {
      tagManager.dropTagsOrAttributes(keySet, fullPath, leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
    }

    // tags, attributes
    tagManager.setTagsOrAttributesValue(alterMap, fullPath, leafMNode);
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

  // region Interfaces and Implementation for Template operations
  @Override
  public void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {
    try {
      getDeviceNodeWithAutoCreate(plan.getActivatePath());

      mtree.activateTemplate(plan.getActivatePath(), template);
      writeToMLog(plan);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new MetadataException(e);
    }
  }

  private void recoverActivatingSchemaTemplate(IActivateTemplateInClusterPlan plan) {
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
    // TODO: We can consider implement this as a consumer passed to MTree which takes responsibility
    // of operating tree structure and concurrency control in future work.
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
    if (showTimeSeriesPlan.getKey() != null && showTimeSeriesPlan.getValue() != null) {
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
      extends SchemaRegionPlanVisitor<RecoverOperationResult, SchemaRegionMemoryImpl> {

    @Override
    public RecoverOperationResult visitSchemaRegionPlan(
        ISchemaRegionPlan plan, SchemaRegionMemoryImpl context) {
      throw new UnsupportedOperationException(
          String.format(
              "SchemaRegionPlan of type %s doesn't support recover operation in SchemaRegionMemoryImpl.",
              plan.getPlanType().name()));
    }

    @Override
    public RecoverOperationResult visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, SchemaRegionMemoryImpl context) {
      try {
        createTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, SchemaRegionMemoryImpl context) {
      try {
        createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDeleteTimeSeries(
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, SchemaRegionMemoryImpl context) {
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
        IChangeAliasPlan changeAliasPlan, SchemaRegionMemoryImpl context) {
      try {
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, SchemaRegionMemoryImpl context) {
      try {
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitAutoCreateDeviceMNode(
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, SchemaRegionMemoryImpl context) {
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
        SchemaRegionMemoryImpl context) {
      recoverActivatingSchemaTemplate(activateTemplateInClusterPlan);
      return RecoverOperationResult.SUCCESS;
    }

    @Override
    public RecoverOperationResult visitPreDeleteTimeSeries(
        IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, SchemaRegionMemoryImpl context) {
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
        SchemaRegionMemoryImpl context) {
      try {
        recoverRollbackPreDeleteTimeseries(rollbackPreDeleteTimeSeriesPlan.getPath());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitPreDeactivateTemplate(
        IPreDeactivateTemplatePlan preDeactivateTemplatePlan, SchemaRegionMemoryImpl context) {
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
        SchemaRegionMemoryImpl context) {
      try {
        rollbackSchemaBlackListWithTemplate(rollbackPreDeactivateTemplatePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDeactivateTemplate(
        IDeactivateTemplatePlan deactivateTemplatePlan, SchemaRegionMemoryImpl context) {
      try {
        deactivateTemplateInBlackList(deactivateTemplatePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }
  }
}
