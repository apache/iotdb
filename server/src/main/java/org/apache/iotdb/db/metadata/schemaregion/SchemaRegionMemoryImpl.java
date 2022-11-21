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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.SeriesNumberOverflowException;
import org.apache.iotdb.db.exception.metadata.SeriesOverflowException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.logfile.FakeCRC32Deserializer;
import org.apache.iotdb.db.metadata.logfile.FakeCRC32Serializer;
import org.apache.iotdb.db.metadata.logfile.SchemaLogReader;
import org.apache.iotdb.db.metadata.logfile.SchemaLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSGMemoryImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanDeserializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanSerializer;
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
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;
import org.apache.iotdb.db.metadata.rescon.SchemaStatisticsManager;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.external.api.ISeriesNumerMonitor;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
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

  // the log file writer
  private boolean usingMLog = true;
  private SchemaLogWriter<ISchemaRegionPlan> logWriter;

  private SchemaStatisticsManager schemaStatisticsManager = SchemaStatisticsManager.getInstance();
  private MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private final IStorageGroupMNode storageGroupMNode;
  private MTreeBelowSGMemoryImpl mtree;
  // device -> DeviceMNode
  private LoadingCache<PartialPath, IMNode> mNodeCache;
  private TagManager tagManager;

  // seriesNumberMonitor may be null
  private final ISeriesNumerMonitor seriesNumerMonitor;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionMemoryImpl(
      PartialPath storageGroup,
      SchemaRegionId schemaRegionId,
      IStorageGroupMNode storageGroupMNode,
      ISeriesNumerMonitor seriesNumerMonitor)
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
        && config
            .getSchemaRegionConsensusProtocolClass()
            .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      File schemaRegionDir = new File(schemaRegionDirPath);
      if (schemaRegionDir.exists()) {
        FileUtils.deleteDirectory(schemaRegionDir);
      }
    }

    this.seriesNumerMonitor = seriesNumerMonitor;

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
              storageGroupMNode, tagManager::readTags, schemaRegionId.getId());

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
    List<IMeasurementMNode> leafMNodes = mtree.getAllMeasurementMNode();

    int seriesCount = leafMNodes.size();
    schemaStatisticsManager.deleteTimeseries(seriesCount);
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries(seriesCount);
    }

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

  public void createTimeseries(ICreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!memoryStatistics.isAllowToCreateNewSeries()) {
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

        // the cached mNode may be replaced by new entityMNode in mtree
        mNodeCache.invalidate(path.getDevicePath());
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(1);
        }
        throw t;
      }

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
    if (!memoryStatistics.isAllowToCreateNewSeries()) {
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

        // the cached mNode may be replaced by new entityMNode in mtree
        mNodeCache.invalidate(prefixPath);
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(seriesCount);
        }
        throw t;
      }

      // update statistics and schemaDataTypeNumMap
      schemaStatisticsManager.addTimeseries(seriesCount);

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

  /**
   * Delete all timeseries matching the given path pattern. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * deleted.
   *
   * @param pathPattern path to be deleted
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return deletion failed Timeseries
   */
  @Override
  public synchronized Pair<Integer, Set<String>> deleteTimeseries(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    try {
      List<MeasurementPath> allTimeseries = mtree.getMeasurementPaths(pathPattern, isPrefixMatch);

      Set<String> failedNames = new HashSet<>();
      int deletedNum = 0;
      for (PartialPath p : allTimeseries) {
        deleteSingleTimeseriesInternal(p);
        deletedNum++;
      }
      return new Pair<>(deletedNum, failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  @Override
  public int constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    int preDeletedNum = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      for (IMeasurementMNode measurementMNode : mtree.getMatchedMeasurementMNode(pathPattern)) {
        // Given pathPatterns may match one timeseries multi times, which may results in the
        // preDeletedNum larger than the actual num of timeseries. It doesn't matter since the main
        // purpose is to check whether there's timeseries to be deleted.
        preDeletedNum++;
        measurementMNode.setPreDeleted(true);
        try {
          writeToMLog(
              SchemaRegionPlanFactory.getPreDeleteTimeSeriesPlan(
                  measurementMNode.getPartialPath()));
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
      for (IMeasurementMNode measurementMNode : mtree.getMatchedMeasurementMNode(pathPattern)) {
        measurementMNode.setPreDeleted(false);
        try {
          writeToMLog(
              SchemaRegionPlanFactory.getRollbackPreDeleteTimeSeriesPlan(
                  measurementMNode.getPartialPath()));
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
              SchemaRegionPlanFactory.getDeleteTimeSeriesPlan(Collections.singletonList(path)));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  private void deleteSingleTimeseriesInBlackList(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, IMeasurementMNode> pair =
        mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);

    IMeasurementMNode measurementMNode = pair.right;
    removeFromTagInvertedIndex(measurementMNode);

    IMNode node = measurementMNode.getParent();

    mNodeCache.invalidate(node.getPartialPath());

    schemaStatisticsManager.deleteTimeseries(1);
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries(1);
    }
  }

  private void recoverRollbackPreDeleteTimeseries(PartialPath path) throws MetadataException {
    IMeasurementMNode measurementMNode = mtree.getMeasurementMNode(path);
    measurementMNode.setPreDeleted(false);
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

  private void deleteSingleTimeseriesInternal(PartialPath p) throws MetadataException, IOException {
    deleteOneTimeseriesUpdateStatistics(p);
    if (!isRecovering) {
      writeToMLog(SchemaRegionPlanFactory.getDeleteTimeSeriesPlan(Collections.singletonList(p)));
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return After delete if the schema region is empty, return its path, otherwise return null
   */
  private PartialPath deleteOneTimeseriesUpdateStatistics(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, IMeasurementMNode> pair =
        mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);

    IMeasurementMNode measurementMNode = pair.right;
    removeFromTagInvertedIndex(measurementMNode);
    PartialPath storageGroupPath = pair.left;

    IMNode node = measurementMNode.getParent();

    mNodeCache.invalidate(node.getPartialPath());

    schemaStatisticsManager.deleteTimeseries(1);
    if (seriesNumerMonitor != null) {
      seriesNumerMonitor.deleteTimeSeries(1);
    }
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
    writeToMLog(SchemaRegionPlanFactory.getAutoCreateDeviceMNodePlan(node.getPartialPath()));
    return node;
  }

  @Override
  public void autoCreateDeviceMNode(IAutoCreateDeviceMNodePlan plan) throws MetadataException {
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
  @Override
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
  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, isPrefixMatch);
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, templateMap, isPrefixMatch);
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
  @Override
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
  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
  }

  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getDevices(pathPattern, isPrefixMatch);
  }

  /**
   * Get all device paths and according database paths as ShowDevicesResult.
   *
   * @param plan ShowDevicesPlan which contains the path pattern and restriction params.
   * @return ShowDevicesResult and the current offset of this region after traverse.
   */
  @Override
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
  @Override
  public List<MeasurementPath> getMeasurementPaths(
      PartialPath pathPattern, boolean isPrefixMatch, boolean withTags) throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch, withTags).left;
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
    return getMeasurementPaths(pathPattern, false, false);
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
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException {
    return mtree.getMeasurementPathsWithAlias(pathPattern, limit, offset, isPrefixMatch, withTags);
  }

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    return mtree.fetchSchema(pathPattern, templateMap, withTags);
  }

  @Override
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
          ? pathPattern.prefixMatchFullPath(leaf.getPartialPath())
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
          Pair<String, String> deadbandInfo =
              MetaUtils.parseDeadbandInfo(measurementSchema.getProps());
          res.add(
              new ShowTimeSeriesResult(
                  leaf.getFullPath(),
                  leaf.getAlias(),
                  storageGroupFullPath,
                  measurementSchema.getType(),
                  measurementSchema.getEncodingType(),
                  measurementSchema.getCompressor(),
                  0,
                  tagAndAttributePair.left,
                  tagAndAttributePair.right,
                  deadbandInfo.left,
                  deadbandInfo.right));
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
                tagAndAttributePair.right,
                ansString.right[7],
                ansString.right[8]));
      } catch (IOException e) {
        throw new MetadataException(
            "Something went wrong while deserialize tag info of " + ansString.left.getFullPath(),
            e);
      }
    }
    return new Pair<>(res, ans.right);
  }
  // endregion
  // endregion

  // region Interfaces and methods for MNode query
  @Override
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

  @Override
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

  @Override
  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(path);
    if (leafMNode.getAlias() != null) {
      leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
    }
    leafMNode.getParent().addAlias(alias, leafMNode);
    mtree.setAlias(leafMNode, alias);

    try {
      writeToMLog(SchemaRegionPlanFactory.getChangeAliasPlan(path, alias));
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
  @Override
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
      writeToMLog(SchemaRegionPlanFactory.getChangeTagOffsetPlan(fullPath, offset));
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
      writeToMLog(SchemaRegionPlanFactory.getChangeAliasPlan(fullPath, alias));
    }
  }

  /**
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);

    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
      writeToMLog(SchemaRegionPlanFactory.getChangeTagOffsetPlan(fullPath, offset));
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
  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
      writeToMLog(SchemaRegionPlanFactory.getChangeTagOffsetPlan(fullPath, offset));
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
   * set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
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
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
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

  // region Interfaces and Implementation for InsertPlan process

  /** create timeseries ignoring MeasurementAlreadyExistException */
  private void internalCreateTimeseries(
      PartialPath path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws MetadataException {
    try {
      createTimeseries(
          SchemaRegionPlanFactory.getCreateTimeSeriesPlan(
              path, dataType, encoding, compressor, Collections.emptyMap(), null, null, null));
    } catch (MeasurementAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore MeasurementAlreadyExistException when concurrent inserting"
                + " a non-exist time series {}",
            path);
      }
    }
  }

  /** create aligned timeseries ignoring MeasurementAlreadyExistException */
  private void internalAlignedCreateTimeseries(
      PartialPath devicePath,
      String measurement,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws MetadataException {
    try {
      createAlignedTimeSeries(
          SchemaRegionPlanFactory.getCreateAlignedTimeSeriesPlan(
              devicePath,
              Collections.singletonList(measurement),
              Collections.singletonList(dataType),
              Collections.singletonList(encoding),
              Collections.singletonList(compressor),
              null,
              null,
              null));
    } catch (MeasurementAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore MeasurementAlreadyExistException when concurrent inserting"
                + " a non-exist time series {}",
            e.getMeasurementPath());
      }
    }
  }

  @Override
  public DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
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
            TSDataType dataType = getDataType.apply(i);
            TSEncoding encoding =
                encodings[i] == null ? getDefaultEncoding(dataType) : encodings[i];
            CompressionType compressionType =
                compressionTypes[i] == null
                    ? TSFileDescriptor.getInstance().getConfig().getCompressor()
                    : compressionTypes[i];
            if (aligned) {
              internalAlignedCreateTimeseries(
                  devicePath, measurements[i], dataType, encoding, compressionType);
            } else {
              internalCreateTimeseries(
                  devicePath.concatNode(measurements[i]), dataType, encoding, compressionType);
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
  public List<String> getPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    return mtree.getPathsUsingTemplate(pathPattern, templateId);
  }

  @Override
  public int constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException {
    int preDeactivateNum = 0;
    Map<PartialPath, List<Integer>> templateSetInfo = plan.getTemplateSetInfo();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      for (IEntityMNode entityMNode :
          mtree.getDeviceMNodeUsingTargetTemplate(entry.getKey(), entry.getValue())) {
        Map<PartialPath, List<Integer>> subTemplateSetInfo = new HashMap<>();
        subTemplateSetInfo.put(
            entityMNode.getPartialPath(),
            Collections.singletonList(entityMNode.getSchemaTemplateId()));
        entityMNode.preDeactivateTemplate();
        preDeactivateNum++;
        try {
          writeToMLog(SchemaRegionPlanFactory.getPreDeactivateTemplatePlan(subTemplateSetInfo));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
    return preDeactivateNum;
  }

  @Override
  public void rollbackSchemaBlackListWithTemplate(IRollbackPreDeactivateTemplatePlan plan)
      throws MetadataException {
    Map<PartialPath, List<Integer>> templateSetInfo = plan.getTemplateSetInfo();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      for (IEntityMNode entityMNode :
          mtree.getPreDeactivatedDeviceMNode(entry.getKey(), entry.getValue())) {
        if (!entityMNode.isPreDeactivateTemplate()) {
          continue;
        }
        Map<PartialPath, List<Integer>> subTemplateSetInfo = new HashMap<>();
        subTemplateSetInfo.put(
            entityMNode.getPartialPath(),
            Collections.singletonList(entityMNode.getSchemaTemplateId()));
        entityMNode.rollbackPreDeactivateTemplate();
        try {
          writeToMLog(
              SchemaRegionPlanFactory.getRollbackPreDeactivateTemplatePlan(subTemplateSetInfo));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException {
    Map<PartialPath, List<Integer>> templateSetInfo = plan.getTemplateSetInfo();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      for (IEntityMNode entityMNode :
          mtree.getPreDeactivatedDeviceMNode(entry.getKey(), entry.getValue())) {
        Map<PartialPath, List<Integer>> subTemplateSetInfo = new HashMap<>();
        subTemplateSetInfo.put(
            entityMNode.getPartialPath(),
            Collections.singletonList(entityMNode.getSchemaTemplateId()));
        entityMNode.deactivateTemplate();
        mtree.deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(entityMNode);
        try {
          writeToMLog(SchemaRegionPlanFactory.getDeactivateTemplatePlan(subTemplateSetInfo));
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    }
  }

  @Override
  public int countPathsUsingTemplate(int templateId, PathPatternTree patternTree)
      throws MetadataException {
    int result = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      result += mtree.countPathsUsingTemplate(pathPattern, templateId);
    }
    return result;
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
