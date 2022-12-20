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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSGCachedImpl;
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
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;
import org.apache.iotdb.db.metadata.rescon.SchemaStatisticsManager;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.external.api.ISeriesNumerMonitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
public class SchemaRegionSchemaFileImpl implements ISchemaRegion {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionSchemaFileImpl.class);

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
  // the log file seriesPath
  //  private String logFilePath;
  //  private File logFile;
  private SchemaLogWriter<ISchemaRegionPlan> logWriter;

  private final SchemaStatisticsManager schemaStatisticsManager =
      SchemaStatisticsManager.getInstance();
  private final MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();

  private MTreeBelowSGCachedImpl mtree;
  // device -> DeviceMNode
  private final LoadingCache<PartialPath, IMNode> mNodeCache;
  private TagManager tagManager;

  // seriesNumberMonitor may be null
  private final ISeriesNumerMonitor seriesNumerMonitor;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SchemaRegionSchemaFileImpl(
      PartialPath storageGroup,
      SchemaRegionId schemaRegionId,
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
            .removalListener(
                (PartialPath path, IMNode node, RemovalCause cause) -> {
                  if (!isClearing) {
                    mtree.unPinMNode(node);
                  }
                })
            .build(
                new com.github.benmanes.caffeine.cache.CacheLoader<PartialPath, IMNode>() {
                  @Override
                  public @Nullable IMNode load(@NonNull PartialPath partialPath)
                      throws MetadataException {

                    return mtree.getNodeByPath(partialPath);
                  }
                });
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
          new MTreeBelowSGCachedImpl(
              new PartialPath(storageGroupFullPath), tagManager::readTags, schemaRegionId.getId());

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
    try {
      logWriter.force();
    } catch (IOException e) {
      logger.error("Cannot force {} mlog to the schema region", schemaRegionId, e);
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
    isClearing = true;
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
    isClearing = false;
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
      } catch (SeriesOverflowException e) {
        logger.warn(
            "Too many timeseries during recovery from MLog, waiting for SchemaFile swapping.");
        try {
          Thread.sleep(3000L);
        } catch (InterruptedException e2) {
          logger.error("Exception occurs during timeseries recovery.");
          throw new MetadataException(e2.getMessage());
        }
      }
    }
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!memoryStatistics.isAllowToCreateNewSeries()) {
      logger.error(
          String.format("Series overflow when creating: [%s]", plan.getPath().getFullPath()));
      throw new SeriesOverflowException();
    }

    if (seriesNumerMonitor != null && !seriesNumerMonitor.addTimeSeries(1)) {
      throw new SeriesNumberOverflowException();
    }

    try {
      PartialPath path = plan.getPath();
      IMeasurementMNode leafMNode;
      // using try-catch to restore seriesNumberMonitor's state while create failed
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
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(1);
        }
        throw t;
      }

      try {
        // the cached mNode may be replaced by new entityMNode in mtree
        mNodeCache.invalidate(path.getDevicePath());

        // update statistics and schemaDataTypeNumMap
        schemaStatisticsManager.addTimeseries(1);

        // update tag index
        if (offset != -1 && isRecovering) {
          // the timeseries has already been created and now system is recovering, using the tag
          // info
          // in tagFile to recover index directly
          mtree.pinMNode(leafMNode);
          tagManager.recoverIndex(offset, leafMNode);
        } else if (plan.getTags() != null) {
          // tag key, tag value
          mtree.pinMNode(leafMNode);
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
          mtree.updateMNode(leafMNode);
        }

      } finally {
        mtree.unPinMNode(leafMNode);
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
          SchemaRegionPlanFactory.getCreateTimeSeriesPlan(
              path, dataType, encoding, compressor, props, null, null, null));
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
        SchemaRegionPlanFactory.getCreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  public void recoverAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException {
    boolean done = false;
    while (!done) {
      try {
        createAlignedTimeSeries(plan);
        done = true;
      } catch (SeriesOverflowException e) {
        logger.warn(
            "Too many timeseries during recovery from MLog, waiting for SchemaFile swapping.");
        try {
          Thread.sleep(3000L);
        } catch (InterruptedException e2) {
          logger.error("Exception occurs during timeseries recovery.");
          throw new MetadataException(e2.getMessage());
        }
      }
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
      } catch (Throwable t) {
        if (seriesNumerMonitor != null) {
          seriesNumerMonitor.deleteTimeSeries(seriesCount);
        }
        throw t;
      }

      try {
        // the cached mNode may be replaced by new entityMNode in mtree
        mNodeCache.invalidate(prefixPath);

        // update statistics and schemaDataTypeNumMap
        schemaStatisticsManager.addTimeseries(seriesCount);

        List<Long> tagOffsets = plan.getTagOffsets();
        for (int i = 0; i < measurements.size(); i++) {
          if (tagOffsets != null && !plan.getTagOffsets().isEmpty() && isRecovering) {
            if (tagOffsets.get(i) != -1) {
              mtree.pinMNode(measurementMNodeList.get(i));
              tagManager.recoverIndex(plan.getTagOffsets().get(i), measurementMNodeList.get(i));
            }
          } else if (tagsList != null && !tagsList.isEmpty()) {
            if (tagsList.get(i) != null) {
              // tag key, tag value
              mtree.pinMNode(measurementMNodeList.get(i));
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
            mtree.updateMNode(measurementMNodeList.get(i));
          }
        }
      } finally {
        for (IMeasurementMNode measurementMNode : measurementMNodeList) {
          mtree.unPinMNode(measurementMNode);
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
  public long constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    long preDeletedNum = 0;
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      List<IMeasurementMNode> measurementMNodeList = mtree.getMatchedMeasurementMNode(pathPattern);
      try {
        for (IMeasurementMNode measurementMNode : measurementMNodeList) {
          // Given pathPatterns may match one timeseries multi times, which may results in the
          // preDeletedNum larger than the actual num of timeseries. It doesn't matter since the
          // main
          // purpose is to check whether there's timeseries to be deleted.
          try {
            preDeletedNum++;
            measurementMNode.setPreDeleted(true);
            mtree.updateMNode(measurementMNode);
            writeToMLog(
                SchemaRegionPlanFactory.getPreDeleteTimeSeriesPlan(
                    measurementMNode.getPartialPath()));
          } catch (IOException e) {
            throw new MetadataException(e);
          }
        }
      } finally {
        for (IMeasurementMNode measurementMNode : measurementMNodeList) {
          mtree.unPinMNode(measurementMNode);
        }
      }
    }
    return preDeletedNum;
  }

  @Override
  public void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
      for (IMeasurementMNode measurementMNode : mtree.getMatchedMeasurementMNode(pathPattern)) {
        try {
          measurementMNode.setPreDeleted(false);
          mtree.updateMNode(measurementMNode);
          writeToMLog(
              SchemaRegionPlanFactory.getRollbackPreDeleteTimeSeriesPlan(
                  measurementMNode.getPartialPath()));
        } catch (IOException e) {
          throw new MetadataException(e);
        } finally {
          mtree.unPinMNode(measurementMNode);
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
      node = mNodeCache.get(path);
      try {
        mtree.pinMNode(node);
        return node;
      } catch (MetadataException e) {
        // the node in mNodeCache has been evicted, thus get it via the following progress
        return mtree.getNodeByPath(path);
      }
    } catch (Exception e) {
      if (e.getCause() instanceof MetadataException) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new PathNotExistException(path.getFullPath());
        }
      } else {
        throw e;
      }
    }

    node = mtree.getDeviceNodeWithAutoCreating(path);
    writeToMLog(SchemaRegionPlanFactory.getAutoCreateDeviceMNodePlan(node.getPartialPath()));
    return node;
  }

  private void autoCreateDeviceMNode(IAutoCreateDeviceMNodePlan plan) throws MetadataException {
    IMNode node = mtree.getDeviceNodeWithAutoCreating(plan.getPath());
    mtree.unPinMNode(node);
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
  public long getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, isPrefixMatch);
  }

  @Override
  public long getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getAllTimeseriesCount(pathPattern, templateMap, isPrefixMatch);
  }

  @Override
  public long getAllTimeseriesCount(
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
  public long getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getDevicesNum(pathPattern, isPrefixMatch);
  }

  @Override
  public Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    return mtree.getMeasurementCountGroupByLevel(pathPattern, level, isPrefixMatch);
  }

  @Override
  public Map<PartialPath, Long> getMeasurementCountGroupByLevel(
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
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException {
    return mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch);
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
    IMeasurementMNode measurementMNode = mtree.getMeasurementMNode(fullPath);
    mtree.unPinMNode(measurementMNode);
    return measurementMNode;
  }

  /**
   * Invoked during insertPlan process. Get target MeasurementMNode from given EntityMNode. If the
   * result is not null and is not MeasurementMNode, it means a timeseries with same path cannot be
   * created thus throw PathAlreadyExistException.
   */
  protected IMeasurementMNode getMeasurementMNode(IMNode deviceMNode, String measurementName)
      throws MetadataException {
    IMNode result = mtree.getChildFromPinnedMNode(deviceMNode, measurementName);
    if (result == null) {
      return null;
    }

    mtree.unPinMNode(result);
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
    try {
      measurementMNode.setOffset(offset);
      mtree.updateMNode(measurementMNode);

      if (isRecovering) {
        try {
          if (tagManager.recoverIndex(offset, measurementMNode)) {
            mtree.pinMNode(measurementMNode);
          }
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }
    } finally {
      mtree.unPinMNode(measurementMNode);
    }
  }

  @Override
  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(path);
    try {
      if (leafMNode.getAlias() != null) {
        leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
      }
      leafMNode.getParent().addAlias(alias, leafMNode);
      mtree.setAlias(leafMNode, alias);
    } finally {
      mtree.unPinMNode(leafMNode);
    }

    try {
      writeToMLog(SchemaRegionPlanFactory.getChangeAliasPlan(path, alias));
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
    try {
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
        mtree.updateMNode(leafMNode);
        // update inverted Index map
        if (tagsMap != null && !tagsMap.isEmpty()) {
          tagManager.addIndex(tagsMap, leafMNode);
          mtree.pinMNode(leafMNode);
        }
        return;
      }

      tagManager.updateTagsAndAttributes(tagsMap, attributesMap, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode);
    }
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
    try {
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(Collections.emptyMap(), attributesMap);
        writeToMLog(SchemaRegionPlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        leafMNode.setOffset(offset);
        mtree.updateMNode(leafMNode);
        return;
      }

      tagManager.addAttributes(attributesMap, fullPath, leafMNode);
    } finally {
      mtree.updateMNode(leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // no tag or attribute, we need to add a new record in log
      if (leafMNode.getOffset() < 0) {
        long offset = tagManager.writeTagFile(tagsMap, Collections.emptyMap());
        writeToMLog(SchemaRegionPlanFactory.getChangeTagOffsetPlan(fullPath, offset));
        leafMNode.setOffset(offset);
        mtree.updateMNode(leafMNode);
        // update inverted Index map
        tagManager.addIndex(tagsMap, leafMNode);
        mtree.pinMNode(leafMNode);
        return;
      }

      tagManager.addTags(tagsMap, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      // no tag or attribute, just do nothing.
      if (leafMNode.getOffset() != -1) {
        tagManager.dropTagsOrAttributes(keySet, fullPath, leafMNode);
        // when the measurementMNode was added to tagIndex, it was pinned
        mtree.unPinMNode(leafMNode);
      }
    } finally {
      mtree.unPinMNode(leafMNode);
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
    try {
      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
      }

      // tags, attributes
      tagManager.setTagsOrAttributesValue(alterMap, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode);
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
    IMeasurementMNode leafMNode = mtree.getMeasurementMNode(fullPath);
    try {
      if (leafMNode.getOffset() < 0) {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey),
            true);
      }
      // tags, attributes
      tagManager.renameTagOrAttributeKey(oldKey, newKey, fullPath, leafMNode);
    } finally {
      mtree.unPinMNode(leafMNode);
    }
  }

  /** remove the node from the tag inverted index */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(IMeasurementMNode node) throws IOException {
    tagManager.removeFromTagInvertedIndex(node);
  }
  // endregion

  // region Interfaces and Implementation for InsertPlan process

  @Override
  public DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean aligned)
      throws MetadataException {
    throw new UnsupportedOperationException();
  }

  // endregion

  // region Interfaces and Implementation for Template operations
  @Override
  public void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {

    try {
      IMNode deviceNode = getDeviceNodeWithAutoCreate(plan.getActivatePath());
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

  @Override
  public List<String> getPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    return mtree.getPathsUsingTemplate(pathPattern, templateId);
  }

  @Override
  public long constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo =
        mtree.constructSchemaBlackListWithTemplate(plan.getTemplateSetInfo());
    try {
      writeToMLog(SchemaRegionPlanFactory.getPreDeactivateTemplatePlan(resultTemplateSetInfo));
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
          SchemaRegionPlanFactory.getRollbackPreDeactivateTemplatePlan(resultTemplateSetInfo));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo =
        mtree.deactivateTemplateInBlackList(plan.getTemplateSetInfo());
    try {
      writeToMLog(SchemaRegionPlanFactory.getDeactivateTemplatePlan(resultTemplateSetInfo));
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
      extends SchemaRegionPlanVisitor<RecoverOperationResult, SchemaRegionSchemaFileImpl> {

    @Override
    public RecoverOperationResult visitSchemaRegionPlan(
        ISchemaRegionPlan plan, SchemaRegionSchemaFileImpl context) {
      throw new UnsupportedOperationException(
          String.format(
              "SchemaRegionPlan of type %s doesn't support recover operation in SchemaRegionSchemaFileImpl.",
              plan.getPlanType().name()));
    }

    @Override
    public RecoverOperationResult visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, SchemaRegionSchemaFileImpl context) {
      try {
        recoverTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan,
        SchemaRegionSchemaFileImpl context) {
      try {
        recoverAlignedTimeSeries(createAlignedTimeSeriesPlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitDeleteTimeSeries(
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, SchemaRegionSchemaFileImpl context) {
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
        IChangeAliasPlan changeAliasPlan, SchemaRegionSchemaFileImpl context) {
      try {
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, SchemaRegionSchemaFileImpl context) {
      try {
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }

    @Override
    public RecoverOperationResult visitAutoCreateDeviceMNode(
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, SchemaRegionSchemaFileImpl context) {
      try {
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        return RecoverOperationResult.SUCCESS;
      } catch (MetadataException e) {
        return new RecoverOperationResult(e);
      }
    }
  }
}
