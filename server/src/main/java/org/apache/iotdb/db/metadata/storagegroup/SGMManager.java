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
package org.apache.iotdb.db.metadata.storagegroup;

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
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSG;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.TimeseriesStatistics;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateManager;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.gson.JsonObject;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info of one certain
 * storage group and persistent it into files. This class contains the interfaces to modify the
 * metadata in storage group for delta system. All the operations will be inserted into the logs
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
 *   <li>Interfaces and Implementation of initialization、snapshot、recover and clear
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
 *   <li>Interfaces and Implementation for InsertPlan process
 *   <li>Interfaces and Implementation for Template operations
 *   <li>TestOnly Interfaces
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
public class SGMManager {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupSchemaManager.class);

  /** A thread will check whether the MTree is modified lately each such interval. Unit: second */
  private static final long MTREE_SNAPSHOT_THREAD_CHECK_TIME = 600L;

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean isRecovering = true;
  private boolean initialized = false;

  private String sgSchemaDirPath;
  private String storageGroupFullPath;

  // the log file seriesPath
  private String logFilePath;
  private File logFile;
  private MLogWriter logWriter;

  private TimeseriesStatistics timeseriesStatistics = TimeseriesStatistics.getInstance();
  private MTreeBelowSG mtree;
  // device -> DeviceMNode
  private LoadingCache<PartialPath, IMNode> mNodeCache;
  private TagManager tagManager;

  // region Interfaces and Implementation of initialization、snapshot、recover and clear
  public SGMManager() {
    int cacheSize = config.getmManagerCacheSize();
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
  }

  // Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  public synchronized void init(IStorageGroupMNode storageGroupMNode) throws MetadataException {
    if (initialized) {
      return;
    }

    storageGroupFullPath = storageGroupMNode.getFullPath();

    sgSchemaDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    File sgSchemaFolder = SystemFileFactory.INSTANCE.getFile(sgSchemaDirPath);
    if (!sgSchemaFolder.exists()) {
      if (sgSchemaFolder.mkdirs()) {
        logger.info("create storage group schema folder {}", sgSchemaDirPath);
      } else {
        logger.error("create storage group schema folder {} failed.", sgSchemaDirPath);
        throw new SchemaDirCreationFailureException(sgSchemaDirPath);
      }
    }
    logFilePath = sgSchemaDirPath + File.separator + MetadataConstant.METADATA_LOG;

    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);

    try {
      // do not write log when recover
      isRecovering = true;

      tagManager = new TagManager(sgSchemaDirPath);
      mtree = new MTreeBelowSG(storageGroupMNode);

      int lineNumber = initFromLog(logFile);

      logWriter = new MLogWriter(sgSchemaDirPath, MetadataConstant.METADATA_LOG);
      logWriter.setLogNum(lineNumber);
      isRecovering = false;
    } catch (IOException e) {
      logger.error(
          "Cannot recover all MTree from {} file, we try to recover as possible as we can",
          storageGroupFullPath,
          e);
    }
    initialized = true;
  }

  public void forceMlog() {
    try {
      logWriter.force();
    } catch (IOException e) {
      logger.error("Cannot force {} mlog to the storage device", storageGroupFullPath, e);
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
          new MLogReader(sgSchemaDirPath, MetadataConstant.METADATA_LOG); ) {
        idx = applyMLog(mLogReader);
        logger.debug(
            "spend {} ms to deserialize {} mtree from mlog.bin",
            System.currentTimeMillis() - time,
            storageGroupFullPath);
        return idx;
      } catch (Exception e) {
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

  /** function for clearing metadata components of one storage group */
  public synchronized void clear() {
    try {
      if (this.mtree != null) {
        this.mtree.clear();
      }
      if (this.mNodeCache != null) {
        this.mNodeCache.invalidateAll();
      }

      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      tagManager.clear();

      initialized = false;
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
  }

  // this method is mainly used for recover and metadata sync
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
      case TTL:
        SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
        setTTL(setTTLPlan.getDataTTL());
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
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }
  // endregion

  // region Interfaces for Storage Group Info query and operation

  public void setTTL(long dataTTL) throws MetadataException, IOException {
    mtree.getStorageGroupMNode().setDataTTL(dataTTL);
    if (!isRecovering) {
      logWriter.setTTL(new PartialPath(storageGroupFullPath), dataTTL);
    }
  }

  public synchronized void deleteStorageGroup() throws MetadataException {
    // collect all the LeafMNode in this storage group
    List<IMeasurementMNode> leafMNodes = mtree.getAllMeasurementMNode();

    timeseriesStatistics.deleteTimeseries(leafMNodes.size());

    // drop triggers with no exceptions
    TriggerEngine.drop(leafMNodes);

    // clear all the components and release all the file handlers
    clear();

    // delete all the storage group files
    File sgSchemaFolder = SystemFileFactory.INSTANCE.getFile(sgSchemaDirPath);
    File[] sgFiles = sgSchemaFolder.listFiles();
    if (sgFiles == null) {
      throw new MetadataException(
          String.format("Can't get files in storage group schema dir %s", storageGroupFullPath));
    }
    for (File file : sgFiles) {
      if (file.delete()) {
        logger.info("delete storage group schema folder {}", sgSchemaFolder.getAbsolutePath());
      } else {
        logger.info(
            "delete storage group schema folder {} failed.", sgSchemaFolder.getAbsolutePath());
        throw new MetadataException(
            String.format(
                "Failed to delete storage group schema folder %s",
                sgSchemaFolder.getAbsolutePath()));
      }
    }

    if (sgSchemaFolder.delete()) {
      logger.info("delete storage group schema folder {}", sgSchemaFolder.getAbsolutePath());
    } else {
      logger.info(
          "delete storage group schema folder {} failed.", sgSchemaFolder.getAbsolutePath());
      throw new MetadataException(
          String.format(
              "Failed to delete storage group schema folder %s", sgSchemaFolder.getAbsolutePath()));
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

      // update tag index

      if (offset != -1 && isRecovering) {
        // the timeseries has already been created and now system is recovering, using the tag info
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
        logWriter.createTimeseries(plan);
      }
      leafMNode.setOffset(offset);

      // update statistics and schemaDataTypeNumMap
      timeseriesStatistics.addTimeseries(1);

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
    try {
      PartialPath prefixPath = plan.getPrefixPath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      // create time series in MTree
      mtree.createAlignedTimeseries(
          prefixPath,
          measurements,
          plan.getDataTypes(),
          plan.getEncodings(),
          plan.getCompressors());

      // the cached mNode may be replaced by new entityMNode in mtree
      mNodeCache.invalidate(prefixPath);

      // write log
      if (!isRecovering) {
        logWriter.createAlignedTimeseries(plan);
      }

      // update statistics and schemaDataTypeNumMap
      timeseriesStatistics.addTimeseries(plan.getMeasurements().size());

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

    timeseriesStatistics.deleteTimeseries(1);
    return storageGroupPath;
  }
  // endregion

  // region Interfaces for get and auto create device
  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param path path
   * @param
   */
  public IMNode getDeviceNodeWithAutoCreate(PartialPath path, boolean autoCreateSchema)
      throws IOException, MetadataException {
    IMNode node;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (Exception e) {
      if (e.getCause() instanceof MetadataException) {
        if (!autoCreateSchema) {
          throw new PathNotExistException(path.getFullPath());
        }
      } else {
        throw e;
      }
    }

    node = mtree.getDeviceNodeWithAutoCreating(path);
    if (!(node.isStorageGroup())) {
      logWriter.autoCreateDeviceMNode(new AutoCreateDeviceMNodePlan(node.getPartialPath()));
    }
    return node;
  }

  public IMNode getDeviceNodeWithAutoCreate(PartialPath path)
      throws MetadataException, IOException {
    return getDeviceNodeWithAutoCreate(path, config.isAutoCreateSchemaEnabled());
  }

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath());
    if (!isRecovering) {
      try {
        logWriter.autoCreateDeviceMNode(plan);
      } catch (IOException e) {
        throw new MetadataException(e);
      }
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
    return mtree.isPathExist(path);
  }

  /** Get metadata in Json format */
  public JsonObject getMetadataInJson() {
    return mtree.toJson();
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

  // endregion

  // region Interfaces for level Node info Query
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, MManager.StorageGroupFilter filter)
      throws MetadataException {
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

    Stream<ShowTimeSeriesResult> stream = res.stream();

    limit = plan.getLimit();
    offset = plan.getOffset();

    if (needLast) {
      stream =
          stream.sorted(
              Comparator.comparingLong(ShowTimeSeriesResult::getLastTime)
                  .reversed()
                  .thenComparing(ShowTimeSeriesResult::getName));

      // no limit
      if (limit != 0) {
        stream = stream.skip(offset).limit(limit);
      }

    } else if (limit != 0) {
      plan.setLimit(limit - res.size());
      plan.setOffset(Math.max(offset - curOffset - 1, 0));
    }

    res = stream.collect(toList());

    return res;
  }

  /**
   * Get the result of ShowTimeseriesPlan
   *
   * @param plan show time series query plan
   */
  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    List<Pair<PartialPath, String[]>> ans = mtree.getAllMeasurementSchema(plan, context);
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
    leafMNode.setAlias(alias);

    try {
      if (!isRecovering) {
        logWriter.changeAlias(path, alias);
      }
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

      TemplateManager.getInstance().checkTemplateCompatible(template, node);

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
      TemplateManager.getInstance().getTemplate(plan.getTemplateName()).unmarkStorageGroup(node);
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
  @TestOnly
  public void flushAllMlogForTest() throws IOException {
    logWriter.close();
  }
  // endregion
}
