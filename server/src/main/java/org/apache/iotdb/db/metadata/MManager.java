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

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 */
public class MManager {

  public static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";
  private static final String TAG_FORMAT = "tag key is %s, tag value is %s, tlog offset is %d";
  private static final String DEBUG_MSG = "%s : TimeSeries %s is removed from tag inverted index, ";
  private static final String DEBUG_MSG_1 = "%s: TimeSeries %s's tag info has been removed from tag inverted index ";
  private static final String PREVIOUS_CONDITION = "before deleting it, tag key is %s, tag value is %s, tlog offset is %d, contains key %b";

  private static final Logger logger = LoggerFactory.getLogger(MManager.class);

  /**
   * A thread will check whether the MTree is modified lately each such interval. Unit: second
   */
  private static final long MTREE_SNAPSHOT_THREAD_CHECK_TIME = 600L;
  private final int mtreeSnapshotInterval;
  private final long mtreeSnapshotThresholdTime;
  // the log file seriesPath
  private String logFilePath;
  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;
  private MTree mtree;
  private MLogWriter logWriter;
  private TagLogFile tagLogFile;
  private boolean isRecovering;
  // device -> DeviceMNode
  private RandomDeleteCache<PartialPath, MNode> mNodeCache;
  // tag key -> tag value -> LeafMNode
  private Map<String, Map<String, Set<MeasurementMNode>>> tagIndex = new ConcurrentHashMap<>();

  // data type -> number
  private Map<TSDataType, Integer> schemaDataTypeNumMap = new ConcurrentHashMap<>();
  private AtomicLong totalSeriesNumber = new AtomicLong();
  private boolean initialized;
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private File logFile;
  private ScheduledExecutorService timedCreateMTreeSnapshotThread;

  /**
   * threshold total size of MTree
   */
  private static final long MTREE_SIZE_THRESHOLD = config.getAllocateMemoryForSchema();

  private boolean allowToCreateNewSeries = true;

  private static final int ESTIMATED_SERIES_SIZE = config.getEstimatedSeriesSize();

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
    mtreeSnapshotPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
    mtreeSnapshotTmpPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;

    // do not write log when recover
    isRecovering = true;

    int cacheSize = config.getmManagerCacheSize();
    mNodeCache = new RandomDeleteCache<PartialPath, MNode>(cacheSize) {

      @Override
      public MNode loadObjectByKey(PartialPath key) throws CacheException {
        try {
          return mtree.getNodeByPathWithStorageGroupCheck(key);
        } catch (MetadataException e) {
          throw new CacheException(e);
        }
      }
    };

    if (config.isEnableMTreeSnapshot()) {
      timedCreateMTreeSnapshotThread = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
          "timedCreateMTreeSnapshotThread"));
      timedCreateMTreeSnapshotThread
          .scheduleAtFixedRate(this::checkMTreeModified, MTREE_SNAPSHOT_THREAD_CHECK_TIME,
              MTREE_SNAPSHOT_THREAD_CHECK_TIME, TimeUnit.SECONDS);
    }
  }

  /**
   * we should not use this function in other place, but only in IoTDB class
   */
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
      tagLogFile = new TagLogFile(config.getSchemaDir(), MetadataConstant.TAG_LOG);

      isRecovering = true;
      int lineNumber = initFromLog(logFile);

      logWriter = new MLogWriter(config.getSchemaDir(), MetadataConstant.METADATA_LOG);
      logWriter.setLineNumber(lineNumber);
      isRecovering = false;
    } catch (IOException e) {
      mtree = new MTree();
      logger.error("Cannot read MTree from file, using an empty new one", e);
    }
    initialized = true;
  }

  /**
   * @return line number of the logFile
   */
  @SuppressWarnings("squid:S3776")
  private int initFromLog(File logFile) throws IOException {
    File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
    if (tmpFile.exists()) {
      logger.warn("Creating MTree snapshot not successful before crashing...");
      Files.delete(tmpFile.toPath());
    }

    File mtreeSnapshot = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    long time = System.currentTimeMillis();
    if (!mtreeSnapshot.exists()) {
      mtree = new MTree();
    } else {
      mtree = MTree.deserializeFrom(mtreeSnapshot);
      logger.debug("spend {} ms to deserialize mtree from snapshot",
          System.currentTimeMillis() - time);
    }

    time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      int idx = 0;
      try (FileReader fr = new FileReader(logFile);
          BufferedReader br = new BufferedReader(fr)) {
        String cmd;
        while ((cmd = br.readLine()) != null) {
          try {
            operation(cmd);
            idx++;
          } catch (Exception e) {
            logger.error("Can not operate cmd {}", cmd, e);
          }
        }
      }
      logger.debug("spend {} ms to deserialize mtree from mlog.txt",
          System.currentTimeMillis() - time);
      return idx;
    } else if (mtreeSnapshot.exists()) {
      throw new IOException("mtree snapshot file exists but mlog.txt does not exist.");
    } else {
      return 0;
    }
  }

  /**
   * function for clearing MTree
   */
  public void clear() {
    try {
      this.mtree = new MTree();
      this.mNodeCache.clear();
      this.tagIndex.clear();
      this.totalSeriesNumber.set(0);
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      if (tagLogFile != null) {
        tagLogFile.close();
        tagLogFile = null;
      }
      this.schemaDataTypeNumMap.clear();
      initialized = false;
      if (config.isEnableMTreeSnapshot() && timedCreateMTreeSnapshotThread != null) {
        timedCreateMTreeSnapshotThread.shutdownNow();
        timedCreateMTreeSnapshotThread = null;
      }
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
  }

  public void operation(String cmd) throws IOException, MetadataException {
    // see createTimeseries() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",", -1);
    switch (args[0]) {
      case MetadataOperationType.CREATE_TIMESERIES:
        if (args.length > 8) {
          String[] tmpArgs = new String[8];
          tmpArgs[0] = args[0];
          int i = 1;
          tmpArgs[1] = "";
          for (; i < args.length - 7; i++) {
            tmpArgs[1] += args[i] + ",";
          }
          tmpArgs[1] += args[i++];
          for (int j = 2; j < 8; j++) {
            tmpArgs[j] = args[i++];
          }
          args = tmpArgs;
        }
        Map<String, String> props = null;
        if (!args[5].isEmpty()) {
          String[] keyValues = args[5].split("&");
          String[] kv;
          props = new HashMap<>();
          for (String keyValue : keyValues) {
            kv = keyValue.split("=");
            props.put(kv[0], kv[1]);
          }
        }

        String alias = null;
        if (!args[6].isEmpty()) {
          alias = args[6];
        }
        long offset = -1L;
        Map<String, String> tagMap = null;
        if (!args[7].isEmpty()) {
          offset = Long.parseLong(args[7]);
          tagMap = tagLogFile.readTag(config.getTagAttributeTotalSize(), offset);
        }

        CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new PartialPath(args[1]),
            TSDataType.deserialize(Short.parseShort(args[2])),
            TSEncoding.deserialize(Short.parseShort(args[3])),
            CompressionType.deserialize(Short.parseShort(args[4])), props, tagMap, null, alias);

        createTimeseries(plan, offset);
        break;
      case MetadataOperationType.DELETE_TIMESERIES:
        if (args.length > 2) {
          StringBuilder tmp = new StringBuilder();
          for (int i = 1; i < args.length - 1; i++) {
            tmp.append(args[i]).append(",");
          }
          tmp.append(args[args.length - 1]);
          args[1] = tmp.toString();
        }
        String failedTimeseries = deleteTimeseries(new PartialPath(args[1]));
        if (!failedTimeseries.isEmpty()) {
          throw new DeleteFailedException(failedTimeseries);
        }
        break;
      case MetadataOperationType.SET_STORAGE_GROUP:
        try {
          setStorageGroup(new PartialPath(args[1]));
        }
        // two time series may set one storage group concurrently,
        // that's normal in our concurrency control protocol
        catch (MetadataException e) {
          logger.info("concurrently operate set storage group cmd {} twice", cmd);
        }
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP:
        deleteStorageGroups(Collections.singletonList(new PartialPath(args[1])));
        break;
      case MetadataOperationType.SET_TTL:
        setTTL(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_OFFSET:
        changeOffset(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_ALIAS:
        changeAlias(new PartialPath(args[1]), args[2]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!allowToCreateNewSeries) {
      throw new MetadataException("IoTDB system load is too large to create timeseries, "
          + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    try {
      PartialPath path = plan.getPath();
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

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

      TSDataType type = plan.getDataType();
      // create time series in MTree
      MeasurementMNode leafMNode = mtree
          .createTimeseries(path, type, plan.getEncoding(), plan.getCompressor(),
              plan.getProps(), plan.getAlias());

      // update tag index
      if (plan.getTags() != null) {
        // tag key, tag value
        for (Entry<String, String> entry : plan.getTags().entrySet()) {
          if (entry.getKey() == null || entry.getValue() == null) {
            continue;
          }
          tagIndex.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
              .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>()).add(leafMNode);
        }
      }

      // update statistics and schemaDataTypeNumMap
      totalSeriesNumber.addAndGet(1);
      if (totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE >= MTREE_SIZE_THRESHOLD) {
        logger.warn("Current series number {} is too large...", totalSeriesNumber);
        allowToCreateNewSeries = false;
      }
      updateSchemaDataTypeNumMap(type, 1);

      // write log
      if (!isRecovering) {
        // either tags or attributes is not empty
        if ((plan.getTags() != null && !plan.getTags().isEmpty())
            || (plan.getAttributes() != null && !plan.getAttributes().isEmpty())) {
          offset = tagLogFile.write(plan.getTags(), plan.getAttributes());
        }
        logWriter.createTimeseries(plan, offset);
      }
      leafMNode.setOffset(offset);

    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path       the timeseries path
   * @param dataType   the dateType {@code DataType} of the timeseries
   * @param encoding   the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  public void createTimeseries(PartialPath path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException {
    try {
      createTimeseries(
          new CreateTimeSeriesPlan(path, dataType, encoding, compressor, props, null, null, null));
    } catch (PathAlreadyExistException | AliasAlreadyExistException e) {
      // just log it, created by multiple thread
      logger.info("Concurrent create timeseries failed, use other thread's result");
    }
  }

  /**
   * Delete all timeseries under the given path, may cross different storage group
   *
   * @param prefixPath path to be deleted, could be root or a prefix path or a full path
   * @return The String is the deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath prefixPath) throws MetadataException {
    if (isStorageGroup(prefixPath)) {
      mNodeCache.clear();
    }
    try {
      List<PartialPath> allTimeseries = mtree.getAllTimeseriesPath(prefixPath);
      // Monitor storage group seriesPath is not allowed to be deleted
      allTimeseries.removeIf(p -> p.startsWith(MonitorConstants.getStatStorageGroupPrefixArray()));

      Set<String> failedNames = new HashSet<>();
      for (PartialPath p : allTimeseries) {
        try {
          PartialPath emptyStorageGroup = deleteOneTimeseriesAndUpdateStatistics(p);
          if (!isRecovering) {
            if (emptyStorageGroup != null) {
              StorageEngine.getInstance().deleteAllDataFilesInOneStorageGroup(emptyStorageGroup);
            }
            logWriter.deleteTimeseries(p.getFullPath());
          }
        } catch (DeleteFailedException e) {
          failedNames.add(e.getName());
        }
      }
      return String.join(",", failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * remove the node from the tag inverted index
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(MeasurementMNode node) throws IOException {
    if (node.getOffset() < 0) {
      return;
    }
    Map<String, String> tagMap =
        tagLogFile.readTag(config.getTagAttributeTotalSize(), node.getOffset());
    if (tagMap != null) {
      for (Entry<String, String> entry : tagMap.entrySet()) {
        if (tagIndex.containsKey(entry.getKey()) && tagIndex.get(entry.getKey())
            .containsKey(entry.getValue())) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format(String.format(DEBUG_MSG, "Delete" + TAG_FORMAT,
                node.getFullPath()), entry.getKey(), entry.getValue(), node.getOffset()));
          }
          tagIndex.get(entry.getKey()).get(entry.getValue()).remove(node);
          if (tagIndex.get(entry.getKey()).get(entry.getValue()).isEmpty()) {
            tagIndex.get(entry.getKey()).remove(entry.getValue());
            if (tagIndex.get(entry.getKey()).isEmpty()) {
              tagIndex.remove(entry.getKey());
            }
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format(String.format(DEBUG_MSG_1, "Delete" + PREVIOUS_CONDITION,
                node.getFullPath()), entry.getKey(), entry.getValue(), node.getOffset(),
                tagIndex.containsKey(entry.getKey())));
          }
        }
      }
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return after delete if the storage group is empty, return its path, otherwise return null
   */
  private PartialPath deleteOneTimeseriesAndUpdateStatistics(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, MeasurementMNode> pair = mtree
        .deleteTimeseriesAndReturnEmptyStorageGroup(path);
    removeFromTagInvertedIndex(pair.right);
    PartialPath storageGroupPath = pair.left;

    // update statistics in schemaDataTypeNumMap
    updateSchemaDataTypeNumMap(pair.right.getSchema().getType(), -1);

    // TODO: delete the path node and all its ancestors
    mNodeCache.clear();
    totalSeriesNumber.addAndGet(-1);
    if (!allowToCreateNewSeries &&
        totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
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
        logWriter.setStorageGroup(storageGroup.getFullPath());
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
        if (!allowToCreateNewSeries &&
            totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
          logger.info("Current series number {} come back to normal level", totalSeriesNumber);
          allowToCreateNewSeries = true;
        }
        mNodeCache.clear();

        // try to delete storage group
        List<MeasurementMNode> leafMNodes = mtree.deleteStorageGroup(storageGroup);
        for (MeasurementMNode leafMNode : leafMNodes) {
          removeFromTagInvertedIndex(leafMNode);
          // update statistics in schemaDataTypeNumMap
          updateSchemaDataTypeNumMap(leafMNode.getSchema().getType(), -1);
        }

        if (!config.isEnableMemControl()) {
          MemTableManager.getInstance().addOrDeleteStorageGroup(-1);
        }

        // if success
        if (!isRecovering) {
          logWriter.deleteStorageGroup(storageGroup.getFullPath());
        }
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * update statistics in schemaDataTypeNumMap
   *
   * @param type data type
   * @param num  1 for creating timeseries and -1 for deleting timeseries
   */
  private synchronized void updateSchemaDataTypeNumMap(TSDataType type, int num) {
    // add an array of the series type
    schemaDataTypeNumMap.put(type, schemaDataTypeNumMap.getOrDefault(type, 0) + num);
    // add an array of time
    schemaDataTypeNumMap.put(TSDataType.INT64,
        schemaDataTypeNumMap.getOrDefault(TSDataType.INT64, 0) + num);

    PrimitiveArrayManager.updateSchemaDataTypeNum(schemaDataTypeNumMap, totalSeriesNumber.get());
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

    return mtree.getSchema(path).getType();
  }

  public MeasurementMNode[] getMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    MNode deviceNode = getNodeByPath(deviceId);
    MeasurementMNode[] mNodes = new MeasurementMNode[measurements.length];
    for (int i = 0; i < mNodes.length; i++) {
      mNodes[i] = ((MeasurementMNode) deviceNode.getChild(measurements[i]));
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
   *                   wildcard can only match one level, otherwise it can match to the tail.
   * @return A HashSet instance which stores devices paths with given prefixPath.
   */
  public Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevices(prefixPath);
  }

  /**
   * Get all nodes from the given level
   *
   * @param prefixPath can be a prefix of a full path. Can not be a full path. can not have
   *                   wildcard. But, the level of the prefixPath can be smaller than the given
   *                   level, e.g., prefixPath = root.a while the given level is 5
   * @param nodeLevel  the level can not be smaller than the level of the prefixPath
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesList(PartialPath prefixPath, int nodeLevel)
      throws MetadataException {
    return getNodesList(prefixPath, nodeLevel, null);
  }

  public List<PartialPath> getNodesList(PartialPath prefixPath, int nodeLevel,
      StorageGroupFilter filter)
      throws MetadataException {
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

  /**
   * Get all storage group paths
   */
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
   *                   wildcard can only match one level, otherwise it can match to the tail.
   * @return A ArrayList instance which stores storage group paths with given prefixPath.
   */
  public List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupPaths(prefixPath);
  }

  /**
   * Get all storage group MNodes
   */
  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  /**
   * Return all paths for given path if the path is abstract. Or return the path itself. Regular
   * expression in this method is formed by the amalgamation of seriesPath and the character '*'.
   *
   * @param prefixPath can be a prefix or a full path. if the wildcard is not at the tail, then each
   *                   wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesPath(prefixPath);
  }

  /**
   * Similar to method getAllTimeseriesPath(), but return Path with alias alias.
   */
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(PartialPath prefixPath,
      int limit, int offset) throws MetadataException {
    return mtree.getAllTimeseriesPathWithAlias(prefixPath, limit, offset);
  }

  /**
   * To calculate the count of timeseries for given prefix path.
   */
  public int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesCount(prefixPath);
  }

  /**
   * To calculate the count of devices for given prefix path.
   */
  public int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevicesNum(prefixPath);
  }

  /**
   * To calculate the count of storage group for given prefix path.
   */
  public int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupNum(prefixPath);
  }

  /**
   * To calculate the count of nodes in the given level for given prefix path.
   *
   * @param prefixPath a prefix path or a full path, can not contain '*'
   * @param level      the level can not be smaller than the level of the prefixPath
   */
  public int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    return mtree.getNodesCountInGivenLevel(prefixPath, level);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private List<ShowTimeSeriesResult> showTimeseriesWithIndex(ShowTimeSeriesPlan plan,
      QueryContext context) throws MetadataException {
    if (!tagIndex.containsKey(plan.getKey())) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.");
    }
    Map<String, Set<MeasurementMNode>> value2Node = tagIndex.get(plan.getKey());
    if (value2Node.isEmpty()) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.");
    }

    List<MeasurementMNode> allMatchedNodes = new ArrayList<>();
    if (plan.isContains()) {
      for (Entry<String, Set<MeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (tagValue.contains(plan.getValue())) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    } else {
      for (Entry<String, Set<MeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (plan.getValue().equals(tagValue)) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    }

    // if ordered by heat, we sort all the timeseries by the descending order of the last insert timestamp
    if (plan.isOrderByHeat()) {
      List<StorageGroupProcessor> list;
      try {
        list = StorageEngine.getInstance()
            .mergeLock(allMatchedNodes.stream().map(MNode::getPartialPath).collect(toList()));
        try {
          allMatchedNodes = allMatchedNodes.stream().sorted(Comparator
              .comparingLong((MeasurementMNode mNode) -> MTree.getLastTimeStamp(mNode, context))
              .reversed().thenComparing(MNode::getFullPath)).collect(toList());
        } finally {
          StorageEngine.getInstance().mergeUnLock(list);
        }
      } catch (StorageEngineException e) {
        throw new MetadataException(e);
      }
    } else {
      // otherwise, we just sort them by the alphabetical order
      allMatchedNodes = allMatchedNodes.stream().sorted(Comparator.comparing(MNode::getFullPath))
          .collect(toList());
    }

    List<ShowTimeSeriesResult> res = new LinkedList<>();
    String[] prefixNodes = plan.getPath().getNodes();
    int curOffset = -1;
    int count = 0;
    int limit = plan.getLimit();
    int offset = plan.getOffset();
    for (MeasurementMNode leaf : allMatchedNodes) {
      if (match(leaf.getPartialPath(), prefixNodes)) {
        if (limit != 0 || offset != 0) {
          curOffset++;
          if (curOffset < offset || count == limit) {
            continue;
          }
        }
        try {
          Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
              tagLogFile.read(config.getTagAttributeTotalSize(), leaf.getOffset());
          MeasurementSchema measurementSchema = leaf.getSchema();
          res.add(new ShowTimeSeriesResult(leaf.getFullPath(), leaf.getAlias(),
              getStorageGroupPath(leaf.getPartialPath()).getFullPath(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getCompressor(), tagAndAttributePair.left,
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
   * whether the full path has the prefixNodes
   */
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

  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    // show timeseries with index
    if (plan.getKey() != null && plan.getValue() != null) {
      return showTimeseriesWithIndex(plan, context);
    } else {
      return showTimeseriesWithoutIndex(plan, context);
    }
  }

  /**
   * Get the result of ShowTimeseriesPlan
   *
   * @param plan show time series query plan
   */
  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(ShowTimeSeriesPlan plan,
      QueryContext context) throws MetadataException {
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
          tagAndAttributePair = tagLogFile.read(config.getTagAttributeTotalSize(), tagFileOffset);
        }
        res.add(new ShowTimeSeriesResult(ansString.left.getFullPath(), ansString.right[0],
            ansString.right[1],
            TSDataType.valueOf(ansString.right[2]), TSEncoding.valueOf(ansString.right[3]),
            CompressionType.valueOf(ansString.right[4]), tagAndAttributePair.left,
            tagAndAttributePair.right));
      } catch (IOException e) {
        throw new MetadataException(
            "Something went wrong while deserialize tag info of " + ansString.left.getFullPath(),
            e);
      }
    }
    return res;

  }

  protected MeasurementMNode getMeasurementMNode(MNode deviceMNode, String measurement) {
    return (MeasurementMNode) deviceMNode.getChild(measurement);
  }

  public MeasurementSchema getSeriesSchema(PartialPath device, String measurement)
      throws MetadataException {
    try {
      MNode node = mtree.getNodeByPath(device);
      MNode leaf = node.getChild(measurement);
      if (leaf != null) {
        return ((MeasurementMNode) leaf).getSchema();
      }
      return null;
    } catch (PathNotExistException | IllegalPathException e) {
      //do nothing and throw it directly.
      throw e;
    }
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    return mtree.getChildNodePathInNextLevel(path);
  }

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    return mtree.isPathExist(path);
  }

  /**
   * Get node by path
   */
  public MNode getNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getNodeByPath(path);
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node by path. If storage group is not
   * set, StorageGroupNotSetException will be thrown
   */
  public StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return mtree.getStorageGroupNodeByStorageGroupPath(path);
  }

  /**
   * Get storage group node by path. the give path don't need to be storage group path.
   */
  public StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getStorageGroupNodeByPath(path);
  }

  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true <p>
   * (we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param path path
   */
  public MNode getDeviceNodeWithAutoCreate(
      PartialPath path, boolean autoCreateSchema, int sgLevel) throws MetadataException {
    MNode node;
    boolean shouldSetStorageGroup;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (CacheException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path.getFullPath());
      }
    }

    try {
      try {
        node = mNodeCache.get(path);
        return node;
      } catch (CacheException e) {
        shouldSetStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
      }

      if (shouldSetStorageGroup) {
        PartialPath storageGroupPath = MetaUtils.getStorageGroupPathByLevel(path, sgLevel);
        setStorageGroup(storageGroupPath);
      }
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      return node;
    } catch (StorageGroupAlreadySetException e) {
      // ignore set storage group concurrently
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      return node;
    }
  }

  /**
   * !!!!!!Attention!!!!! must call the return node's readUnlock() if you call this method.
   */
  public MNode getDeviceNodeWithAutoCreate(PartialPath path) throws MetadataException {
    return getDeviceNodeWithAutoCreate(
        path, config.isAutoCreateSchemaEnabled(), config.getDefaultStorageGroupLevel());
  }

  public MNode getDeviceNode(PartialPath path) throws MetadataException {
    MNode node;
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
      MNode deviceNode = getDeviceNode(path);
      device = deviceNode.getFullPath();
    } catch (MetadataException | NullPointerException e) {
      // Cannot get deviceId from MManager, return the input deviceId
    }
    return device;
  }

  /**
   * Get metadata in string
   */
  public String getMetadataInString() {
    return TIME_SERIES_TREE_HEADER + mtree.toString();
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
    if (!isRecovering) {
      logWriter.setTTL(storageGroup.getFullPath(), dataTTL);
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
   * @param path   timeseries
   * @param offset offset in the tag file
   */
  public void changeOffset(PartialPath path, long offset) throws MetadataException {
    ((MeasurementMNode) mtree.getNodeByPath(path)).setOffset(offset);
  }

  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    MeasurementMNode leafMNode = (MeasurementMNode) mtree.getNodeByPath(path);
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
   * @param alias         newly added alias
   * @param tagsMap       newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath      timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upsertTagsAndAttributes(String alias, Map<String, String> tagsMap,
      Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
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
      logWriter.changeAlias(fullPath.getFullPath(), alias);
    }

    if (tagsMap == null && attributesMap == null) {
      return;
    }
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(tagsMap, attributesMap);
      logWriter.changeOffset(fullPath.getFullPath(), offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      if (tagsMap != null) {
        for (Entry<String, String> entry : tagsMap.entrySet()) {
          tagIndex.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
              .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>()).add(leafMNode);
        }
      }
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    if (tagsMap != null) {
      for (Entry<String, String> entry : tagsMap.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        String beforeValue = pair.left.get(key);
        pair.left.put(key, value);
        // if the key has existed and the value is not equal to the new one
        // we should remove before key-value from inverted index map
        if (beforeValue != null && !beforeValue.equals(value)) {

          if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {
            if (logger.isDebugEnabled()) {
              logger.debug(String.format(
                  DEBUG_MSG, "Upsert"
                      + TAG_FORMAT,
                  leafMNode.getFullPath(), key, beforeValue, leafMNode.getOffset()));
            }

            tagIndex.get(key).get(beforeValue).remove(leafMNode);
            if (tagIndex.get(key).get(beforeValue).isEmpty()) {
              tagIndex.get(key).remove(beforeValue);
            }
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(String.format(
                  DEBUG_MSG_1, "Upsert"
                      + PREVIOUS_CONDITION,
                  leafMNode.getFullPath(), key, beforeValue, leafMNode.getOffset(),
                  tagIndex.containsKey(key)));
            }
          }
        }

        // if the key doesn't exist or the value is not equal to the new one
        // we should add a new key-value to inverted index map
        if (beforeValue == null || !beforeValue.equals(value)) {
          tagIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
              .computeIfAbsent(value, v -> new CopyOnWriteArraySet<>()).add(leafMNode);
        }
      }
    }

    if (attributesMap != null) {
      pair.right.putAll(attributesMap);
    }


    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath      timeseries
   */
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(Collections.emptyMap(), attributesMap);
      logWriter.changeOffset(fullPath.getFullPath(), offset);
      leafMNode.setOffset(offset);
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    for (Entry<String, String> entry : attributesMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.right.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the attribute [%s].", fullPath, key));
      }
      pair.right.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * add new tags key-value for the timeseries
   *
   * @param tagsMap  newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(tagsMap, Collections.emptyMap());
      logWriter.changeOffset(fullPath.getFullPath(), offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      for (Entry<String, String> entry : tagsMap.entrySet()) {
        tagIndex.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
            .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>()).add(leafMNode);
      }
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    for (Entry<String, String> entry : tagsMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.left.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the tag [%s].", fullPath, key));
      }
      pair.left.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    // update tag inverted map
    tagsMap.forEach((key, value) -> tagIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(value, v -> new CopyOnWriteArraySet<>()).add(leafMNode));
  }

  /**
   * drop tags or attributes of the timeseries
   *
   * @param keySet   tags key or attributes key
   * @param fullPath timeseries path
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, just do nothing.
    if (leafMNode.getOffset() < 0) {
      return;
    }
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    Map<String, String> deleteTag = new HashMap<>();
    for (String key : keySet) {
      // check tag map
      // check attribute map
      if (pair.left.containsKey(key)) {
        deleteTag.put(key, pair.left.remove(key));
      } else {
        pair.right.remove(key);
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    for (Entry<String, String> entry : deleteTag.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // change the tag inverted index map
      if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(value)) {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG, "Drop"
                  + TAG_FORMAT,
              leafMNode.getFullPath(), entry.getKey(), entry.getValue(), leafMNode.getOffset()));
        }

        tagIndex.get(key).get(value).remove(leafMNode);
        if (tagIndex.get(key).get(value).isEmpty()) {
          tagIndex.get(key).remove(value);
          if (tagIndex.get(key).isEmpty()) {
            tagIndex.remove(key);
          }
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG_1, "Drop"
                  + PREVIOUS_CONDITION,
              leafMNode.getFullPath(), key, value, leafMNode.getOffset(),
              tagIndex.containsKey(key)));
        }
      }

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
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
    }

    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());
    Map<String, String> oldTagValue = new HashMap<>();
    Map<String, String> newTagValue = new HashMap<>();

    for (Entry<String, String> entry : alterMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // check tag map
      if (pair.left.containsKey(key)) {
        oldTagValue.put(key, pair.left.get(key));
        newTagValue.put(key, value);
        pair.left.put(key, value);
      } else if (pair.right.containsKey(key)) {
        // check attribute map
        pair.right.put(key, value);
      } else {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, key));
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    for (Entry<String, String> entry : oldTagValue.entrySet()) {
      String key = entry.getKey();
      String beforeValue = entry.getValue();
      String currentValue = newTagValue.get(key);
      // change the tag inverted index map
      if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {

        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG, "Set"
                  + TAG_FORMAT,
              leafMNode.getFullPath(), entry.getKey(), beforeValue, leafMNode.getOffset()));
        }

        tagIndex.get(key).get(beforeValue).remove(leafMNode);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG_1, "Set"
                  + PREVIOUS_CONDITION,
              leafMNode.getFullPath(), key, beforeValue, leafMNode.getOffset(),
              tagIndex.containsKey(key)));
        }
      }
      tagIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(currentValue, k -> new CopyOnWriteArraySet<>()).add(leafMNode);
    }
  }

  /**
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey   old key of tag or attribute
   * @param newKey   new key of tag or attribute
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey));
    }
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    // current name has existed
    if (pair.left.containsKey(newKey) || pair.right.containsKey(newKey)) {
      throw new MetadataException(
          String.format(
              "TimeSeries [%s] already has a tag/attribute named [%s].", fullPath, newKey));
    }

    // check tag map
    if (pair.left.containsKey(oldKey)) {
      String value = pair.left.remove(oldKey);
      pair.left.put(newKey, value);
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
      // change the tag inverted index map
      if (tagIndex.containsKey(oldKey) && tagIndex.get(oldKey).containsKey(value)) {

        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG, "Rename"
                  + TAG_FORMAT,
              leafMNode.getFullPath(), oldKey, value, leafMNode.getOffset()));
        }

        tagIndex.get(oldKey).get(value).remove(leafMNode);

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format(
              DEBUG_MSG_1, "Rename"
                  + PREVIOUS_CONDITION,
              leafMNode.getFullPath(), oldKey, value, leafMNode.getOffset(),
              tagIndex.containsKey(oldKey)));
        }
      }
      tagIndex.computeIfAbsent(newKey, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(value, k -> new CopyOnWriteArraySet<>()).add(leafMNode);
    } else if (pair.right.containsKey(oldKey)) {
      // check attribute map
      pair.right.put(newKey, pair.right.remove(oldKey));
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
    } else {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, oldKey));
    }
  }

  /**
   * Check whether the given path contains a storage group
   */
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

  public void collectTimeseriesSchema(MNode startingNode,
      Collection<TimeseriesSchema> timeseriesSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node instanceof MeasurementMNode) {
        MeasurementSchema nodeSchema = ((MeasurementMNode) node).getSchema();
        timeseriesSchemas.add(new TimeseriesSchema(node.getFullPath(), nodeSchema.getType(),
            nodeSchema.getEncodingType(), nodeSchema.getCompressor()));
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  public void collectTimeseriesSchema(String prefixPath,
      Collection<TimeseriesSchema> timeseriesSchemas) throws MetadataException {
    collectTimeseriesSchema(getNodeByPath(new PartialPath(prefixPath)), timeseriesSchemas);
  }

  public void collectMeasurementSchema(MNode startingNode,
      Collection<MeasurementSchema> measurementSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node instanceof MeasurementMNode) {
        MeasurementSchema nodeSchema = ((MeasurementMNode) node).getSchema();
        measurementSchemas.add(new MeasurementSchema(node.getName(), nodeSchema.getType(),
            nodeSchema.getEncodingType(), nodeSchema.getCompressor()));
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  /**
   * Collect the timeseries schemas under "startingPath".
   */
  public void collectSeries(PartialPath startingPath, List<MeasurementSchema> measurementSchemas) {
    MNode mNode;
    try {
      mNode = getNodeByPath(startingPath);
    } catch (MetadataException e) {
      return;
    }
    collectMeasurementSchema(mNode, measurementSchemas);
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

    return mtree.determineStorageGroup(path);

  }

  /**
   * if the path is in local mtree, nothing needed to do (because mtree is in the memory); Otherwise
   * cache the path to mRemoteSchemaCache
   */
  public void cacheMeta(PartialPath path, MeasurementMeta meta) {
    // do nothing
  }

  public void updateLastCache(PartialPath seriesPath, TimeValuePair timeValuePair,
      boolean highPriorityUpdate, Long latestFlushedTime,
      MeasurementMNode node) {
    if (node != null) {
      node.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    } else {
      try {
        MeasurementMNode node1 = (MeasurementMNode) mtree.getNodeByPath(seriesPath);
        node1.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
      } catch (MetadataException e) {
        logger.warn("failed to update last cache for the {}, err:{}", seriesPath, e.getMessage());
      }
    }
  }

  public TimeValuePair getLastCache(PartialPath seriesPath) {
    try {
      MeasurementMNode node = (MeasurementMNode) mtree.getNodeByPath(seriesPath);
      return node.getCachedLast();
    } catch (MetadataException e) {
      logger.warn("failed to get last cache for the {}, err:{}", seriesPath, e.getMessage());
    }
    return null;
  }

  private void checkMTreeModified() {
    if (logWriter == null || logFile == null) {
      // the logWriter is not initialized now, we skip the check once.
      return;
    }
    if (System.currentTimeMillis() - logFile.lastModified() < mtreeSnapshotThresholdTime) {
      if (logger.isDebugEnabled()) {
        logger.debug("MTree snapshot need not be created. Time from last modification: {} ms.",
            System.currentTimeMillis() - logFile.lastModified());
      }
    } else if (logWriter.getLineNumber() < mtreeSnapshotInterval) {
      if (logger.isDebugEnabled()) {
        logger.debug("MTree snapshot need not be created. New mlog line number: {}.",
            logWriter.getLineNumber());
      }
    } else {
      logger.info("New mlog line number: {}, time from last modification: {} ms",
          logWriter.getLineNumber(), System.currentTimeMillis() - logFile.lastModified());
      createMTreeSnapshot();
    }
  }

  public void createMTreeSnapshot() {
    long time = System.currentTimeMillis();
    logger.info("Start creating MTree snapshot to {}", mtreeSnapshotPath);
    try {
      mtree.serializeTo(mtreeSnapshotTmpPath);
      File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
      File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
      if (snapshotFile.exists()) {
        Files.delete(snapshotFile.toPath());
      }
      if (tmpFile.renameTo(snapshotFile)) {
        logger.info("Finish creating MTree snapshot to {}, spend {} ms.", mtreeSnapshotPath,
            System.currentTimeMillis() - time);
      }
      logWriter.clear();
    } catch (IOException e) {
      logger.warn("Failed to create MTree snapshot to {}", mtreeSnapshotPath, e);
      if (SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).exists()) {
        try {
          Files.delete(SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).toPath());
        } catch (IOException e1) {
          logger.warn("delete file {} failed: {}", mtreeSnapshotTmpPath, e1.getMessage());
        }
      }
    }
  }

  /**
   * get schema for device. Attention!!!  Only support insertPlan
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public MNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException {

    PartialPath deviceId = plan.getDeviceId();
    String[] measurementList = plan.getMeasurements();
    MeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device node
    MNode deviceMNode = getDeviceNodeWithAutoCreate(deviceId);

    // 2. get schema of each measurement
    for (int i = 0; i < measurementList.length; i++) {
      try {
        // if do not has measurement
        MeasurementMNode measurementMNode;
        if (!deviceMNode.hasChild(measurementList[i])) {
          // could not create it
          if (!config.isAutoCreateSchemaEnabled()) {
            // but measurement not in MTree and cannot auto-create, try the cache
            measurementMNode = getMeasurementMNode(deviceMNode, measurementList[i]);
            if (measurementMNode == null) {
              throw new PathNotExistException(deviceId + PATH_SEPARATOR + measurementList[i]);
            }
          } else {
            // create it

            TSDataType dataType = getTypeInLoc(plan, i);
            // create it, may concurrent created by multiple thread
            internalCreateTimeseries(deviceId.concatNode(measurementList[i]), dataType);
            measurementMNode = (MeasurementMNode) deviceMNode.getChild(measurementList[i]);
          }
        } else {
          measurementMNode = getMeasurementMNode(deviceMNode, measurementList[i]);
        }

        // check type is match
        TSDataType insertDataType = null;
        if (plan instanceof InsertRowPlan) {
          if (!((InsertRowPlan) plan).isNeedInferType()) {
            // only when InsertRowPlan's values is object[], we should check type
            insertDataType = getTypeInLoc(plan, i);
          } else {
            insertDataType = measurementMNode.getSchema().getType();
          }
        } else if (plan instanceof InsertTabletPlan) {
          insertDataType = getTypeInLoc(plan, i);
        }

        if (measurementMNode.getSchema().getType() != insertDataType) {
          logger.warn("DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
              measurementList[i], insertDataType, measurementMNode.getSchema().getType());
          DataTypeMismatchException mismatchException = new DataTypeMismatchException(
              measurementList[i],
              insertDataType, measurementMNode.getSchema().getType());
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

      } catch (MetadataException e) {
        logger.warn("meet error when check {}.{}, message: {}", deviceId, measurementList[i],
            e.getMessage());
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

  /**
   * create timeseries with ignore PathAlreadyExistException
   */
  private void internalCreateTimeseries(PartialPath path, TSDataType dataType)
      throws MetadataException {
    try {
      createTimeseries(
          path,
          dataType,
          getDefaultEncoding(dataType),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    } catch (PathAlreadyExistException | AliasAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore PathAlreadyExistException and AliasAlreadyExistException when Concurrent inserting"
                + " a non-exist time series {}", path);
      }
    }
  }

  /**
   * Get default encoding by dataType
   */
  private TSEncoding getDefaultEncoding(TSDataType dataType) {
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    switch (dataType) {
      case BOOLEAN:
        return conf.getDefaultBooleanEncoding();
      case INT32:
        return conf.getDefaultInt32Encoding();
      case INT64:
        return conf.getDefaultInt64Encoding();
      case FLOAT:
        return conf.getDefaultFloatEncoding();
      case DOUBLE:
        return conf.getDefaultDoubleEncoding();
      case TEXT:
        return conf.getDefaultTextEncoding();
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType.toString()));
    }
  }

  /**
   * get dataType of plan, in loc measurements only support InsertRowPlan and InsertTabletPlan
   */
  private TSDataType getTypeInLoc(InsertPlan plan, int loc) throws MetadataException {
    TSDataType dataType;
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      dataType = TypeInferenceUtils
          .getPredictedDataType(tPlan.getValues()[loc], tPlan.isNeedInferType());
    } else if (plan instanceof InsertTabletPlan) {
      dataType = (plan).getDataTypes()[loc];
    } else {
      throw new MetadataException(String.format(
          "Only support insert and insertTablet, plan is [%s]", plan.getOperatorType()));
    }
    return dataType;
  }

  /**
   * StorageGroupFilter filters unsatisfied storage groups in metadata queries to speed up and
   * deduplicate.
   */
  @FunctionalInterface
  public interface StorageGroupFilter {

    boolean satisfy(String storageGroup);
  }

  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }
}
