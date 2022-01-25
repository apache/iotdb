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
package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.metadata.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.CollectorTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.StorageGroupCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MNodeLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementGroupByLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.StorageGroupCounter;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.lastCache.LastCacheManager.getLastTimeStamp;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>MTree initialization, clear and serialization
 *   <li>Timeseries operation, including create and delete
 *   <li>Entity/Device operation
 *   <li>StorageGroup Operation, including set and delete
 *   <li>Interfaces and Implementation for metadata info Query
 *       <ol>
 *         <li>Interfaces for Storage Group info Query
 *         <li>Interfaces for Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *         <li>Interfaces for Level Node info Query
 *         <li>Interfaces and Implementation for metadata count
 *       </ol>
 *   <li>Interfaces and Implementation for MNode Query
 *   <li>Interfaces and Implementation for Template check
 *   <li>TestOnly Interface
 * </ol>
 */
public class MTree implements Serializable {

  public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final long serialVersionUID = -4200394435237291964L;
  private static final Logger logger = LoggerFactory.getLogger(MTree.class);
  private IMNode root;

  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;

  // region MTree initialization, clear and serialization
  public MTree() {
    this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
  }

  private MTree(InternalMNode root) {
    this.root = root;
  }

  public void init() throws IOException {
    mtreeSnapshotPath =
        IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
            + File.separator
            + MetadataConstant.MTREE_SNAPSHOT;
    mtreeSnapshotTmpPath =
        IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
            + File.separator
            + MetadataConstant.MTREE_SNAPSHOT_TMP;

    File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
    if (tmpFile.exists()) {
      logger.warn("Creating MTree snapshot not successful before crashing...");
      Files.delete(tmpFile.toPath());
    }

    File mtreeSnapshot = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    long time = System.currentTimeMillis();
    if (mtreeSnapshot.exists()) {
      this.root = deserializeFrom(mtreeSnapshot).root;
      logger.debug(
          "spend {} ms to deserialize mtree from snapshot", System.currentTimeMillis() - time);
    }
  }

  public void clear() {
    root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
  }

  public void createSnapshot() throws IOException {
    long time = System.currentTimeMillis();
    logger.info("Start creating MTree snapshot to {}", mtreeSnapshotPath);
    try {
      serializeTo(mtreeSnapshotTmpPath);
      File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
      File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
      if (snapshotFile.exists()) {
        Files.delete(snapshotFile.toPath());
      }
      if (tmpFile.renameTo(snapshotFile)) {
        logger.info(
            "Finish creating MTree snapshot to {}, spend {} ms.",
            mtreeSnapshotPath,
            System.currentTimeMillis() - time);
      }
    } catch (IOException e) {
      logger.warn("Failed to create MTree snapshot to {}", mtreeSnapshotPath, e);
      if (SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).exists()) {
        try {
          Files.delete(SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).toPath());
        } catch (IOException e1) {
          logger.warn("delete file {} failed: {}", mtreeSnapshotTmpPath, e1.getMessage());
        }
      }
      throw e;
    }
  }

  private static String jsonToString(JsonObject jsonObject) {
    return GSON.toJson(jsonObject);
  }

  public void serializeTo(String snapshotPath) throws IOException {
    try (MLogWriter mLogWriter = new MLogWriter(snapshotPath)) {
      root.serializeTo(mLogWriter);
    }
  }

  public static MTree deserializeFrom(File mtreeSnapshot) {
    try (MLogReader mLogReader = new MLogReader(mtreeSnapshot)) {
      return new MTree(deserializeFromReader(mLogReader));
    } catch (IOException e) {
      logger.warn("Failed to deserialize from {}. Use a new MTree.", mtreeSnapshot.getPath());
      return new MTree();
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static InternalMNode deserializeFromReader(MLogReader mLogReader) {
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    IMNode node = null;
    while (mLogReader.hasNext()) {
      PhysicalPlan plan = null;
      try {
        plan = mLogReader.next();
        if (plan == null) {
          continue;
        }
        int childrenSize = 0;
        if (plan instanceof StorageGroupMNodePlan) {
          node = StorageGroupMNode.deserializeFrom((StorageGroupMNodePlan) plan);
          childrenSize = ((StorageGroupMNodePlan) plan).getChildSize();
          if (!IoTDBDescriptor.getInstance().getConfig().isEnableMemControl()) {
            MemTableManager.getInstance().addOrDeleteStorageGroup(1);
          }
        } else if (plan instanceof MeasurementMNodePlan) {
          node = MeasurementMNode.deserializeFrom((MeasurementMNodePlan) plan);
          childrenSize = ((MeasurementMNodePlan) plan).getChildSize();
        } else if (plan instanceof MNodePlan) {
          node = InternalMNode.deserializeFrom((MNodePlan) plan);
          childrenSize = ((MNodePlan) plan).getChildSize();
        }

        if (childrenSize != 0) {
          ConcurrentHashMap<String, IMNode> childrenMap = new ConcurrentHashMap<>();
          for (int i = 0; i < childrenSize; i++) {
            IMNode child = nodeStack.removeFirst();
            childrenMap.put(child.getName(), child);
            if (child.isMeasurement()) {
              if (!node.isEntity()) {
                node = MNodeUtils.setToEntity(node);
              }
              String alias = child.getAsMeasurementMNode().getAlias();
              if (alias != null) {
                node.getAsEntityMNode().addAlias(alias, child.getAsMeasurementMNode());
              }
            }
            child.setParent(node);
          }
          node.setChildren(childrenMap);
        }
        nodeStack.push(node);
      } catch (Exception e) {
        logger.error(
            "Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(), e);
      }
    }
    if (!IoTDBConstant.PATH_ROOT.equals(node.getName())) {
      logger.error("Snapshot file corrupted!");
      //      throw new MetadataException("Snapshot file corrupted!");
    }

    return (InternalMNode) node;
  }

  @Override
  public String toString() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(root.getName(), mNodeToJSON(root, null));
    return jsonToString(jsonObject);
  }

  private JsonObject mNodeToJSON(IMNode node, String storageGroupName) {
    JsonObject jsonObject = new JsonObject();
    if (node.getChildren().size() > 0) {
      if (node.isStorageGroup()) {
        storageGroupName = node.getFullPath();
      }
      for (IMNode child : node.getChildren().values()) {
        jsonObject.add(child.getName(), mNodeToJSON(child, storageGroupName));
      }
    } else if (node.isMeasurement()) {
      IMeasurementMNode leafMNode = node.getAsMeasurementMNode();
      jsonObject.add("DataType", GSON.toJsonTree(leafMNode.getSchema().getType()));
      jsonObject.add("Encoding", GSON.toJsonTree(leafMNode.getSchema().getEncodingType()));
      jsonObject.add("Compressor", GSON.toJsonTree(leafMNode.getSchema().getCompressor()));
      if (leafMNode.getSchema().getProps() != null) {
        jsonObject.addProperty("args", leafMNode.getSchema().getProps().toString());
      }
      jsonObject.addProperty("StorageGroup", storageGroupName);
    }
    return jsonObject;
  }
  // endregion

  // region Timeseries operation, including create and delete
  /**
   * Create a timeseries with a full path from root to leaf node. Before creating a timeseries, the
   * storage group should be set first, throw exception otherwise
   *
   * @param path timeseries path
   * @param dataType data type
   * @param encoding encoding
   * @param compressor compressor
   * @param props props
   * @param alias alias of measurement
   */
  public IMeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(path);
    IMNode cur = root;
    boolean hasSetStorageGroup = false;
    Template upperTemplate = cur.getSchemaTemplate();
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length - 1; i++) {
      if (cur.isMeasurement()) {
        throw new PathAlreadyExistException(cur.getFullPath());
      }
      if (cur.isStorageGroup()) {
        hasSetStorageGroup = true;
      }
      String childName = nodeNames[i];

      // even template not in use, measurement path shall not be conflict with MTree
      if (upperTemplate != null && upperTemplate.getDirectNode(childName) != null) {
        throw new TemplateImcompatibeException(
            path.getFullPath(), upperTemplate.getName(), childName);
      }

      if (!cur.hasChild(childName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }

        cur.addChild(childName, new InternalMNode(cur, childName));
      }
      cur = cur.getChild(childName);

      if (cur.getSchemaTemplate() != null) {
        upperTemplate = cur.getSchemaTemplate();
      }
    }

    if (cur.isMeasurement()) {
      throw new PathAlreadyExistException(cur.getFullPath());
    }

    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

    String leafName = path.getMeasurement();

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      if (cur.hasChild(leafName)) {
        throw new PathAlreadyExistException(path.getFullPath());
      }

      if (alias != null && cur.hasChild(alias)) {
        throw new AliasAlreadyExistException(path.getFullPath(), alias);
      }

      if (upperTemplate != null
          && (upperTemplate.getDirectNode(leafName) != null
              || upperTemplate.getDirectNode(alias) != null)) {
        throw new TemplateImcompatibeException(path.getFullPath(), upperTemplate.getName());
      }

      if (cur.isEntity() && cur.getAsEntityMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
            cur.getFullPath());
      }

      IEntityMNode entityMNode = MNodeUtils.setToEntity(cur);

      IMeasurementMNode measurementMNode =
          MeasurementMNode.getMeasurementMNode(
              entityMNode,
              leafName,
              new MeasurementSchema(leafName, dataType, encoding, compressor, props),
              alias);
      entityMNode.addChild(leafName, measurementMNode);
      // link alias to LeafMNode
      if (alias != null) {
        entityMNode.addAlias(alias, measurementMNode);
      }
      return measurementMNode;
    }
  }

  /**
   * Create aligned timeseries with full paths from root to one leaf node. Before creating
   * timeseries, the * storage group should be set first, throw exception otherwise
   *
   * @param devicePath device path
   * @param measurements measurements list
   * @param dataTypes data types list
   * @param encodings encodings list
   * @param compressors compressor
   */
  public void createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    Pair<IMNode, Template> pair = checkAndAutoCreateInternalPath(devicePath);
    IMNode cur = pair.left;
    Template upperTemplate = pair.right;

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      for (String measurement : measurements) {
        if (cur.hasChild(measurement)) {
          throw new PathAlreadyExistException(devicePath.getFullPath() + "." + measurement);
        }
      }

      if (upperTemplate != null) {
        for (String measurement : measurements) {
          if (upperTemplate.getDirectNode(measurement) != null) {
            throw new TemplateImcompatibeException(
                devicePath.concatNode(measurement).getFullPath(), upperTemplate.getName());
          }
        }
      }

      if (cur.isEntity() && !cur.getAsEntityMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is not aligned, please use createTimeseries or change entity.",
            devicePath.getFullPath());
      }

      IEntityMNode entityMNode = MNodeUtils.setToEntity(cur);
      entityMNode.setAligned(true);

      for (int i = 0; i < measurements.size(); i++) {
        IMeasurementMNode measurementMNode =
            MeasurementMNode.getMeasurementMNode(
                entityMNode,
                measurements.get(i),
                new MeasurementSchema(
                    measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i)),
                null);
        entityMNode.addChild(measurements.get(i), measurementMNode);
      }
    }
  }

  private Pair<IMNode, Template> checkAndAutoCreateInternalPath(PartialPath devicePath)
      throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    if (nodeNames.length < 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(devicePath.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(devicePath);
    IMNode cur = root;
    boolean hasSetStorageGroup = false;
    Template upperTemplate = cur.getSchemaTemplate();
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length; i++) {
      String childName = nodeNames[i];
      if (!cur.hasChild(childName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        if (upperTemplate != null && upperTemplate.getDirectNode(childName) != null) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(childName).getFullPath()
                  + " ( which is incompatible with template )");
        }
        cur.addChild(childName, new InternalMNode(cur, childName));
      }
      cur = cur.getChild(childName);

      if (cur.isMeasurement()) {
        throw new PathAlreadyExistException(cur.getFullPath());
      }
      if (cur.isStorageGroup()) {
        hasSetStorageGroup = true;
      }

      if (cur.getSchemaTemplate() != null) {
        upperTemplate = cur.getSchemaTemplate();
      }
    }
    return new Pair<>(cur, upperTemplate);
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  public Pair<PartialPath, IMeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(
      PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path.getFullPath());
    }

    if (isPathExistsWithinTemplate(path)) {
      throw new MetadataException(
          "Cannot delete a timeseries inside a template: " + path.toString());
    }

    IMeasurementMNode deletedNode = getMeasurementMNode(path);
    IEntityMNode parent = deletedNode.getParent();
    // delete the last node of path
    parent.deleteChild(path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      parent.deleteAliasChild((deletedNode.getAlias()));
    }
    IMNode curNode = parent;
    if (!parent.isUseTemplate()) {
      boolean hasMeasurement = false;
      for (IMNode child : parent.getChildren().values()) {
        if (child.isMeasurement()) {
          hasMeasurement = true;
          break;
        }
      }
      if (!hasMeasurement) {
        synchronized (this) {
          curNode = MNodeUtils.setToInternal(parent);
        }
      }
    }

    // delete all empty ancestors except storage group and MeasurementMNode
    while (curNode.isEmptyInternal()) {
      // if current storage group has no time series, return the storage group name
      if (curNode.isStorageGroup()) {
        return new Pair<>(curNode.getPartialPath(), deletedNode);
      }
      curNode.getParent().deleteChild(curNode.getName());
      curNode = curNode.getParent();
    }
    return new Pair<>(null, deletedNode);
  }
  // endregion

  // region Entity/Device operation
  // including device auto creation and transform from InternalMNode to EntityMNode
  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  public IMNode getDeviceNodeWithAutoCreating(PartialPath deviceId, int sgLevel)
      throws MetadataException {
    String[] nodeNames = deviceId.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(deviceId.getFullPath());
    }
    IMNode cur = root;
    Template upperTemplate = cur.getSchemaTemplate();
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        if (cur.isUseTemplate() && upperTemplate.getDirectNode(nodeNames[i]) != null) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        if (i == sgLevel) {
          cur.addChild(
              nodeNames[i],
              new StorageGroupMNode(
                  cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL()));
        } else {
          cur.addChild(nodeNames[i], new InternalMNode(cur, nodeNames[i]));
        }
      }
      cur = cur.getChild(nodeNames[i]);
      // update upper template
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
    }

    return cur;
  }

  public IEntityMNode setToEntity(IMNode node) {
    // synchronize check and replace, we need replaceChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      return MNodeUtils.setToEntity(node);
    }
  }
  // endregion

  // region StorageGroup Operation, including set and delete
  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  public void setStorageGroup(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    MetaFormatUtils.checkStorageGroup(path.getFullPath());
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    Template upperTemplate = cur.getSchemaTemplate();
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      IMNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        cur.addChild(nodeNames[i], new InternalMNode(cur, nodeNames[i]));
      } else if (temp.isStorageGroup()) {
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
      i++;
    }

    // synchronize check and add, we need addChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      if (cur.hasChild(nodeNames[i])) {
        // node b has child sg
        if (cur.getChild(nodeNames[i]).isStorageGroup()) {
          throw new StorageGroupAlreadySetException(path.getFullPath());
        } else {
          throw new StorageGroupAlreadySetException(path.getFullPath(), true);
        }
      } else {
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        IStorageGroupMNode storageGroupMNode =
            new StorageGroupMNode(
                cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
        cur.addChild(nodeNames[i], storageGroupMNode);
      }
    }
  }

  /** Delete a storage group */
  public List<IMeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    IMNode cur = getNodeByPath(path);
    if (!(cur.isStorageGroup())) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    cur.getParent().deleteChild(cur.getName());

    // collect all the LeafMNode in this storage group
    List<IMeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<IMNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      IMNode node = queue.poll();
      for (IMNode child : node.getChildren().values()) {
        if (child.isMeasurement()) {
          leafMNodes.add(child.getAsMeasurementMNode());
        } else {
          queue.add(child);
        }
      }
    }

    cur = cur.getParent();
    // delete node b while retain root.a.sg2
    while (!IoTDBConstant.PATH_ROOT.equals(cur.getName()) && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
    return leafMNodes;
  }
  // endregion

  // region Interfaces and Implementation for metadata info Query
  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return false;
    }
    Template upperTemplate = cur.getSchemaTemplate();
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        if (!cur.isUseTemplate() || upperTemplate.getDirectNode(nodeNames[i]) == null) {
          return false;
        }
        cur = upperTemplate.getDirectNode(nodeNames[i]);
      }
      cur = cur.getChild(nodeNames[i]);
      if (cur.isMeasurement()) {
        return i == nodeNames.length - 1;
      }
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
    }
    return true;
  }

  // region Interfaces for Storage Group info Query
  /**
   * Check whether path is storage group or not
   *
   * <p>e.g., path = root.a.b.sg. if nor a and b is StorageGroupMNode and sg is a StorageGroupMNode
   * path is a storage group
   *
   * @param path path
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      return false;
    }
    IMNode cur = root;
    int i = 1;
    while (i < nodeNames.length - 1) {
      cur = cur.getChild(nodeNames[i]);
      if (cur == null || cur.isStorageGroup()) {
        return false;
      }
      i++;
    }
    cur = cur.getChild(nodeNames[i]);
    return cur != null && cur.isStorageGroup();
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        return false;
      } else if (cur.isStorageGroup()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get storage group path by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    String[] nodes = path.getNodes();
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        throw new StorageGroupNotSetException(path.getFullPath());
      } else if (cur.isStorageGroup()) {
        return cur.getPartialPath();
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /**
   * Get the storage group that given path pattern matches or belongs to.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param pathPattern a path pattern or a full path
   * @return a list contains all storage groups related to given path
   */
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return collectStorageGroups(pathPattern, false, true);
  }

  /**
   * Get all storage group that the given path pattern matches. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected.
   *
   * @param pathPattern a path pattern or a full path
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return a list contains all storage group names under given path pattern
   */
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return collectStorageGroups(pathPattern, isPrefixMatch, false);
  }

  private List<PartialPath> collectStorageGroups(
      PartialPath pathPattern, boolean isPrefixMatch, boolean collectInternal)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    StorageGroupCollector<List<PartialPath>> collector =
        new StorageGroupCollector<List<PartialPath>>(root, pathPattern) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            result.add(node.getPartialPath());
          }
        };
    collector.setCollectInternal(collectInternal);
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  public List<PartialPath> getAllStorageGroupPaths() {
    List<PartialPath> res = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        res.add(current.getPartialPath());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return res;
  }

  /**
   * Resolve the path or path pattern into StorageGroupName-FullPath pairs. Try determining the
   * storage group using the children of a mNode. If one child is a storage group node, put a
   * storageGroupName-fullPath pair into paths.
   */
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    Map<String, List<PartialPath>> result = new HashMap<>();
    StorageGroupCollector<Map<String, String>> collector =
        new StorageGroupCollector<Map<String, String>>(root, path) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            PartialPath sgPath = node.getPartialPath();
            result.put(sgPath.getFullPath(), path.alterPrefixPath(sgPath));
          }
        };
    collector.setCollectInternal(true);
    collector.traverse();
    return result;
  }
  // endregion

  // region Interfaces for Device info Query
  /**
   * Get all devices matching the given path pattern. If isPrefixMatch, then the devices under the
   * paths matching given path pattern will be collected too.
   *
   * @return a list contains all distinct devices names
   */
  public Set<PartialPath> getDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    Set<PartialPath> result = new TreeSet<>();
    EntityCollector<Set<PartialPath>> collector =
        new EntityCollector<Set<PartialPath>>(root, pathPattern) {
          @Override
          protected void collectEntity(IEntityMNode node) throws MetadataException {
            result.add(getCurrentPartialPath(node));
          }
        };
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    List<ShowDevicesResult> res = new ArrayList<>();
    EntityCollector<List<ShowDevicesResult>> collector =
        new EntityCollector<List<ShowDevicesResult>>(
            root, plan.getPath(), plan.getLimit(), plan.getOffset()) {
          @Override
          protected void collectEntity(IEntityMNode node) throws MetadataException {
            PartialPath device = getCurrentPartialPath(node);
            if (plan.hasSgCol()) {
              res.add(
                  new ShowDevicesResult(
                      device.getFullPath(),
                      node.isAligned(),
                      getBelongedStorageGroup(device).getFullPath()));
            } else {
              res.add(new ShowDevicesResult(device.getFullPath(), node.isAligned()));
            }
          }
        };
    collector.setPrefixMatch(plan.isPrefixMatch());
    collector.traverse();
    return res;
  }

  public Set<PartialPath> getDevicesByTimeseries(PartialPath timeseries) throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    MeasurementCollector<Set<PartialPath>> collector =
        new MeasurementCollector<Set<PartialPath>>(root, timeseries) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            result.add(getCurrentPartialPath(node).getDevicePath());
          }
        };
    collector.traverse();
    return result;
  }
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query
  /**
   * Get all measurement paths matching the given path pattern. If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected and return.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch).left;
  }

  /**
   * Get all measurement paths matching the given path pattern
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   */
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern)
      throws MetadataException {
    return getMeasurementPaths(pathPattern, false);
  }

  /**
   * Get all measurement paths matching the given path pattern If using prefix match, the path
   * pattern is used to match prefix path. All timeseries start with the matched prefix path will be
   * collected and return.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return Pair.left contains all the satisfied paths Pair.right means the current offset or zero
   *     if we don't set offset.
   */
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(root, pathPattern, limit, offset) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
            if (nodes[nodes.length - 1].equals(node.getAlias())) {
              // only when user query with alias, the alias in path will be set
              path.setMeasurementAlias(node.getAlias());
            }
            result.add(path);
          }
        };
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    offset = collector.getCurOffset() + 1;
    return new Pair<>(result, offset);
  }

  /**
   * Get all measurement paths matching the given path pattern
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @return Pair.left contains all the satisfied paths Pair.right means the current offset or zero
   *     if we don't set offset.
   */
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset) throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, limit, offset, false);
  }

  /**
   * Get all measurement schema matching the given path pattern order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException {
    List<Pair<PartialPath, String[]>> allMatchedNodes =
        collectMeasurementSchema(plan.getPath(), 0, 0, queryContext, true, plan.isPrefixMatch());

    Stream<Pair<PartialPath, String[]>> sortedStream =
        allMatchedNodes.stream()
            .sorted(
                Comparator.comparingLong(
                        (Pair<PartialPath, String[]> p) -> Long.parseLong(p.right[6]))
                    .reversed()
                    .thenComparing((Pair<PartialPath, String[]> p) -> p.left));

    // no limit
    if (plan.getLimit() == 0) {
      return sortedStream.collect(toList());
    } else {
      return sortedStream.skip(plan.getOffset()).limit(plan.getLimit()).collect(toList());
    }
  }

  /**
   * Get all measurement schema matching the given path pattern
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    return collectMeasurementSchema(
        plan.getPath(), plan.getLimit(), plan.getOffset(), null, false, plan.isPrefixMatch());
  }

  private List<Pair<PartialPath, String[]>> collectMeasurementSchema(
      PartialPath pathPattern,
      int limit,
      int offset,
      QueryContext queryContext,
      boolean needLast,
      boolean isPrefixMatch)
      throws MetadataException {
    List<Pair<PartialPath, String[]>> result = new LinkedList<>();
    MeasurementCollector<List<Pair<PartialPath, String[]>>> collector =
        new MeasurementCollector<List<Pair<PartialPath, String[]>>>(
            root, pathPattern, limit, offset) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            IMeasurementSchema measurementSchema = node.getSchema();
            String[] tsRow = new String[7];
            tsRow[0] = node.getAlias();
            tsRow[1] = getStorageGroupNodeInTraversePath().getFullPath();
            tsRow[2] = measurementSchema.getType().toString();
            tsRow[3] = measurementSchema.getEncodingType().toString();
            tsRow[4] = measurementSchema.getCompressor().toString();
            tsRow[5] = String.valueOf(node.getOffset());
            tsRow[6] = needLast ? String.valueOf(getLastTimeStamp(node, queryContext)) : null;
            Pair<PartialPath, String[]> temp = new Pair<>(getCurrentPartialPath(node), tsRow);
            result.add(temp);
          }
        };
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  private PartialPath getBelongedStorageGroupPath(IMeasurementMNode node)
      throws StorageGroupNotSetException {
    if (node == null) {
      return null;
    }
    IMNode temp = node;
    while (temp != null) {
      if (temp.isStorageGroup()) {
        break;
      }
      temp = temp.getParent();
    }
    if (temp == null) {
      throw new StorageGroupNotSetException(node.getFullPath());
    }
    return temp.getPartialPath();
  }

  public Map<PartialPath, IMeasurementSchema> getAllMeasurementSchemaByPrefix(
      PartialPath prefixPath) throws MetadataException {
    Map<PartialPath, IMeasurementSchema> result = new HashMap<>();
    MeasurementCollector<List<IMeasurementSchema>> collector =
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            result.put(getCurrentPartialPath(node), node.getSchema());
          }
        };
    collector.setPrefixMatch(true);
    collector.traverse();
    return result;
  }

  /**
   * Collect the timeseries schemas as IMeasurementSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas)
      throws MetadataException {
    MeasurementCollector<List<IMeasurementSchema>> collector =
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            measurementSchemas.add(node.getSchema());
          }
        };
    collector.setPrefixMatch(true);
    collector.traverse();
  }

  /**
   * Collect the timeseries schemas as TimeseriesSchema under "prefixPath".
   *
   * @apiNote :for cluster
   */
  public void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas)
      throws MetadataException {
    MeasurementCollector<List<IMeasurementSchema>> collector =
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            IMeasurementSchema nodeSchema = node.getSchema();
            timeseriesSchemas.add(
                new TimeseriesSchema(
                    getCurrentPartialPath(node).getFullPath(),
                    nodeSchema.getType(),
                    nodeSchema.getEncodingType(),
                    nodeSchema.getCompressor()));
          }
        };
    collector.setPrefixMatch(true);
    collector.traverse();
  }

  // endregion

  // region Interfaces for Level Node info Query
  /**
   * Get child node path in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException {
    try {
      MNodeCollector<Set<String>> collector =
          new MNodeCollector<Set<String>>(root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD)) {
            @Override
            protected void transferToResult(IMNode node) {
              try {
                resultSet.add(getCurrentPartialPath(node).getFullPath());
              } catch (IllegalPathException e) {
                logger.error(e.getMessage());
              }
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();
      return collector.getResult();
    } catch (IllegalPathException e) {
      throw new IllegalPathException(pathPattern.getFullPath());
    }
  }

  /**
   * Get child node in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2]
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1.d1
   * return [s1, s2]
   *
   * @param pathPattern Path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    try {
      MNodeCollector<Set<String>> collector =
          new MNodeCollector<Set<String>>(root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD)) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(node.getName());
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();
      return collector.getResult();
    } catch (IllegalPathException e) {
      throw new IllegalPathException(pathPattern.getFullPath());
    }
  }

  /** Get all paths from root to the given level */
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
    MNodeCollector<List<PartialPath>> collector =
        new MNodeCollector<List<PartialPath>>(root, pathPattern) {
          @Override
          protected void transferToResult(IMNode node) {
            try {
              resultSet.add(getCurrentPartialPath(node));
            } catch (MetadataException e) {
              logger.error(e.getMessage());
            }
          }
        };
    collector.setResultSet(new LinkedList<>());
    collector.setTargetLevel(nodeLevel);
    collector.setStorageGroupFilter(filter);
    collector.traverse();
    return collector.getResult();
  }
  // endregion

  // region Interfaces and Implementation for metadata count
  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new MeasurementCounter(root, pathPattern);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  /**
   * Get the count of devices matching the given path. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new EntityCounter(root, pathPattern);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of devices matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
  }

  /**
   * Get the count of storage group matching the given path. If using prefix match, the path pattern
   * is used to match prefix path. All timeseries start with the matched prefix path will be
   * counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new StorageGroupCounter(root, pathPattern);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of storage group matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   */
  public int getStorageGroupNum(PartialPath pathPattern) throws MetadataException {
    return getStorageGroupNum(pathPattern, false);
  }

  /**
   * Get the count of nodes in the given level matching the given path. If using prefix match, the
   * path pattern is used to match prefix path. All timeseries start with the matched prefix path
   * will be counted.
   */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    MNodeLevelCounter counter = new MNodeLevelCounter(root, pathPattern, level);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /** Get the count of nodes in the given level matching the given path. */
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level)
      throws MetadataException {
    return getNodesCountInGivenLevel(pathPattern, level, false);
  }

  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    MeasurementGroupByLevelCounter counter =
        new MeasurementGroupByLevelCounter(root, pathPattern, level);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getResult();
  }

  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level) throws MetadataException {
    return getMeasurementCountGroupByLevel(pathPattern, level, false);
  }

  // endregion

  // endregion

  // region Interfaces and Implementation for MNode Query
  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  public IMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    Template upperTemplate = cur.getSchemaTemplate();

    for (int i = 1; i < nodes.length; i++) {
      if (cur.isMeasurement()) {
        if (i == nodes.length - 1) {
          return cur;
        } else {
          throw new PathNotExistException(path.getFullPath(), true);
        }
      }
      if (cur.getSchemaTemplate() != null) {
        upperTemplate = cur.getSchemaTemplate();
      }
      IMNode next = cur.getChild(nodes[i]);
      if (next == null) {
        if (upperTemplate == null
            || !cur.isUseTemplate()
            || upperTemplate.getDirectNode(nodes[i]) == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        }
        next = upperTemplate.getDirectNode(nodes[i]);
      }
      cur = next;
    }
    return cur;
  }

  /**
   * Get node by path with storage group check If storage group is not set,
   * StorageGroupNotSetException will be thrown
   */
  public IMNode getNodeByPathWithStorageGroupCheck(PartialPath path) throws MetadataException {
    boolean storageGroupChecked = false;
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMNode cur = root;
    Template upperTemplate = null;

    for (int i = 1; i < nodes.length; i++) {
      if (cur.getSchemaTemplate() != null) {
        upperTemplate = cur.getSchemaTemplate();
      }

      if (cur.getChild(nodes[i]) != null) {
        cur = cur.getChild(nodes[i]);
      } else {
        // seek child in template
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }

        if (upperTemplate == null
            || !cur.isUseTemplate()
            || upperTemplate.getDirectNode(nodes[i]) == null) {
          throw new PathNotExistException(path.getFullPath());
        }

        cur = upperTemplate.getDirectNode(nodes[i]);
      }

      if (cur.isStorageGroup()) {
        storageGroupChecked = true;
      }
    }

    if (!storageGroupChecked) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    return cur;
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], throw exception Get storage group node, if the give path is not a storage group, throw
   * exception
   */
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    IMNode node = getNodeByPath(path);
    if (node.isStorageGroup()) {
      return node.getAsStorageGroupMNode();
    } else {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.STORAGE_GROUP_MNODE_TYPE);
    }
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node, the give path don't need to be
   * storage group path.
   */
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        break;
      }
      if (cur.isStorageGroup()) {
        return cur.getAsStorageGroupMNode();
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    List<IStorageGroupMNode> ret = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        ret.add(current.getAsStorageGroupMNode());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath path) throws MetadataException {
    IMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.MEASUREMENT_MNODE_TYPE);
    }
  }

  // endregion

  // region Interfaces and Implementation for Template check and query
  /**
   * check whether there is template on given path and the subTree has template return true,
   * otherwise false
   */
  public void checkTemplateOnPath(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return;
    }
    if (cur.getSchemaTemplate() != null) {
      throw new MetadataException("Template already exists on " + cur.getFullPath());
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (cur.isMeasurement()) {
        return;
      }
      if (!cur.hasChild(nodeNames[i])) {
        return;
      }
      cur = cur.getChild(nodeNames[i]);
      if (cur.getSchemaTemplate() != null) {
        throw new MetadataException("Template already exists on " + cur.getFullPath());
      }
    }

    checkTemplateOnSubtree(cur);
  }

  /**
   * Check route 1: If template has no direct measurement, just pass the check.
   *
   * <p>Check route 2: If template has direct measurement and mounted node is Internal, it should be
   * set to Entity.
   *
   * <p>Check route 3: If template has direct measurement and mounted node is Entity,
   *
   * <p>route 3.1: mounted node has no measurement child, then its alignment will be set as the
   * template.
   *
   * <p>route 3.2: mounted node has measurement child, then alignment of it and template should be
   * identical, otherwise cast a exception.
   *
   * @return return the node competent to be mounted.
   */
  public IMNode checkTemplateAlignmentWithMountedNode(IMNode mountedNode, Template template)
      throws MetadataException {
    boolean hasDirectMeasurement = false;
    for (IMNode child : template.getDirectNodes()) {
      if (child.isMeasurement()) {
        hasDirectMeasurement = true;
      }
    }
    if (hasDirectMeasurement) {
      if (!mountedNode.isEntity()) {
        return setToEntity(mountedNode);
      } else {
        for (IMNode child : mountedNode.getChildren().values()) {
          if (child.isMeasurement()) {
            if (template.isDirectAligned() != mountedNode.getAsEntityMNode().isAligned()) {
              throw new MetadataException(
                  "Template and mounted node has different alignment: "
                      + template.getName()
                      + mountedNode.getFullPath());
            } else {
              return mountedNode;
            }
          }
        }
        mountedNode.getAsEntityMNode().setAligned(template.isDirectAligned());
      }
    }
    return mountedNode;
  }

  // traverse  all the  descendant of the given path node
  private void checkTemplateOnSubtree(IMNode node) throws MetadataException {
    if (node.isMeasurement()) {
      return;
    }
    for (IMNode child : node.getChildren().values()) {
      if (child.isMeasurement()) {
        continue;
      }
      if (child.getSchemaTemplate() != null) {
        throw new MetadataException("Template already exists on " + child.getFullPath());
      }
      checkTemplateOnSubtree(child);
    }
  }

  public void checkTemplateInUseOnLowerNode(IMNode node) throws TemplateIsInUseException {
    if (node.isMeasurement()) {
      return;
    }
    for (IMNode child : node.getChildren().values()) {
      if (child.isMeasurement()) {
        continue;
      }
      if (child.isUseTemplate()) {
        throw new TemplateIsInUseException(child.getFullPath());
      }
      checkTemplateInUseOnLowerNode(child);
    }
  }

  /**
   * Note that template and MTree cannot have overlap paths.
   *
   * @return true iff path corresponding to a measurement inside a template, whether using or not.
   */
  public boolean isPathExistsWithinTemplate(PartialPath path) {
    if (path.getNodes().length < 2) {
      return false;
    }
    String[] pathNodes = path.getNodes();
    IMNode cur = root;
    Template upperTemplate = cur.getUpperTemplate();
    for (int i = 1; i < pathNodes.length; i++) {
      if (cur.hasChild(pathNodes[i])) {
        cur = cur.getChild(pathNodes[i]);
        if (cur.isMeasurement()) {
          return false;
        }
        upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
      } else if (upperTemplate != null) {
        String suffixPath =
            new PartialPath(Arrays.copyOfRange(pathNodes, i, pathNodes.length)).toString();
        if (upperTemplate.hasSchema(suffixPath)) {
          return true;
        } else {
          // has template, but not match
          return false;
        }
      } else {
        // no child and no template
        return false;
      }
    }
    return false;
  }

  /**
   * Check measurement path and return the mounted node index on path. The node could have not
   * created yet. The result is used for getDeviceNodeWithAutoCreate, which return corresponding
   * IMNode on MTree.
   *
   * @return index on full path of the node which matches all measurements path with its
   *     upperTemplate.
   */
  public int getMountedNodeIndexOnMeasurementPath(PartialPath measurementPath)
      throws MetadataException {
    String[] fullPathNodes = measurementPath.getNodes();
    IMNode cur = root;
    Template upperTemplate = cur.getSchemaTemplate();

    if (!cur.getName().equals(fullPathNodes[0])) {
      throw new IllegalPathException(measurementPath.toString());
    }

    for (int index = 1; index < fullPathNodes.length; index++) {
      upperTemplate = cur.getSchemaTemplate() != null ? cur.getSchemaTemplate() : upperTemplate;
      if (!cur.hasChild(fullPathNodes[index])) {
        if (upperTemplate != null) {
          // for this fullPath, cur is the last node on MTree
          // since upperTemplate exists, need to find the matched suffix path of fullPath and
          // template
          String suffixPath =
              new PartialPath(Arrays.copyOfRange(fullPathNodes, index, fullPathNodes.length))
                  .toString();

          // if suffix matches template, then fullPathNodes[index-1] should be the node to use
          // template on MTree
          if (upperTemplate.hasSchema(suffixPath)) {
            return index - 1;
          }

          // if suffix doesn't match, but first node name matched, it's an overlap with template
          // cast exception for now
          if (upperTemplate.getDirectNode(fullPathNodes[index]) != null) {
            throw new TemplateImcompatibeException(
                measurementPath.getFullPath(), upperTemplate.getName(), fullPathNodes[index]);
          }
        } else {
          // no matched child, no template, need to create device node as logical device path
          return fullPathNodes.length - 1;
        }
      } else {
        // has child on MTree
        cur = cur.getChild(fullPathNodes[index]);
      }
    }
    // all nodes on path exist in MTree, device node should be the penultimate one
    return fullPathNodes.length - 1;
  }

  public List<String> getPathsSetOnTemplate(String templateName) throws MetadataException {
    List<String> resSet = new ArrayList<>();
    CollectorTraverser<Set<String>> setTemplatePaths =
        new CollectorTraverser<Set<String>>(
            root, root.getPartialPath().concatNode(MULTI_LEVEL_PATH_WILDCARD)) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            // will never get here, implement for placeholder
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            // shall not traverse nodes inside template
            if (!node.getPartialPath().equals(getCurrentPartialPath(node))) {
              return true;
            }

            // if node not set template, go on traversing
            if (node.getUpperTemplate() != null) {
              // if set template, and equals to target or target for all, add to result
              if (templateName.equals("")
                  || templateName.equals(node.getUpperTemplate().getName())) {
                resSet.add(node.getFullPath());
              }
              // descendants of the node cannot set another template, exit from this branch
              return true;
            }
            return false;
          }
        };
    setTemplatePaths.traverse();
    return resSet;
  }

  public List<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    List<String> result = new ArrayList<>();

    CollectorTraverser<Set<String>> usingTemplatePaths =
        new CollectorTraverser<Set<String>>(
            root, root.getPartialPath().concatNode(MULTI_LEVEL_PATH_WILDCARD)) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            // will never get here, implement for placeholder
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
              throws MetadataException {
            // shall not traverse nodes inside template
            if (!node.getPartialPath().equals(getCurrentPartialPath(node))) {
              return true;
            }

            if (node.getUpperTemplate() != null) {
              // this node and its descendants are set other template, exit from this branch
              if (!templateName.equals("")
                  && !templateName.equals(node.getUpperTemplate().getName())) {
                return true;
              }

              // descendants of this node may be using template too
              if (node.isUseTemplate()) {
                result.add(node.getFullPath());
              }
            }
            return false;
          }
        };

    usingTemplatePaths.traverse();
    return result;
  }

  public boolean isTemplateSetOnMTree(String templateName) {
    // check whether template has been set
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(root);

    // DFT traverse on MTree
    while (nodeStack.size() != 0) {
      IMNode curNode = nodeStack.pop();
      if (curNode.getUpperTemplate() != null) {
        if (curNode.getUpperTemplate().getName().equals(templateName)) {
          return true;
        }
        // curNode set to other templates, cut this branch
      }

      // no template on curNode, push children to stack
      for (IMNode child : curNode.getChildren().values()) {
        nodeStack.push(child);
      }
    }
    return false;
  }

  // endregion

  // region TestOnly Interface
  /** combine multiple metadata in string format */
  @TestOnly
  public static JsonObject combineMetadataInStrings(String[] metadataStrs) {
    JsonObject[] jsonObjects = new JsonObject[metadataStrs.length];
    for (int i = 0; i < jsonObjects.length; i++) {
      jsonObjects[i] = GSON.fromJson(metadataStrs[i], JsonObject.class);
    }

    JsonObject root = jsonObjects[0];
    for (int i = 1; i < jsonObjects.length; i++) {
      root = combineJsonObjects(root, jsonObjects[i]);
    }

    return root;
  }

  private static JsonObject combineJsonObjects(JsonObject a, JsonObject b) {
    JsonObject res = new JsonObject();

    Set<String> retainSet = new HashSet<>(a.keySet());
    retainSet.retainAll(b.keySet());
    Set<String> aCha = new HashSet<>(a.keySet());
    Set<String> bCha = new HashSet<>(b.keySet());
    aCha.removeAll(retainSet);
    bCha.removeAll(retainSet);

    for (String key : aCha) {
      res.add(key, a.get(key));
    }

    for (String key : bCha) {
      res.add(key, b.get(key));
    }
    for (String key : retainSet) {
      JsonElement v1 = a.get(key);
      JsonElement v2 = b.get(key);
      if (v1 instanceof JsonObject && v2 instanceof JsonObject) {
        res.add(key, combineJsonObjects((JsonObject) v1, (JsonObject) v2));
      } else {
        res.add(v1.getAsString(), v2);
      }
    }
    return res;
  }
  // endregion
}
