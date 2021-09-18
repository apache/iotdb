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
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityPathCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementPathCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementSchemaCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.StorageGroupDeterminator;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.StorageGroupPathCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.TSEntityPathCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MNodeLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.StorageGroupCounter;
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
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

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
import java.util.Comparator;
import java.util.Deque;
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
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/** The hierarchical struct of the Metadata Tree is implemented in this class. */
public class MTree implements Serializable {

  public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final long serialVersionUID = -4200394435237291964L;
  private static final Logger logger = LoggerFactory.getLogger(MTree.class);
  private static final String NO_CHILDNODE_MSG = " does not have the child node ";
  private IMNode root;

  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;

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
      if (!cur.hasChild(childName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        if (cur.isUseTemplate() && upperTemplate.hasSchema(childName)) {
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(childName).getFullPath());
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

    if (upperTemplate != null && !upperTemplate.isCompatible(path)) {
      throw new PathAlreadyExistException(
          path.getFullPath() + " ( which is incompatible with template )");
    }

    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

    String leafName = nodeNames[nodeNames.length - 1];

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      if (cur.hasChild(leafName)) {
        throw new PathAlreadyExistException(path.getFullPath());
      }

      if (alias != null && cur.hasChild(alias)) {
        throw new AliasAlreadyExistException(path.getFullPath(), alias);
      }

      IEntityMNode entityMNode = IEntityMNode.setToEntity(cur);

      IMeasurementMNode measurementMNode =
          new MeasurementMNode(entityMNode, leafName, alias, dataType, encoding, compressor, props);
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
   * @param compressor compressor
   */
  public void createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor)
      throws MetadataException {
    String[] deviceNodeNames = devicePath.getNodes();
    if (deviceNodeNames.length <= 1 || !deviceNodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(devicePath.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(devicePath);
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    IMNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, devicePath = root.sg.d1, create internal nodes and set cur to d1 node
    for (int i = 1; i < deviceNodeNames.length - 1; i++) {
      if (cur.isMeasurement()) {
        throw new PathAlreadyExistException(cur.getFullPath());
      }
      if (cur.isStorageGroup()) {
        hasSetStorageGroup = true;
      }
      String nodeName = deviceNodeNames[i];
      if (!cur.hasChild(nodeName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        cur.addChild(nodeName, new InternalMNode(cur, nodeName));
      }
      cur = cur.getChild(nodeName);
    }

    if (cur.isMeasurement()) {
      throw new PathAlreadyExistException(cur.getFullPath());
    }

    String leafName = deviceNodeNames[deviceNodeNames.length - 1];

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      if (cur.hasChild(leafName)) {
        throw new PathAlreadyExistException(devicePath.getFullPath() + "." + leafName);
      }

      IEntityMNode entityMNode = IEntityMNode.setToEntity(cur);

      int measurementsSize = measurements.size();

      // this measurementMNode could be a leaf or not.
      IMeasurementMNode measurementMNode =
          new MeasurementMNode(
              entityMNode,
              leafName,
              new VectorMeasurementSchema(
                  leafName,
                  measurements.toArray(new String[measurementsSize]),
                  dataTypes.toArray(new TSDataType[measurementsSize]),
                  encodings.toArray(new TSEncoding[measurementsSize]),
                  compressor),
              null);
      entityMNode.addChild(leafName, measurementMNode);
    }
  }

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
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
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
        return cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i]);
      }
      cur = cur.getChild(nodeNames[i]);
      if (cur.isMeasurement()) {
        if (i == nodeNames.length - 1) {
          return true;
        }
        if (((IMeasurementMNode) cur).getSchema() instanceof VectorMeasurementSchema) {
          return i == nodeNames.length - 2
              && ((IMeasurementMNode) cur).getSchema().containsSubMeasurement(nodeNames[i + 1]);
        } else {
          return false;
        }
      }
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
    }
    return true;
  }

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
          leafMNodes.add((IMeasurementMNode) child);
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

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  public Pair<PartialPath, IMeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(
      PartialPath path) throws MetadataException {
    IMNode curNode = getNodeByPath(path);
    if (!(curNode.isMeasurement())) {
      throw new PathNotExistException(path.getFullPath());
    }
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMeasurementMNode deletedNode = (IMeasurementMNode) curNode;

    // delete the last node of path
    curNode.getParent().deleteChild(path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      deletedNode.getParent().deleteAliasChild(((IMeasurementMNode) curNode).getAlias());
    }
    curNode = curNode.getParent();
    // delete all empty ancestors except storage group and MeasurementMNode
    while (!IoTDBConstant.PATH_ROOT.equals(curNode.getName())
        && !(curNode.isMeasurement())
        && curNode.getChildren().size() == 0) {
      // if current storage group has no time series, return the storage group name
      if (curNode.isStorageGroup()) {
        return new Pair<>(curNode.getPartialPath(), deletedNode);
      }
      curNode.getParent().deleteChild(curNode.getName());
      curNode = curNode.getParent();
    }
    return new Pair<>(null, deletedNode);
  }

  /**
   * Get measurement schema for a given path. Path must be a complete Path from root to leaf node.
   */
  public IMeasurementSchema getSchema(PartialPath path) throws MetadataException {
    IMeasurementMNode node = (IMeasurementMNode) getNodeByPath(path);
    return node.getSchema();
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

    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        // not find
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        throw new PathNotExistException(path.getFullPath());
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
      return (IStorageGroupMNode) node;
    } else {
      throw new StorageGroupNotSetException(path.getFullPath(), true);
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
        return (IStorageGroupMNode) cur;
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

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
        if (i == nodes.length - 1
            || ((IMeasurementMNode) cur).getSchema() instanceof VectorMeasurementSchema) {
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
        if (upperTemplate == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        }

        String realName = nodes[i];
        IMeasurementSchema schema = upperTemplate.getSchemaMap().get(realName);
        if (schema == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        }
        return new MeasurementMNode(cur, schema.getMeasurementId(), schema, null);
      }
      cur = next;
    }
    return cur;
  }

  /**
   * Get all storage groups under the given path
   *
   * @return storage group list
   * @apiNote :for cluster
   */
  public List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    StorageGroupPathCollector collector = new StorageGroupPathCollector(root, path);
    collector.setCollectInternal(true);
    collector.traverse();
    return collector.getResult().stream().map(PartialPath::getFullPath).collect(toList());
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
   * Get the storage group that given path matches or belongs to. The path may contain wildcard.
   *
   * <p>Suppose we have (root.sg1.d1.s1, root.sg2.d2.s2), refer the following cases: 1. given path
   * "root.sg1", ("root.sg1") will be returned. 2. given path "root.*", ("root.sg1", "root.sg2")
   * will be returned. 3. given path "root.*.d1.s1", ("root.sg1", "root.sg2") will be returned.
   *
   * @param path a path pattern or a full path, may contain wildcard
   * @return a list contains all storage groups related to given path
   */
  public List<PartialPath> searchAllRelatedStorageGroups(PartialPath path)
      throws MetadataException {
    StorageGroupPathCollector collector = new StorageGroupPathCollector(root, path);
    collector.setCollectInternal(true);
    collector.traverse();
    return collector.getResult();
  }

  /**
   * Get all storage group that the given path matches.
   *
   * @param path a path pattern or a full path, may contain wildcard
   * @return a list contains all storage group names under give path
   */
  public List<PartialPath> getStorageGroupPaths(PartialPath path) throws MetadataException {
    StorageGroupPathCollector collector = new StorageGroupPathCollector(root, path);
    collector.setCollectInternal(false);
    collector.traverse();
    return collector.getResult();
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    List<IStorageGroupMNode> ret = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        ret.add((IStorageGroupMNode) current);
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Get storage group path by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
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
   * Get all timeseries matching the given path pattern
   *
   * @param path a path pattern or a full path, may contain wildcard.
   */
  public List<PartialPath> getAllTimeseriesPath(PartialPath path) throws MetadataException {
    return getAllTimeseriesPathWithAlias(path, 0, 0).left;
  }

  /**
   * Get all timeseries paths matching the given path pattern
   *
   * @param path a path pattern or a full path, may contain wildcard
   * @return Pair.left contains all the satisfied paths Pair.right means the current offset or zero
   *     if we don't set offset.
   */
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath path, int limit, int offset) throws MetadataException {
    MeasurementPathCollector collector = new MeasurementPathCollector(root, path, limit, offset);
    collector.traverse();
    offset = collector.getCurOffset() + 1;
    List<PartialPath> res = collector.getResult();
    return new Pair<>(res, offset);
  }

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param path a path pattern or a full path, may contain wildcard
   */
  public int getAllTimeseriesCount(PartialPath path) throws MetadataException {
    CounterTraverser counter = new MeasurementCounter(root, path);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of devices matching the given path.
   *
   * @param path a path pattern or a full path, may contain wildcard
   */
  public int getDevicesNum(PartialPath path) throws MetadataException {
    CounterTraverser counter = new EntityCounter(root, path);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of storage group matching the given path.
   *
   * @param path a path pattern or a full path, may contain wildcard.
   */
  public int getStorageGroupNum(PartialPath path) throws MetadataException {
    CounterTraverser counter = new StorageGroupCounter(root, path);
    counter.traverse();
    return counter.getCount();
  }

  /** Get the count of nodes in the given level matching the given path. */
  public int getNodesCountInGivenLevel(PartialPath path, int level) throws MetadataException {
    MNodeLevelCounter counter = new MNodeLevelCounter(root, path, level);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get all time series schema matching the given path pattern order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException {
    MeasurementSchemaCollector collector = new MeasurementSchemaCollector(root, plan.getPath());
    collector.setQueryContext(queryContext);
    collector.setNeedLast(true);
    collector.traverse();
    List<Pair<PartialPath, String[]>> allMatchedNodes = collector.getResult();

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
   * Get all time series schema matching the given path pattern
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    MeasurementSchemaCollector collector =
        new MeasurementSchemaCollector(root, plan.getPath(), plan.getLimit(), plan.getOffset());
    collector.traverse();
    return collector.getResult();
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
    try {
      MNodeCollector<Set<String>> collector =
          new MNodeCollector<Set<String>>(root, path.concatNode(ONE_LEVEL_PATH_WILDCARD)) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(node.getFullPath());
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();
      return collector.getResult();
    } catch (IllegalPathException e) {
      throw new IllegalPathException(path.getFullPath());
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
   * @param path Path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException {
    try {
      MNodeCollector<Set<String>> collector =
          new MNodeCollector<Set<String>>(root, path.concatNode(ONE_LEVEL_PATH_WILDCARD)) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(node.getName());
            }
          };
      collector.setResultSet(new TreeSet<>());
      collector.traverse();
      return collector.getResult();
    } catch (IllegalPathException e) {
      throw new IllegalPathException(path.getFullPath());
    }
  }

  /**
   * Get all devices matching the given path. If isPrefixMatch, then the devices under the given
   * path will be collected too.
   *
   * @return a list contains all distinct devices names
   */
  public Set<PartialPath> getDevices(PartialPath path, boolean isPrefixMatch)
      throws MetadataException {
    EntityPathCollector collector = new EntityPathCollector(root, path);
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return collector.getResult();
  }

  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    EntityPathCollector collector =
        new EntityPathCollector(root, plan.getPath(), plan.getLimit(), plan.getOffset());
    collector.traverse();
    Set<PartialPath> devices = collector.getResult();
    List<ShowDevicesResult> res = new ArrayList<>();
    for (PartialPath device : devices) {
      if (plan.hasSgCol()) {
        res.add(
            new ShowDevicesResult(device.getFullPath(), getStorageGroupPath(device).getFullPath()));
      } else {
        res.add(new ShowDevicesResult(device.getFullPath()));
      }
    }
    return res;
  }

  public Set<PartialPath> getDevicesForTimeseries(PartialPath timeseries) throws MetadataException {
    TSEntityPathCollector collector = new TSEntityPathCollector(root, timeseries);
    collector.traverse();
    return collector.getResult();
  }

  /** Get all paths from root to the given level */
  public List<PartialPath> getNodesList(PartialPath path, int nodeLevel, StorageGroupFilter filter)
      throws MetadataException {
    MNodeCollector<List<PartialPath>> collector =
        new MNodeCollector<List<PartialPath>>(root, path) {
          @Override
          protected void transferToResult(IMNode node) {
            resultSet.add(node.getPartialPath());
          }
        };
    collector.setResultSet(new LinkedList<>());
    collector.setTargetLevel(nodeLevel);
    collector.setStorageGroupFilter(filter);
    collector.traverse();
    return collector.getResult();
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
                node = IEntityMNode.setToEntity(node);
              }
              String alias = ((IMeasurementMNode) child).getAlias();
              if (alias != null) {
                ((IEntityMNode) node).addAlias(alias, (IMeasurementMNode) child);
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
      IMeasurementMNode leafMNode = (IMeasurementMNode) node;
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

  /**
   * Try determining the storage group using the children of a mNode. If one child is a storage
   * group node, put a storageGroupName-fullPath pair into paths. Otherwise put the children that
   * match the path into the queue and discard other children.
   */
  public Map<String, String> determineStorageGroup(PartialPath path) throws MetadataException {
    StorageGroupDeterminator determinator = new StorageGroupDeterminator(root, path);
    determinator.traverse();
    return determinator.getResult();
  }

  public IEntityMNode setToEntity(IMNode node) {
    // synchronize check and replace, we need replaceChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      return IEntityMNode.setToEntity(node);
    }
  }

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
}
