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
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;
import static org.apache.iotdb.db.query.executor.LastQueryExecutor.calculateLastPairForOneSeriesLocally;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
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
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 */
public class MTree implements Serializable {

  private static final long serialVersionUID = -4200394435237291964L;
  private static final Logger logger = LoggerFactory.getLogger(MTree.class);

  private MNode root;
  private static transient ThreadLocal<Integer> limit = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> offset = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> count = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> curOffset = new ThreadLocal<>();

  MTree() {
    this.root = new MNode(null, IoTDBConstant.PATH_ROOT);
  }

  private MTree(MNode root) {
    this.root = root;
  }

  /**
   * Create a timeseries with a full path from root to leaf node Before creating a timeseries, the
   * storage group should be set first, throw exception otherwise
   *
   * @param path       timeseries path
   * @param dataType   data type
   * @param encoding   encoding
   * @param compressor compressor
   * @param props      props
   * @param alias      alias of measurement
   */
  MeasurementMNode createTimeseries(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    if (nodeNames.length <= 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length - 1; i++) {
      String nodeName = nodeNames[i];
      if (cur instanceof StorageGroupMNode) {
        hasSetStorageGroup = true;
      }
      if (!cur.hasChild(nodeName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        cur.addChild(nodeName, new MNode(cur, nodeName));
      }
      cur = cur.getChild(nodeName);
    }
    String leafName = nodeNames[nodeNames.length - 1];
    if (cur.hasChild(leafName)) {
      throw new PathAlreadyExistException(path);
    }
    if (alias != null && cur.hasChild(alias)) {
      throw new AliasAlreadyExistException(path, alias);
    }
    MeasurementMNode leaf = new MeasurementMNode(cur, leafName, alias, dataType, encoding,
        compressor, props);
    cur.addChild(leafName, leaf);
    // link alias to LeafMNode
    if (alias != null) {
      cur.addAlias(alias, leaf);
    }
    return leaf;
  }

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  MNode getDeviceNodeWithAutoCreating(String deviceId, int sgLevel) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(deviceId);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(deviceId);
    }
    MNode cur = root;
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        if (i == sgLevel) {
          cur.addChild(nodeNames[i], new StorageGroupMNode(cur, nodeNames[i],
              IoTDBDescriptor.getInstance().getConfig().getDefaultTTL()));
        } else {
          cur.addChild(nodeNames[i], new MNode(cur, nodeNames[i]));
        }
      }
      cur = cur.getChild(nodeNames[i]);
    }
    return cur;
  }

  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  boolean isPathExist(String path) {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    MNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return false;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      String childName = nodeNames[i];
      if (cur.hasChild(childName)) {
        cur = cur.getChild(childName);
      } else {
        return false;
      }
    }
    return true;
  }

  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  void setStorageGroup(String path) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        cur.addChild(nodeNames[i], new MNode(cur, nodeNames[i]));
      } else if (temp instanceof StorageGroupMNode) {
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    if (cur.hasChild(nodeNames[i])) {
      // node b has child sg
      throw new StorageGroupAlreadySetException(path);
    } else {
      StorageGroupMNode storageGroupMNode =
          new StorageGroupMNode(
              cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
      cur.addChild(nodeNames[i], storageGroupMNode);
    }
  }

  /**
   * Delete a storage group
   */
  List<MeasurementMNode> deleteStorageGroup(String path) throws MetadataException {
    MNode cur = getNodeByPath(path);
    if (!(cur instanceof StorageGroupMNode)) {
      throw new StorageGroupNotSetException(path);
    }
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    cur.getParent().deleteChild(cur.getName());

    // collect all the LeafMNode in this storage group
    List<MeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<MNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      MNode node = queue.poll();
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode) {
          leafMNodes.add((MeasurementMNode) child);
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
  boolean isStorageGroup(String path) {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      return false;
    }
    MNode cur = root;
    int i = 1;
    while (i < nodeNames.length - 1) {
      cur = cur.getChild(nodeNames[i]);
      if (cur == null || cur instanceof StorageGroupMNode) {
        return false;
      }
      i++;
    }
    cur = cur.getChild(nodeNames[i]);
    return cur instanceof StorageGroupMNode;
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  Pair<String, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(String path)
      throws MetadataException {
    MNode curNode = getNodeByPath(path);
    if (!(curNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(path);
    }
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path);
    }
    // delete the last node of path
    curNode.getParent().deleteChild(curNode.getName());
    MeasurementMNode deletedNode = (MeasurementMNode) curNode;
    if (deletedNode.getAlias() != null) {
      curNode.getParent().deleteAliasChild(((MeasurementMNode) curNode).getAlias());
    }
    curNode = curNode.getParent();
    // delete all empty ancestors except storage group
    while (!IoTDBConstant.PATH_ROOT.equals(curNode.getName())
        && curNode.getChildren().size() == 0) {
      // if current storage group has no time series, return the storage group name
      if (curNode instanceof StorageGroupMNode) {
        return new Pair<>(curNode.getFullPath(), deletedNode);
      }
      curNode.getParent().deleteChild(curNode.getName());
      curNode = curNode.getParent();
    }
    return new Pair<>(null, deletedNode);
  }

  /**
   * Get measurement schema for a given path. Path must be a complete Path from root to leaf node.
   */
  MeasurementSchema getSchema(String path) throws MetadataException {
    MeasurementMNode node = (MeasurementMNode) getNodeByPath(path);
    return node.getSchema();
  }

  /**
   * Get node by path with storage group check If storage group is not set,
   * StorageGroupNotSetException will be thrown
   */
  MNode getNodeByPathWithStorageGroupCheck(String path) throws MetadataException {
    boolean storageGroupChecked = false;
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }

    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(path);
        }
        throw new PathNotExistException(path);
      }
      cur = cur.getChild(nodes[i]);

      if (cur instanceof StorageGroupMNode) {
        storageGroupChecked = true;
      }
    }

    if (!storageGroupChecked) {
      throw new StorageGroupNotSetException(path);
    }
    return cur;
  }

  /**
   * Get storage group node, if the give path is not a storage group, throw exception
   */
  StorageGroupMNode getStorageGroupNode(String path) throws MetadataException {
    MNode node = getNodeByPath(path);
    if (node instanceof StorageGroupMNode) {
      return (StorageGroupMNode) node;
    } else {
      throw new StorageGroupNotSetException(path);
    }
  }

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  MNode getNodeByPath(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathNotExistException(path);
      }
      cur = cur.getChild(nodes[i]);
    }
    return cur;
  }

  /**
   * Get all storage groups under the given path
   *
   * @return storage group list
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(String path) throws MetadataException {
    List<String> storageGroups = new ArrayList<>();
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    findStorageGroup(root, nodes, 1, "", storageGroups);
    return storageGroups;
  }

  /**
   * Recursively find all storage group according to a specific path
   *
   * @apiNote :for cluster
   */
  private void findStorageGroup(
      MNode node, String[] nodes, int idx, String parent, List<String> storageGroupNames) {
    if (node instanceof StorageGroupMNode) {
      storageGroupNames.add(node.getFullPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findStorageGroup(
            node.getChild(nodeReg),
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            storageGroupNames);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroup(
            child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    }
  }

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  List<String> getAllStorageGroupNames() {
    List<String> res = new ArrayList<>();
    Deque<MNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      MNode current = nodeStack.pop();
      if (current instanceof StorageGroupMNode) {
        res.add(current.getFullPath());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return res;
  }

  /**
   * Get all storage group MNodes
   */
  List<StorageGroupMNode> getAllStorageGroupNodes() {
    List<StorageGroupMNode> ret = new ArrayList<>();
    Deque<MNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      MNode current = nodeStack.pop();
      if (current instanceof StorageGroupMNode) {
        ret.add((StorageGroupMNode) current);
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  String getStorageGroupName(String path) throws StorageGroupNotSetException {
    String[] nodes = MetaUtils.getNodeNames(path);
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur instanceof StorageGroupMNode) {
        return cur.getFullPath();
      } else if (cur == null) {
        throw new StorageGroupNotSetException(path);
      }
    }
    throw new StorageGroupNotSetException(path);
  }

  /**
   * Check whether the given path contains a storage group
   */
  boolean checkStorageGroupByPath(String path) {
    String[] nodes = MetaUtils.getNodeNames(path);
    MNode cur = root;
    for (int i = 1; i <= nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        return false;
      } else if (cur instanceof StorageGroupMNode) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get all timeseries under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  List<String> getAllTimeseriesName(String prefixPath) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(new Path(prefixPath));
    List<String[]> res = getAllMeasurementSchema(plan);
    List<String> paths = new ArrayList<>();
    for (String[] p : res) {
      paths.add(p[0]);
    }
    return paths;
  }

  /**
   * Get all timeseries paths under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  List<Path> getAllTimeseriesPath(String prefixPath) throws MetadataException {
    Path prePath = new Path(prefixPath);
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prePath);
    List<String[]> res = getAllMeasurementSchema(plan);
    List<Path> paths = new ArrayList<>();
    for (String[] p : res) {
      Path path = new Path(p[0]);
      if (prePath.getMeasurement().equals(p[1])) {
        path.setAlias(p[1]);
      }
      paths.add(path);
    }
    return paths;
  }

  /**
   * Get the count of timeseries under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getAllTimeseriesCount(String prefixPath) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(prefixPath);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath);
    }
    return getCount(root, nodes, 1);
  }

  /**
   * Get the count of nodes in the given level under the given prefix path.
   */
  int getNodesCountInGivenLevel(String prefixPath, int level) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(prefixPath);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath);
    }
    MNode node = root;
    for (int i = 1; i < nodes.length; i++) {
      if (node.getChild(nodes[i]) != null) {
        node = node.getChild(nodes[i]);
      } else {
        throw new MetadataException(nodes[i - 1] + " does not have the child node " + nodes[i]);
      }
    }
    return getCountInGivenLevel(node, level - (nodes.length - 1));
  }

  /**
   * Traverse the MTree to get the count of timeseries.
   */
  private int getCount(MNode node, String[] nodes, int idx) throws MetadataException {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        if (node.getChild(nodeReg) instanceof MeasurementMNode) {
          return 1;
        } else {
          return getCount(node.getChild(nodeReg), nodes, idx + 1);
        }
      } else {
        throw new MetadataException(node.getName() + " does not have the child node " + nodeReg);
      }
    } else {
      int cnt = 0;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode) {
          cnt++;
        }
        cnt += getCount(child, nodes, idx + 1);
      }
      return cnt;
    }
  }

  /**
   * Traverse the MTree to get the count of timeseries in the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private int getCountInGivenLevel(MNode node, int targetLevel) {
    if (targetLevel == 0) {
      return 1;
    }
    int cnt = 0;
    for (MNode child : node.getChildren().values()) {
      cnt += getCountInGivenLevel(child, targetLevel - 1);
    }
    return cnt;
  }

  /**
   * Get all time series schema under the given path order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<String[]> getAllMeasurementSchemaByHeatOrder(ShowTimeSeriesPlan plan,
      QueryContext queryContext) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(plan.getPath().getFullPath());
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    List<String[]> allMatchedNodes = new ArrayList<>();

    findPath(root, nodes, 1, allMatchedNodes, false, true, queryContext);

    Stream<String[]> sortedStream = allMatchedNodes.stream().sorted(
        Comparator.comparingLong((String[] s) -> Long.parseLong(s[7])).reversed()
            .thenComparing((String[] array) -> array[0]));

    // no limit
    if (plan.getLimit() == 0) {
      return sortedStream.collect(toList());
    } else {
      return sortedStream.skip(plan.getOffset()).limit(plan.getLimit()).collect(toList());
    }
  }


  /**
   * Get all time series schema under the given path
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<String[]> getAllMeasurementSchema(ShowTimeSeriesPlan plan) throws MetadataException {
    List<String[]> res;
    String[] nodes = MetaUtils.getNodeNames(plan.getPath().getFullPath());
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    if (offset.get() != 0 || limit.get() != 0) {
      res = new LinkedList<>();
      findPath(root, nodes, 1, res, true, false, null);
    } else {
      res = new LinkedList<>();
      findPath(root, nodes, 1, res, false, false, null);
    }
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    return res;
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   *
   * @param needLast             if false, lastTimeStamp in timeseriesSchemaList will be null
   * @param timeseriesSchemaList List<timeseriesSchema> result: [name, alias, storage group,
   *                             dataType, encoding, compression, offset, lastTimeStamp]
   */
  private void findPath(MNode node, String[] nodes, int idx, List<String[]> timeseriesSchemaList,
      boolean hasLimit, boolean needLast, QueryContext queryContext) throws MetadataException {
    if (node instanceof MeasurementMNode && nodes.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
      String nodeName;
      if (node.getName().contains(TsFileConstant.PATH_SEPARATOR)) {
        nodeName = "\"" + node + "\"";
      } else {
        nodeName = node.getName();
      }
      String nodePath = node.getParent().getFullPath() + TsFileConstant.PATH_SEPARATOR + nodeName;
      String[] tsRow = new String[8];
      tsRow[0] = nodePath;
      tsRow[1] = ((MeasurementMNode) node).getAlias();
      MeasurementSchema measurementSchema = ((MeasurementMNode) node).getSchema();
      tsRow[2] = getStorageGroupName(nodePath);
      tsRow[3] = measurementSchema.getType().toString();
      tsRow[4] = measurementSchema.getEncodingType().toString();
      tsRow[5] = measurementSchema.getCompressor().toString();
      tsRow[6] = String.valueOf(((MeasurementMNode) node).getOffset());
      tsRow[7] =
          needLast ? String.valueOf(getLastTimeStamp((MeasurementMNode) node, queryContext)) : null;
      timeseriesSchemaList.add(tsRow);

      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, timeseriesSchemaList, hasLimit, needLast,
            queryContext);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findPath(child, nodes, idx + 1, timeseriesSchemaList, hasLimit, needLast, queryContext);
        if (hasLimit) {
          if (count.get().intValue() == limit.get().intValue()) {
            return;
          }
        }
      }
    }
  }

  static long getLastTimeStamp(MeasurementMNode node, QueryContext queryContext) {
    TimeValuePair last = node.getCachedLast();
    if (last != null) {
      return node.getCachedLast().getTimestamp();
    } else {
      try {
        last = calculateLastPairForOneSeriesLocally(new Path(node.getFullPath()),
            node.getSchema().getType(), queryContext, Collections.emptySet());
        return last.getTimestamp();
      } catch (Exception e) {
        logger.error("Something wrong happened while trying to get last time value pair of {}",
            node.getFullPath(), e);
        return Long.MIN_VALUE;
      }
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
  Set<String> getChildNodePathInNextLevel(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    Set<String> childNodePaths = new TreeSet<>();
    findChildNodePathInNextLevel(root, nodes, 1, "", childNodePaths, nodes.length + 1);
    return childNodePaths;
  }

  /**
   * Traverse the MTree to match all child node path in next level
   *
   * @param node   the current traversing node
   * @param nodes  split the prefix path with '.'
   * @param idx    the current index of array nodes
   * @param parent store the node string having traversed
   * @param res    store all matched device names
   * @param length expected length of path
   */
  private void findChildNodePathInNextLevel(
      MNode node, String[] nodes, int idx, String parent, Set<String> res, int length) {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(parent + node.getName());
      } else {
        findChildNodePathInNextLevel(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, res, length);
      }
    } else {
      if (node.getChildren().size() > 0) {
        for (MNode child : node.getChildren().values()) {
          if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
            continue;
          }
          if (idx == length) {
            res.add(parent + node.getName());
          } else {
            findChildNodePathInNextLevel(
                child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, res, length);
          }
        }
      } else if (idx == length) {
        String nodeName;
        if (node.getName().contains(TsFileConstant.PATH_SEPARATOR)) {
          nodeName = "\"" + node + "\"";
        } else {
          nodeName = node.getName();
        }
        res.add(parent + nodeName);
      }
    }
  }

  /**
   * Get all devices under give path
   *
   * @return a list contains all distinct devices names
   */
  Set<String> getDevices(String prefixPath) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(prefixPath);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath);
    }
    Set<String> devices = new TreeSet<>();
    findDevices(root, nodes, 1, devices);
    return devices;
  }

  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node  the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx   the current index of array nodes
   * @param res   store all matched device names
   */
  private void findDevices(MNode node, String[] nodes, int idx, Set<String> res) {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        if (node.getChild(nodeReg) instanceof MeasurementMNode) {
          res.add(node.getFullPath());
        } else {
          findDevices(node.getChild(nodeReg), nodes, idx + 1, res);
        }
      }
    } else {
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode && !deviceAdded) {
          res.add(node.getFullPath());
          deviceAdded = true;
        }
        findDevices(child, nodes, idx + 1, res);
      }
    }
  }

  /**
   * Get all paths from root to the given level.
   */
  List<String> getNodesList(String path, int nodeLevel) throws MetadataException {
    return getNodesList(path, nodeLevel, null);
  }

  /**
   * Get all paths from root to the given level
   */
  List<String> getNodesList(String path, int nodeLevel, StorageGroupFilter filter)
      throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (!nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    List<String> res = new ArrayList<>();
    MNode node = root;
    for (int i = 1; i < nodes.length; i++) {
      if (node.getChild(nodes[i]) != null) {
        node = node.getChild(nodes[i]);
        if (node instanceof StorageGroupMNode && filter != null && !filter
            .satisfy(node.getFullPath())) {
          return res;
        }
      } else {
        throw new MetadataException(nodes[i - 1] + " does not have the child node " + nodes[i]);
      }
    }
    findNodes(node, path, res, nodeLevel - (nodes.length - 1), filter);
    return res;
  }

  /**
   * Get all paths under the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private void findNodes(MNode node, String path, List<String> res, int targetLevel,
      StorageGroupFilter filter) {
    if (node == null || node instanceof StorageGroupMNode && filter != null && !filter
        .satisfy(node.getFullPath())) {
      return;
    }
    if (targetLevel == 0) {
      res.add(path);
      return;
    }
    for (MNode child : node.getChildren().values()) {
      findNodes(child, path + PATH_SEPARATOR + child.toString(), res, targetLevel - 1, filter);
    }
  }

  public void serializeTo(String snapshotPath) throws IOException {
    try (BufferedWriter bw = new BufferedWriter(
        new FileWriter(SystemFileFactory.INSTANCE.getFile(snapshotPath)))) {
      root.serializeTo(bw);
    }
  }

  public static MTree deserializeFrom(File mtreeSnapshot) {
    try (BufferedReader br = new BufferedReader(new FileReader(mtreeSnapshot))) {
      String s;
      Deque<MNode> nodeStack = new ArrayDeque<>();
      MNode node = null;

      while ((s = br.readLine()) != null) {
        String[] nodeInfo = s.split(",");
        short nodeType = Short.parseShort(nodeInfo[0]);
        if (nodeType == MetadataConstant.STORAGE_GROUP_MNODE_TYPE) {
          node = StorageGroupMNode.deserializeFrom(nodeInfo);
        } else if (nodeType == MetadataConstant.MEASUREMENT_MNODE_TYPE) {
          node = MeasurementMNode.deserializeFrom(nodeInfo);
        } else {
          node = new MNode(null, nodeInfo[1]);
        }

        int childrenSize = Integer.parseInt(nodeInfo[nodeInfo.length - 1]);
        if (childrenSize == 0) {
          nodeStack.push(node);
        } else {
          Map<String, MNode> childrenMap = new LinkedHashMap<>();
          for (int i = 0; i < childrenSize; i++) {
            MNode child = nodeStack.removeFirst();
            child.setParent(node);
            childrenMap.put(child.getName(), child);
            if (child instanceof MeasurementMNode) {
              String alias = ((MeasurementMNode) child).getAlias();
              if (alias != null) {
                node.addAlias(alias, child);
              }
            }
          }
          node.setChildren(childrenMap);
          nodeStack.push(node);
        }
      }
      return new MTree(node);
    } catch (IOException e) {
      logger.warn("Failed to deserialize from {}. Use a new MTree.", mtreeSnapshot.getPath());
      return new MTree();
    } finally {
      limit = new ThreadLocal<>();
      offset = new ThreadLocal<>();
      count = new ThreadLocal<>();
      curOffset = new ThreadLocal<>();
    }
  }

  @Override
  public String toString() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(root.getName(), mNodeToJSON(root, null));
    return jsonToString(jsonObject);
  }

  private static String jsonToString(JSONObject jsonObject) {
    return JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat);
  }

  private JSONObject mNodeToJSON(MNode node, String storageGroupName) {
    JSONObject jsonObject = new JSONObject();
    if (node.getChildren().size() > 0) {
      if (node instanceof StorageGroupMNode) {
        storageGroupName = node.getFullPath();
      }
      for (MNode child : node.getChildren().values()) {
        jsonObject.put(child.getName(), mNodeToJSON(child, storageGroupName));
      }
    } else if (node instanceof MeasurementMNode) {
      MeasurementMNode leafMNode = (MeasurementMNode) node;
      jsonObject.put("DataType", leafMNode.getSchema().getType());
      jsonObject.put("Encoding", leafMNode.getSchema().getEncodingType());
      jsonObject.put("Compressor", leafMNode.getSchema().getCompressor());
      jsonObject.put("args", leafMNode.getSchema().getProps().toString());
      jsonObject.put("StorageGroup", storageGroupName);
    }
    return jsonObject;
  }

  /**
   * combine multiple metadata in string format
   */
  static String combineMetadataInStrings(String[] metadataStrs) {
    JSONObject[] jsonObjects = new JSONObject[metadataStrs.length];
    for (int i = 0; i < jsonObjects.length; i++) {
      jsonObjects[i] = JSONObject.parseObject(metadataStrs[i]);
    }

    JSONObject root = jsonObjects[0];
    for (int i = 1; i < jsonObjects.length; i++) {
      root = combineJSONObjects(root, jsonObjects[i]);
    }
    return jsonToString(root);
  }

  private static JSONObject combineJSONObjects(JSONObject a, JSONObject b) {
    JSONObject res = new JSONObject();

    Set<String> retainSet = new HashSet<>(a.keySet());
    retainSet.retainAll(b.keySet());
    Set<String> aCha = new HashSet<>(a.keySet());
    Set<String> bCha = new HashSet<>(b.keySet());
    aCha.removeAll(retainSet);
    bCha.removeAll(retainSet);
    for (String key : aCha) {
      res.put(key, a.getJSONObject(key));
    }
    for (String key : bCha) {
      res.put(key, b.get(key));
    }
    for (String key : retainSet) {
      Object v1 = a.get(key);
      Object v2 = b.get(key);
      if (v1 instanceof JSONObject && v2 instanceof JSONObject) {
        res.put(key, combineJSONObjects((JSONObject) v1, (JSONObject) v2));
      } else {
        res.put(key, v1);
      }
    }
    return res;
  }

  Map<String, String> determineStorageGroup(String path) throws IllegalPathException {
    Map<String, String> paths = new HashMap<>();
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }

    Deque<MNode> nodeStack = new ArrayDeque<>();
    Deque<Integer> depthStack = new ArrayDeque<>();
    if (!root.getChildren().isEmpty()) {
      nodeStack.push(root);
      depthStack.push(0);
    }

    while (!nodeStack.isEmpty()) {
      MNode mNode = nodeStack.removeFirst();
      int depth = depthStack.removeFirst();

      determineStorageGroup(depth + 1, nodes, mNode, paths, nodeStack, depthStack);
    }
    return paths;
  }

  /**
   * Try determining the storage group using the children of a mNode. If one child is a storage
   * group node, put a storageGroupName-fullPath pair into paths. Otherwise put the children that
   * match the path into the queue and discard other children.
   */
  private void determineStorageGroup(
      int depth,
      String[] nodes,
      MNode mNode,
      Map<String, String> paths,
      Deque<MNode> nodeStack,
      Deque<Integer> depthStack) {
    String currNode = depth >= nodes.length ? PATH_WILDCARD : nodes[depth];
    for (Entry<String, MNode> entry : mNode.getChildren().entrySet()) {
      if (!currNode.equals(PATH_WILDCARD) && !currNode.equals(entry.getKey())) {
        continue;
      }
      // this child is desired
      MNode child = entry.getValue();
      if (child instanceof StorageGroupMNode) {
        // we have found one storage group, record it
        String sgName = child.getFullPath();
        // concat the remaining path with the storage group name
        StringBuilder pathWithKnownSG = new StringBuilder(sgName);
        for (int i = depth + 1; i < nodes.length; i++) {
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(nodes[i]);
        }
        if (depth >= nodes.length - 1 && currNode.equals(PATH_WILDCARD)) {
          // the we find the sg at the last node and the last node is a wildcard (find "root
          // .group1", for "root.*"), also append the wildcard (to make "root.group1.*")
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(PATH_WILDCARD);
        }
        paths.put(sgName, pathWithKnownSG.toString());
      } else if (!child.getChildren().isEmpty()) {
        // push it back so we can traver its children later
        nodeStack.push(child);
        depthStack.push(depth);
      }
    }
  }
}
