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
import java.util.Arrays;
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
   * @param detachedPath       timeseries path
   * @param dataType   data type
   * @param encoding   encoding
   * @param compressor compressor
   * @param props      props
   * @param alias      alias of measurement
   */
  MeasurementMNode createTimeseries(
      List<String> detachedPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    if (detachedPath.size() <= 2 || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    MNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, path = root.sg.d1.s1,  create internal detachedPath and set cur to d1 node
    for (int i = 1; i < detachedPath.size() - 1; i++) {
      String nodeName = detachedPath.get(i);
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
    String leafName = detachedPath.get(detachedPath.size() - 1);
    if (cur.hasChild(leafName)) {
      throw new PathAlreadyExistException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    if (alias != null && cur.hasChild(alias)) {
      throw new AliasAlreadyExistException(MetaUtils.concatDetachedPathByDot(detachedPath), alias);
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
   * <p>e.g., get root.sg.d1, get or create all internal detachedPath and return the node of d1
   */
  MNode getDeviceNodeWithAutoCreating(List<String> nodeNames, int sgLevel) throws MetadataException {
    if (nodeNames.size() <= 1 || !nodeNames.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(nodeNames));
    }
    MNode cur = root;
    for (int i = 1; i < nodeNames.size(); i++) {
      if (!cur.hasChild(nodeNames.get(i))) {
        if (i == sgLevel) {
          cur.addChild(nodeNames.get(i), new StorageGroupMNode(cur, nodeNames.get(i),
              IoTDBDescriptor.getInstance().getConfig().getDefaultTTL()));
        } else {
          cur.addChild(nodeNames.get(i), new MNode(cur, nodeNames.get(i)));
        }
      }
      cur = cur.getChild(nodeNames.get(i));
    }
    return cur;
  }

  /**
   * Check whether the given path exists.
   *
   * @param nodeNames detachedPath of a full path or a prefix path
   */
  boolean isPathExist(List<String> nodeNames) {
    MNode cur = root;
    if (!nodeNames.get(0).equals(root.getName())) {
      return false;
    }
    for (int i = 1; i < nodeNames.size(); i++) {
      String childName = nodeNames.get(i);
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
   * @param detachedPath nodeNames
   */
  MNode setStorageGroup(List<String> detachedPath) throws MetadataException {
    MNode cur = root;
    StorageGroupMNode storageGroupMNode = null;
    if (detachedPath.size() <= 1 || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    int i = 1;
    // e.g., path = root.a.b.sg, create internal detachedPath for a, b
    while (i < detachedPath.size() - 1) {
      MNode temp = cur.getChild(detachedPath.get(i));
      if (temp == null) {
        cur.addChild(detachedPath.get(i), new MNode(cur, detachedPath.get(i)));
      } else if (temp instanceof StorageGroupMNode) {
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(detachedPath.get(i));
      i++;
    }
    if (cur.hasChild(detachedPath.get(i))) {
      // node b has child sg
      throw new StorageGroupAlreadySetException(MetaUtils.concatDetachedPathByDot(detachedPath));
    } else {
      storageGroupMNode =
          new StorageGroupMNode(
              cur, detachedPath.get(i), IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
      cur.addChild(detachedPath.get(i), storageGroupMNode);
    }
    return storageGroupMNode;
  }

  /**
   * Delete a storage group
   */
  List<MeasurementMNode> deleteStorageGroup(List<String> detachedPath) throws MetadataException {
    MNode cur = getMNodeByDetachedPath(detachedPath);
    if (!(cur instanceof StorageGroupMNode)) {
      throw new StorageGroupNotSetException(cur.getFullPath());
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
   * @param nodeNames path
   * @apiNote :for cluster
   */
  boolean isStorageGroup(List<String> nodeNames) {
    if (nodeNames.size() <= 1 || !nodeNames.get(0).equals(IoTDBConstant.PATH_ROOT)) {
      return false;
    }
    MNode cur = root;
    int i = 1;
    while (i < nodeNames.size() - 1) {
      cur = cur.getChild(nodeNames.get(i));
      if (cur == null || cur instanceof StorageGroupMNode) {
        return false;
      }
      i++;
    }
    cur = cur.getChild(nodeNames.get(i));
    return cur instanceof StorageGroupMNode;
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param detachedPath Format: [root, node, node]
   */
  Pair<String, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(List<String> detachedPath)
      throws MetadataException {
    MNode curNode = getMNodeByDetachedPath(detachedPath);
    if (!(curNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }

    if (detachedPath.isEmpty() || !IoTDBConstant.PATH_ROOT.equals(detachedPath.get(0))) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
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
  MeasurementSchema getSchema(List<String> detachedPath) throws MetadataException {
    MeasurementMNode node = (MeasurementMNode) getMNodeByDetachedPath(detachedPath);
    return node.getSchema();
  }

  /**
   * Get node by path with storage group check If storage group is not set,
   * StorageGroupNotSetException will be thrown
   */
  MNode getMNodeByDetachedPathWithStorageGroupCheck(List<String> detachedPath) throws MetadataException {
    boolean storageGroupChecked = false;
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }

    MNode cur = root;
    for (int i = 1; i < detachedPath.size(); i++) {
      if (!cur.hasChild(detachedPath.get(i))) {
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
        }
        throw new PathNotExistException(MetaUtils.concatDetachedPathByDot(detachedPath));
      }
      cur = cur.getChild(detachedPath.get(i));

      if (cur instanceof StorageGroupMNode) {
        storageGroupChecked = true;
      }
    }

    if (!storageGroupChecked) {
      throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    return cur;
  }

  /**
   * Get storage group node, if the give path is not a storage group, throw exception
   */
  StorageGroupMNode getStorageGroupMNode(List<String> detachedPath) throws MetadataException {
    MNode node = getMNodeByDetachedPath(detachedPath);
    if (node instanceof StorageGroupMNode) {
      return (StorageGroupMNode) node;
    } else {
      throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
  }

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  MNode getMNodeByDetachedPath(List<String> detachedPath) throws MetadataException {
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    MNode cur = root;
    for (int i = 1; i < detachedPath.size(); i++) {
      if (!cur.hasChild(detachedPath.get(i))) {
        throw new PathNotExistException(MetaUtils.concatDetachedPathByDot(detachedPath));
      }
      cur = cur.getChild(detachedPath.get(i));
    }
    return cur;
  }

  /**
   * Get all storage groups under the given path
   *
   * @return storage group list
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByDetachedPath(List<String> detachedPath) throws MetadataException {
    List<String> storageGroups = new ArrayList<>();
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    findStorageGroup(root, detachedPath.toArray(new String[0]), 1, "", storageGroups);
    return storageGroups;
  }

  /**
   * Recursively find all storage group according to a specific path
   *
   * @apiNote :for cluster
   */
  private void findStorageGroup(
      MNode node, String[] detachedPath, int idx, String parent, List<String> storageGroupNames) {
    if (node instanceof StorageGroupMNode) {
      storageGroupNames.add(node.getFullPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findStorageGroup(
            node.getChild(nodeReg),
            detachedPath,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            storageGroupNames);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroup(
            child, detachedPath, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    }
  }

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  List<String> getAllDetachedStorageGroups() {
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
  List<StorageGroupMNode> getAllStorageGroupMNodes() {
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
  String getStorageGroup(List<String> detachedPath) throws StorageGroupNotSetException {
    MNode cur = root;
    for (int i = 1; i < detachedPath.size(); i++) {
      cur = cur.getChild(detachedPath.get(i));
      if (cur instanceof StorageGroupMNode) {
        return cur.getFullPath();
      } else if (cur == null) {
        throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
      }
    }
    throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
  }

  /**
   * Get storage group name by detachedPath
   *
   * <p>e.g., root.sg1 is storage group, detachedPath is [root, sg1, d1], return [root, sg1]
   *
   * @return storage group in the given detachedPath
   */
  List<String> getDetachedStorageGroup(List<String> detachedPath) throws StorageGroupNotSetException {
    MNode cur = root;
    List<String> detachedStorageGroup = new ArrayList<>();
    detachedStorageGroup.add(root.getName());
    for (int i = 1; i < detachedPath.size(); i++) {
      cur = cur.getChild(detachedPath.get(i));
      if (cur instanceof StorageGroupMNode) {
        detachedStorageGroup.add(cur.getName());
        break;
      } else if (cur == null) {
        throw new StorageGroupNotSetException(MetaUtils.concatDetachedPathByDot(detachedPath));
      } else {
        detachedStorageGroup.add(cur.getName());
      }
    }
    return detachedStorageGroup;
  }

  /**
   * Check whether the given path contains a storage group
   */
  boolean checkStorageGroupByPath(List<String> detachedPath) {
    MNode cur = root;
    for (int i = 1; i <= detachedPath.size(); i++) {
      cur = cur.getChild(detachedPath.get(i));
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
  List<String> getAllTimeseries(String prefixPath) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(new Path(prefixPath));
    List<String[]> res = getAllMeasurementSchema(plan);
    List<String> paths = new ArrayList<>();
    for (String[] p : res) {
      paths.add(p[0]);
    }
    return paths;
  }

  /**
   * Get all timeseries under the given path
   *
   * @param prefixPathNodes a prefix path or a full path, may contain '*'.
   */
  List<String> getAllTimeseries(List<String> prefixPathNodes) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(new Path(prefixPathNodes));
    List<String[]> res = getAllMeasurementSchemaByDetachedPath(plan);
    List<String> paths = new ArrayList<>();
    for (String[] p : res) {
      paths.add(p[0]);
    }
    return paths;
  }

  /**
   * Get all timeseries under the given path
   *
   * @param detachedPath a prefix path or a full path, may contain '*'.
   */
  List<List<String>> getAllDetachedTimeseries(List<String> detachedPath) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(new Path(detachedPath));
    return getAllDetachedTimeSeries(plan);
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
   * Get all timeseries paths under the given path
   *
   * @param detachedPath a node list
   */
  List<Path> getAllTimeseriesPath(List<String> detachedPath) throws MetadataException {
    Path prePath = new Path(detachedPath);
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prePath);
    List<List<String>> res = getAllMeasurementSchemaAndDetachedPathByDetachedPath(plan);
    List<Path> paths = new ArrayList<>();
    for (int i = 0; i < res.size(); i+=2) {
      Path path = new Path(res.get(i));
      if (detachedPath.get(detachedPath.size() -1).equals(res.get(i + 1).get(0))) {
        path.setAlias(res.get(i + 1).get(0));
      }
      paths.add(path);
    }
    return paths;
  }

  int getAllTimeseriesCount(List<String> detachedPath) throws MetadataException {
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    return getCount(root, detachedPath.toArray(new String[0]), 1);
  }

  /**
   * Get the count of detachedPath in the given level under the given prefix path.
   */
  int getNodesCountInGivenLevel(List<String> detachedPath, int level) throws MetadataException {
    if (detachedPath.isEmpty()|| !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    MNode mNode = root;
    for (int i = 1; i < detachedPath.size(); i++) {
      if (mNode.getChild(detachedPath.get(i)) != null) {
        mNode = mNode.getChild(detachedPath.get(i));
      } else {
        throw new MetadataException(detachedPath.get(i -  1) + " does not have the child node " + detachedPath.get(i));
      }
    }
    return getCountInGivenLevel(mNode, level - (detachedPath.size() - 1));
  }

  /**
   * Traverse the MTree to get the count of timeseries.
   */
  private int getCount(MNode mNode, String[] detachedPath, int idx) throws MetadataException {
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (mNode.hasChild(nodeReg)) {
        if (mNode.getChild(nodeReg) instanceof MeasurementMNode) {
          return 1;
        } else {
          return getCount(mNode.getChild(nodeReg), detachedPath, idx + 1);
        }
      } else {
        throw new MetadataException(mNode.getName() + " does not have the child node " + nodeReg);
      }
    } else {
      int cnt = 0;
      for (MNode child : mNode.getChildren().values()) {
        if (child instanceof MeasurementMNode) {
          cnt++;
        }
        cnt += getCount(child, detachedPath, idx + 1);
      }
      return cnt;
    }
  }

  /**
   * Traverse the MTree to get the count of timeseries in the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private int getCountInGivenLevel(MNode mNode, int targetLevel) {
    if (targetLevel == 0) {
      return 1;
    }
    int cnt = 0;
    for (MNode child : mNode.getChildren().values()) {
      cnt += getCountInGivenLevel(child, targetLevel - 1);
    }
    return cnt;
  }

  /**
   * Get all time series schema under the given path order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */

  List<String[]> getAllMeasurementSchemaByHeatOrder(ShowTimeSeriesPlan plan, QueryContext queryContext)
      throws MetadataException {
    String[] detachedPath = plan.getPath().getDetachedPath().toArray(new String[0]);
    if (detachedPath.length == 0 || !detachedPath[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    List<String[]> allMatchedMNodes = new ArrayList<>();

    findPath(root, detachedPath, 1, allMatchedMNodes, false, true, queryContext);

    Stream<String[]> sortedStream = allMatchedMNodes.stream().sorted(
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
    String[] detachedPath = plan.getPath().getDetachedPath().toArray(new String[0]);
    if (detachedPath.length == 0 || !detachedPath[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    if (offset.get() != 0 || limit.get() != 0) {
      res = new LinkedList<>();
      findPath(root, detachedPath, 1, res, true, false, null);
    } else {
      res = new LinkedList<>();
      findPath(root, detachedPath, 1, res, false, false, null);
    }
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    return res;
  }

  /**
   * Get all time series schema under the given detachedPath
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  private List<String[]> getAllMeasurementSchemaByDetachedPath(ShowTimeSeriesPlan plan) throws MetadataException {
    List<String[]> res;
    List<String> detachedPath = plan.getPath().getDetachedPath();
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    if (offset.get() != 0 || limit.get() != 0) {
      res = new LinkedList<>();
      findPath(root, detachedPath.toArray(new String[0]), 1, res, true, false, null);
    } else {
      res = new LinkedList<>();
      findPath(root, detachedPath.toArray(new String[0]), 1, res, false, false, null);
    }
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    return res;
  }

  /**
   * Get all time series schema and path name detachedPath under the given detachedPath
   *
   * <p>result: [[root, node], [alias, storage group, dataType, encoding, compression, offset]]
   */
  private List<List<String>> getAllMeasurementSchemaAndDetachedPathByDetachedPath(ShowTimeSeriesPlan plan) throws MetadataException {
    List<List<String>> res;
    List<String> detachedPath = plan.getPath().getDetachedPath();
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    if (offset.get() != 0 || limit.get() != 0) {
      res = new LinkedList<>();
      findDetachedPathAndSchema(root, detachedPath.toArray(new String[0]), 1, res, true, false, null);
    } else {
      res = new LinkedList<>();
      findDetachedPathAndSchema(root, detachedPath.toArray(new String[0]), 1, res, false, false, null);
    }
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    return res;
  }

  /**
   * Get all time series under the given detachedPath
   *
   * <p>result: [[root, node], [root, node]]
   */
  private List<List<String>> getAllDetachedTimeSeries(ShowTimeSeriesPlan plan) throws MetadataException {
    List<List<String>> res;
    List<String> detachedPath = plan.getPath().getDetachedPath();
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    if (offset.get() != 0 || limit.get() != 0) {
      res = new ArrayList<>(2);
      findDetachedPath(root, detachedPath.toArray(new String[0]), 1, res, true);
    } else {
      res = new ArrayList<>(2);
      findDetachedPath(root, detachedPath.toArray(new String[0]), 1, res, false);
    }
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    return res;
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf detachedPath under the given seriesPath
   *
   * @param needLast             if false, lastTimeStamp in timeseriesSchemaList will be null
   * @param timeseriesSchemaList List<timeseriesSchema> result: [name, alias, storage group,
   *                             dataType, encoding, compression, offset, lastTimeStamp]
   */
  private void findPath(MNode mNode, String[] detachedPath, int idx, List<String[]> timeseriesSchemaList,
      boolean hasLimit, boolean needLast, QueryContext queryContext) throws MetadataException {
    if (mNode instanceof MeasurementMNode && detachedPath.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
      String nodeName;
      if (mNode.getName().contains(TsFileConstant.PATH_SEPARATOR)) {
        nodeName = "\"" + mNode + "\"";
      } else {
        nodeName = mNode.getName();
      }
      String nodePath = mNode.getParent().getFullPath() + TsFileConstant.PATH_SEPARATOR + nodeName;
      detachedPath[detachedPath.length - 1] = nodeName;
      List<String> fullPath = new ArrayList<>(Arrays.asList(detachedPath));
      MNode temp = mNode;
      while (temp.getParent() != null) {
        temp = temp.getParent();
        fullPath.add(0, temp.getName());
      }
      String[] tsRow = new String[8];
      tsRow[0] = nodePath;
      tsRow[1] = ((MeasurementMNode) mNode).getAlias();
      MeasurementSchema measurementSchema = ((MeasurementMNode) mNode).getSchema();
      tsRow[2] = getStorageGroup(fullPath);
      tsRow[3] = measurementSchema.getType().toString();
      tsRow[4] = measurementSchema.getEncodingType().toString();
      tsRow[5] = measurementSchema.getCompressor().toString();
      tsRow[6] = String.valueOf(((MeasurementMNode) mNode).getOffset());
      tsRow[7] =
          needLast ? String.valueOf(getLastTimeStamp((MeasurementMNode) mNode, queryContext)) : null;
      timeseriesSchemaList.add(tsRow);

      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (mNode.hasChild(nodeReg)) {
        findPath(mNode.getChild(nodeReg), detachedPath, idx + 1, timeseriesSchemaList, hasLimit, needLast,
            queryContext);
      }
    } else {
      for (MNode child : mNode.getChildren().values()) {
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findPath(child, detachedPath, idx + 1, timeseriesSchemaList, hasLimit, needLast, queryContext);
        if (hasLimit && count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
    }
  }


  /**
   * Iterate through MTree to fetch metadata info of all leaf detachedPath under the given seriesPath
   *
   * @param timeseriesSchemaList List<timeseriesSchema> result: [[root,node,node], [alias, storage group,
   *                             dataType, encoding, compression, offset, lastTimeStamp]]
   */
  private void findDetachedPath(MNode mNode, String[] detachedPath, int idx,
      List<List<String>> timeseriesSchemaList,
      boolean hasLimit) {
    if (mNode instanceof MeasurementMNode && detachedPath.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
      List<String> nodeNames = new ArrayList<>();
      nodeNames.add(0, mNode.getName());
      MNode tempMNode = mNode;
      while(!tempMNode.getName().equals(IoTDBConstant.PATH_ROOT)) {
        tempMNode = tempMNode.getParent();
        nodeNames.add(0, tempMNode.getName());
      }
      timeseriesSchemaList.add(nodeNames);
      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeNameReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!nodeNameReg.contains(PATH_WILDCARD)) {
      if (mNode.hasChild(nodeNameReg)) {
        findDetachedPath(mNode.getChild(nodeNameReg), detachedPath, idx + 1, timeseriesSchemaList, hasLimit);
      }
    } else {
      for (MNode child : mNode.getChildren().values()) {
        if (!Pattern.matches(nodeNameReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findDetachedPath(child, detachedPath, idx + 1, timeseriesSchemaList, hasLimit);
        if (hasLimit && count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
    }
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf detachedPath under the given seriesPath
   *
   * @param timeseriesSchemaList List<timeseriesSchema> result: [[root,node,node], [alias, storage group,
   *                             dataType, encoding, compression, offset, lastTimeStamp]]
   */
  private void findDetachedPathAndSchema(MNode mNode, String[] detachedPath, int idx,
      List<List<String>> timeseriesSchemaList,
      boolean hasLimit, boolean needLast, QueryContext queryContext) throws StorageGroupNotSetException {
    if (mNode instanceof MeasurementMNode && detachedPath.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
      List<String> detachedFullPath = new ArrayList<>();
      detachedFullPath.add(0, mNode.getName());
      MNode tempNode = mNode;
      while(!tempNode.getName().equals(IoTDBConstant.PATH_ROOT)) {
        tempNode = tempNode.getParent();
        detachedFullPath.add(0, tempNode.getName());
      }
      timeseriesSchemaList.add(detachedFullPath);
      List<String> others = new ArrayList<>(7);
      others.add(((MeasurementMNode) mNode).getAlias());
      MeasurementSchema measurementSchema = ((MeasurementMNode) mNode).getSchema();
      others.add(getStorageGroup(detachedFullPath));
      others.add(measurementSchema.getType().toString());
      others.add(measurementSchema.getEncodingType().toString());
      others.add(measurementSchema.getCompressor().toString());
      others.add(String.valueOf(((MeasurementMNode) mNode).getOffset()));
      String last =  needLast ? String.valueOf(getLastTimeStamp((MeasurementMNode) mNode, queryContext)) : null;
      others.add(last);
      timeseriesSchemaList.add(others);
      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeNameReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!nodeNameReg.contains(PATH_WILDCARD)) {
      if (mNode.hasChild(nodeNameReg)) {
        findDetachedPathAndSchema(mNode.getChild(nodeNameReg), detachedPath, idx + 1, timeseriesSchemaList, hasLimit, needLast, queryContext);
      }
    } else {
      for (MNode child : mNode.getChildren().values()) {
        if (!Pattern.matches(nodeNameReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findDetachedPathAndSchema(child, detachedPath, idx + 1, timeseriesSchemaList, hasLimit, needLast, queryContext);
        if (hasLimit && count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
    }
  }

  static long getLastTimeStamp(MeasurementMNode mNode, QueryContext queryContext) {
    TimeValuePair last = mNode.getCachedLast();
    if (last != null) {
      return mNode.getCachedLast().getTimestamp();
    } else {
      try {
        MNode temp = mNode;
        List<String> detachedPath = new ArrayList<>();
        detachedPath.add(temp.getName());
        while (temp.getParent() != null) {
          temp = temp.getParent();
          detachedPath.add(0, temp.getName());
        }
        last = calculateLastPairForOneSeriesLocally(new Path(detachedPath),
            mNode.getSchema().getType(), queryContext, Collections.emptySet());
        return last.getTimestamp();
      } catch (Exception e) {
        logger.error("Something wrong happened while trying to get last time value pair of {}",
            mNode.getFullPath(), e);
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
   * @return All child detachedPath' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildPathInNextLevel(List<String> detachedPath) throws MetadataException {
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    Set<String> childPaths = new TreeSet<>();
    findChildPathInNextLevel(root, detachedPath.toArray(new String[0]), 1, "", childPaths, detachedPath.size() + 1);
    return childPaths;
  }

  /**
   * Traverse the MTree to match all child node path in next level
   *
   * @param node   the current traversing node
   * @param detachedPath  split the prefix path with '.'
   * @param idx    the current index of array detachedPath
   * @param parent store the node string having traversed
   * @param res    store all matched device names
   * @param length expected length of path
   */
  private void findChildPathInNextLevel(
      MNode node, String[] detachedPath, int idx, String parent, Set<String> res, int length) {
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(parent + node.getName());
      } else {
        findChildPathInNextLevel(node.getChild(nodeReg), detachedPath, idx + 1,
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
            findChildPathInNextLevel(
                child, detachedPath, idx + 1, parent + node.getName() + PATH_SEPARATOR, res, length);
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
  Set<String> getDevices(List<String> detachedPath) throws MetadataException {
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    Set<String> devices = new TreeSet<>();
    findDevices(root, detachedPath.toArray(new String[0]), 1, devices);
    return devices;
  }

  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node  the current traversing node
   * @param detachedPath split the prefix path with '.'
   * @param idx   the current index of array detachedPath
   * @param res   store all matched device names
   */
  private void findDevices(MNode node, String[] detachedPath, int idx, Set<String> res) {
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        if (node.getChild(nodeReg) instanceof MeasurementMNode) {
          res.add(node.getFullPath());
        } else {
          findDevices(node.getChild(nodeReg), detachedPath, idx + 1, res);
        }
      }
    } else {
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode && !deviceAdded) {
          res.add(node.getFullPath());
          deviceAdded = true;
        }
        findDevices(child, detachedPath, idx + 1, res);
      }
    }
  }


  /**
   * Get all devices under detachedPath of give path
   *
   * @return a list contains all distinct devices names
   */
  Set<Path> getDevicesPath(List<String> detachedPath) throws MetadataException {
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    Set<Path> devices = new TreeSet<>();
    findDevicesPath(root, detachedPath.toArray(new String[0]), 1, devices, new ArrayList<>());
    return devices;
  }




  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node  the current traversing node
   * @param detachedPath split the prefix path with '.'
   * @param idx   the current index of array detachedPath
   * @param res   store all matched device names
   */
  private void findDevicesPath(MNode node, String[] detachedPath, int idx, Set<Path> res, List<String> curNodes) {
    String nodeReg = MetaUtils.getNodeNameRegByIdx(idx, detachedPath);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        if (node.getChild(nodeReg) instanceof MeasurementMNode) {
          curNodes.add(node.getName());
          Path path = new Path(new ArrayList<>(curNodes));
          path.setDevice(node.getFullPath());
          res.add(path);
        } else {
          curNodes.add(node.getName());
          findDevicesPath(node.getChild(nodeReg), detachedPath, idx + 1, res, curNodes);
        }
      }
    } else {
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        curNodes.add(idx - 1, node.getName());
        if (child instanceof MeasurementMNode && !deviceAdded) {
          Path path = new Path(new ArrayList<>(curNodes));
          path.setDevice(node.getFullPath());
          res.add(path);
          deviceAdded = true;
        }
        findDevicesPath(child, detachedPath, idx + 1, res, curNodes);
        curNodes.remove(curNodes.size() - 1);
      }
    }
  }

  /**
   * Get all paths from root to the given level
   */
  List<String> getNodeNamesList(List<String> detachedPath, int nodeLevel, StorageGroupFilter filter)
      throws MetadataException {
    if (!detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }
    List<String> res = new ArrayList<>();
    MNode mNode = root;
    for (int i = 1; i < detachedPath.size(); i++) {
      if (mNode.getChild(detachedPath.get(i)) != null) {
        mNode = mNode.getChild(detachedPath.get(i));
        if (mNode instanceof StorageGroupMNode && filter != null && !filter
            .satisfy(mNode.getFullPath())) {
          return res;
        }
      } else {
        throw new MetadataException(detachedPath.get(i) + " does not have the child node " + MetaUtils.concatDetachedPathByDot(detachedPath));
      }
    }
    findNodes(mNode, MetaUtils.concatDetachedPathByDot(detachedPath), res, nodeLevel - (detachedPath.size() - 1), filter);
    return res;
  }

  /**
   * Get all paths under the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private void findNodes(MNode mNode, String path, List<String> res, int targetLevel,
      StorageGroupFilter filter) {
    if (mNode == null || mNode instanceof StorageGroupMNode && filter != null && !filter
        .satisfy(mNode.getFullPath())) {
      return;
    }
    if (targetLevel == 0) {
      res.add(path);
      return;
    }
    for (MNode child : mNode.getChildren().values()) {
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
      Deque<MNode> mNodeStack = new ArrayDeque<>();
      MNode mNode = null;

      while ((s = br.readLine()) != null) {
        String[] nodeNameInfo = s.split(",");
        short nodeType = Short.parseShort(nodeNameInfo[0]);
        if (nodeType == MetadataConstant.STORAGE_GROUP_MNODE_TYPE) {
          mNode = StorageGroupMNode.deserializeFrom(nodeNameInfo);
        } else if (nodeType == MetadataConstant.MEASUREMENT_MNODE_TYPE) {
          mNode = MeasurementMNode.deserializeFrom(nodeNameInfo);
        } else {
          mNode = new MNode(null, nodeNameInfo[1]);
        }

        int childrenSize = Integer.parseInt(nodeNameInfo[nodeNameInfo.length - 1]);
        if (childrenSize == 0) {
          mNodeStack.push(mNode);
        } else {
          Map<String, MNode> childrenMap = new LinkedHashMap<>();
          for (int i = 0; i < childrenSize; i++) {
            MNode child = mNodeStack.removeFirst();
            child.setParent(mNode);
            childrenMap.put(child.getName(), child);
            if (child instanceof MeasurementMNode) {
              String alias = ((MeasurementMNode) child).getAlias();
              if (alias != null) {
                mNode.addAlias(alias, child);
              }
            }
          }
          mNode.setChildren(childrenMap);
          mNodeStack.push(mNode);
        }
      }
      return new MTree(mNode);
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

  private JSONObject mNodeToJSON(MNode mNode, String storageGroupName) {
    JSONObject jsonObject = new JSONObject();
    if (mNode.getChildren().size() > 0) {
      if (mNode instanceof StorageGroupMNode) {
        storageGroupName = mNode.getFullPath();
      }
      for (MNode child : mNode.getChildren().values()) {
        jsonObject.put(child.getName(), mNodeToJSON(child, storageGroupName));
      }
    } else if (mNode instanceof MeasurementMNode) {
      MeasurementMNode leafMNode = (MeasurementMNode) mNode;
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

  Map<String, String> determineStorageGroup(List<String> detachedPath) throws IllegalPathException {
    Map<String, String> paths = new HashMap<>();
    if (detachedPath.isEmpty() || !detachedPath.get(0).equals(root.getName())) {
      throw new IllegalPathException(MetaUtils.concatDetachedPathByDot(detachedPath));
    }

    Deque<MNode> mNodeStack = new ArrayDeque<>();
    Deque<Integer> depthStack = new ArrayDeque<>();
    if (!root.getChildren().isEmpty()) {
      mNodeStack.push(root);
      depthStack.push(0);
    }

    while (!mNodeStack.isEmpty()) {
      MNode mNode = mNodeStack.removeFirst();
      int depth = depthStack.removeFirst();

      determineStorageGroup(depth + 1, detachedPath.toArray(new String[0]), mNode, paths, mNodeStack, depthStack);
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
      String[] detachedPath,
      MNode mNode,
      Map<String, String> paths,
      Deque<MNode> mNodeStack,
      Deque<Integer> depthStack) {
    String currNode = depth >= detachedPath.length ? PATH_WILDCARD : detachedPath[depth];
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
        for (int i = depth + 1; i < detachedPath.length; i++) {
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(detachedPath[i]);
        }
        if (depth >= detachedPath.length - 1 && currNode.equals(PATH_WILDCARD)) {
          // the we find the sg at the last node and the last node is a wildcard (find "root
          // .group1", for "root.*"), also append the wildcard (to make "root.group1.*")
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(PATH_WILDCARD);
        }
        paths.put(sgName, pathWithKnownSG.toString());
      } else if (!child.getChildren().isEmpty()) {
        // push it back so we can traver its children later
        mNodeStack.push(child);
        depthStack.push(depth);
      }
    }
  }
}
