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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 */
public class MTree implements Serializable {

  private static final long serialVersionUID = -4200394435237291964L;
  private MNode root;

  MTree() {
    this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
  }

  /**
   * Create a timeseries with a full path from root to leaf node Before creating a timeseries, the
   * storage group should be set first, throw exception otherwise
   *
   * @param path timeseries path
   * @param dataType data type
   * @param encoding encoding
   * @param compressor compressor
   * @param props props
   */
  void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    if (nodeNames.length <= 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, path = root.sg.d1.s1,  create internal node root -> sg -> d1
    for (int i = 1; i < nodeNames.length - 1; i++) {
      String nodeName = nodeNames[i];
      if (cur instanceof StorageGroupMNode) {
        hasSetStorageGroup = true;
      }
      if (!cur.hasChild(nodeName)) {
        if (cur instanceof LeafMNode) {
          throw new PathAlreadyExistException(cur.getFullPath());
        } else if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        cur.addChild(new InternalMNode(cur, nodeName));
      }
      cur = cur.getChild(nodeName);
    }
    MNode leaf = new LeafMNode(cur, nodeNames[nodeNames.length - 1], dataType, encoding,
        compressor, props);
    cur.addChild(leaf);
  }

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  MNode getDeviceNodeWithAutoCreating(String deviceId) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(deviceId);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(deviceId);
    }
    MNode cur = root;
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        cur.addChild(new InternalMNode(cur, nodeNames[i]));
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
        cur.addChild(new InternalMNode(cur, nodeNames[i]));
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
      StorageGroupMNode storageGroupMNode = new StorageGroupMNode(cur, nodeNames[i], path,
          IoTDBDescriptor.getInstance().getConfig().getDefaultTTL(), new HashMap<>());
      cur.addChild(storageGroupMNode);
    }
  }

  /**
   * Delete a storage group
   */
  void deleteStorageGroup(String path) throws MetadataException {
    MNode cur = getNodeByPath(path);
    if (!(cur instanceof StorageGroupMNode)) {
      throw new StorageGroupNotSetException(path);
    }
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    // delete node b while retain root.a.sg2
    while (!IoTDBConstant.PATH_ROOT.equals(cur.getName()) && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
  }

  /**
   * Check whether path is storage group or not
   *
   * e.g., path = root.a.b.sg. if nor a and b is StorageGroupMNode and sg is a StorageGroupMNode
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

  String deleteTimeseriesAndReturnEmptyStorageGroup(String path) throws MetadataException {
    MNode curNode = getNodeByPath(path);
    if (!(curNode instanceof LeafMNode)) {
      throw new PathNotExistException(path);
    }
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path);
    }
    // delete the last node of path
    curNode.getParent().deleteChild(curNode.getName());
    curNode = curNode.getParent();
    // delete all empty ancestors except storage group
    while (!IoTDBConstant.PATH_ROOT.equals(curNode.getName())
        && curNode.getChildren().size() == 0) {
      // if current storage group has no time series, return the storage group name
      if (curNode instanceof StorageGroupMNode) {
        return curNode.getFullPath();
      }
      curNode.getParent().deleteChild(curNode.getName());
      curNode = curNode.getParent();
    }
    return null;
  }

  /**
   * Get measurement schema for a given path. Path must be a complete Path from root to leaf node.
   */
  MeasurementSchema getSchema(String path) throws MetadataException {
    MNode node = getNodeByPath(path);
    if (!(node instanceof LeafMNode)) {
      throw new PathNotExistException(path);
    }
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
  private void findStorageGroup(MNode node, String[] nodes, int idx, String parent,
      List<String> storageGroupNames) {
    if (node instanceof StorageGroupMNode) {
      storageGroupNames.add(node.getFullPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findStorageGroup(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroup(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR,
            storageGroupNames);
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
      } else if (current instanceof InternalMNode) {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Get storage group name by path
   *
   * e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  String getStorageGroupName(String path) throws MetadataException {
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
    List<String[]> res = getAllTimeseriesSchema(prefixPath);
    List<String> paths = new ArrayList<>();
    for (String[] p : res) {
      paths.add(p[0]);
    }
    return paths;
  }

  /**
   * Get all time series schema under the given path
   *
   * timeseriesSchema: [name, storage group, dataType, encoding, compression]
   */
  List<String[]> getAllTimeseriesSchema(String prefixPath) throws MetadataException {
    List<String[]> res = new ArrayList<>();
    String[] nodes = MetaUtils.getNodeNames(prefixPath);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath);
    }
    findPath(root, nodes, 1, "", res);
    return res;
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   *
   * @param timeseriesSchemaList List<timeseriesSchema>
   */
  private void findPath(MNode node, String[] nodes, int idx, String parent,
      List<String[]> timeseriesSchemaList) throws MetadataException {
    if (node instanceof LeafMNode) {
      if (nodes.length <= idx) {
        String nodeName;
        if (node.getName().contains(TsFileConstant.PATH_SEPARATOR)) {
          nodeName = "\"" + node + "\"";
        } else {
          nodeName = node.getName();
        }
        String nodePath = parent + nodeName;
        String[] tsRow = new String[5];
        tsRow[0] = nodePath;
        MeasurementSchema measurementSchema = node.getSchema();
        tsRow[1] = getStorageGroupName(nodePath);
        tsRow[2] = measurementSchema.getType().toString();
        tsRow[3] = measurementSchema.getEncodingType().toString();
        tsRow[4] = measurementSchema.getCompressor().toString();
        timeseriesSchemaList.add(tsRow);
      }
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR,
            timeseriesSchemaList);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findPath(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR,
            timeseriesSchemaList);
      }
    }
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1, return
   * [root.sg1.d1, root.sg1.d2]
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
   * @param node the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx the current index of array nodes
   * @param parent store the node string having traversed
   * @param res store all matched device names
   * @param length expected length of path
   */
  private void findChildNodePathInNextLevel(MNode node, String[] nodes, int idx, String parent,
      Set<String> res, int length) {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(parent + node.getName());
      } else {
        findChildNodePathInNextLevel(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, res, length);
      }
    } else {
      if (node instanceof InternalMNode) {
        for (MNode child : node.getChildren().values()) {
          if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
            continue;
          }
          if (idx == length) {
            res.add(parent + node.getName());
          } else {
            findChildNodePathInNextLevel(child, nodes, idx + 1,
                parent + node.getName() + PATH_SEPARATOR, res, length);
          }
        }
      } else if (idx == length) {
        res.add(parent + node.getName());
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
    findDevices(root, nodes, 1, "", devices);
    return devices;
  }

  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx the current index of array nodes
   * @param parent store the node string having traversed
   * @param res store all matched device names
   */
  private void findDevices(MNode node, String[] nodes, int idx, String parent, Set<String> res) {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        if (node.getChild(nodeReg) instanceof LeafMNode) {
          res.add(parent + node.getName());
        } else {
          findDevices(node.getChild(nodeReg), nodes, idx + 1,
              parent + node.getName() + PATH_SEPARATOR, res);
        }
      }
    } else {
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof LeafMNode && !deviceAdded) {
          res.add(parent + node.getName());
          deviceAdded = true;
        } else if (!(child instanceof LeafMNode)) {
          findDevices(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, res);
        }
      }
    }
  }

  /**
   * Get all paths from root to the given level
   */
  List<String> getNodesList(String path, int nodeLevel) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (!nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    List<String> res = new ArrayList<>();
    MNode node = root;
    for (int i = 1; i < nodes.length; i++) {
      if (node.getChild(nodes[i]) != null) {
        node = node.getChild(nodes[i]);
      } else {
        throw new MetadataException(nodes[i - 1] + " does not have the child node " + nodes[i]);
      }
    }
    findNodes(node, path, res, nodeLevel - (nodes.length - 1));
    return res;
  }

  private void findNodes(MNode node, String path, List<String> res, int targetLevel) {
    if (node == null) {
      return;
    }
    if (targetLevel == 0) {
      res.add(path);
      return;
    }
    if (node instanceof InternalMNode) {
      for (MNode child : node.getChildren().values()) {
        findNodes(child, path + PATH_SEPARATOR + child.toString(), res, targetLevel - 1);
      }
    }
  }

  /**
   * Get all ColumnSchemas for the storage group path.
   *
   * @return ArrayList<ColumnSchema> The list of the schema
   */
  List<MeasurementSchema> getStorageGroupSchema(String storageGroup) throws MetadataException {
    StorageGroupMNode storageGroupMNode = getStorageGroupNode(storageGroup);
    return new ArrayList<>(storageGroupMNode.getSchemaMap().values());
  }

  /**
   * Get schema map for the storage group
   *
   * measurement -> measurementSchema
   */
  Map<String, MeasurementSchema> getStorageGroupSchemaMap(String storageGroup)
      throws MetadataException {
    StorageGroupMNode storageGroupMNode = getStorageGroupNode(storageGroup);
    return storageGroupMNode.getSchemaMap();
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
    } else if (node instanceof LeafMNode) {
      jsonObject.put("DataType", node.getSchema().getType());
      jsonObject.put("Encoding", node.getSchema().getEncodingType());
      jsonObject.put("Compressor", node.getSchema().getCompressor());
      jsonObject.put("args", node.getSchema().getProps().toString());
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
  private void determineStorageGroup(int depth, String[] nodes, MNode mNode,
      Map<String, String> paths, Deque<MNode> nodeStack, Deque<Integer> depthStack) {
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
