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
import java.util.Set;
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

  MTree(String rootName) {
    this.root = new InternalMNode(rootName, null);
  }

  /**
   * Add path
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
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    String storageGroupName = null;
    int i = 1;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur instanceof StorageGroupMNode) {
        storageGroupName = cur.getStorageGroupName();
      }
      if (!cur.hasChild(nodeName)) {
        if (cur instanceof LeafMNode) {
          throw new PathAlreadyExistException(cur.getFullPath());
        }
        cur.addChild(new InternalMNode(nodeName, cur));
      }
      cur.setStorageGroupName(storageGroupName);
      cur = cur.getChild(nodeName);
      if (storageGroupName == null) {
        storageGroupName = cur.getStorageGroupName();
      }
      i++;
    }
    cur.setStorageGroupName(storageGroupName);
    MNode leaf = new LeafMNode(nodeNames[nodeNames.length - 1], cur, dataType, encoding,
        compressor, props);
    leaf.setStorageGroupName(cur.getStorageGroupName());
    cur.addChild(leaf);
  }

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * @param path device id
   */
  MNode addInternalPath(String path) throws MetadataException {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        cur.addChild(new InternalMNode(nodeNames[i], cur));
      }
      cur = cur.getChild(nodeNames[i]);
    }
    return cur;
  }

  /**
   * Check whether the given path exists.
   *
   * @param path not necessary to be the whole path (possibly a prefix of a sequence), but must
   * start from root
   */
  boolean isPathExist(String path) {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    MNode cur = root;
    int i = 0;
    while (i < nodeNames.length - 1) {
      i++;
      String nodeName = nodeNames[i];
      if (cur.hasChild(nodeName)) {
        cur = cur.getChild(nodeName);
      } else {
        return false;
      }
    }
    return cur.getName().equals(nodeNames[i]);
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
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        cur.addChild(new InternalMNode(nodeNames[i], cur));
      } else if (temp instanceof StorageGroupMNode) {
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    MNode temp = cur.getChild(nodeNames[i]);
    if (temp == null) {
      cur.addChild(new StorageGroupMNode(nodeNames[i], cur,
          IoTDBDescriptor.getInstance().getConfig().getDefaultTTL(), new HashMap<>()));
    } else {
      throw new PathAlreadyExistException(temp.getFullPath());
    }
    setStorageGroup(path, cur.getChild(nodeNames[i]));
  }

  /**
   * Set storage group of MNode children recursively
   *
   * @param path path
   * @param node MNode
   */
  private void setStorageGroup(String path, MNode node) {
    node.setStorageGroupName(path);
    if (node.getChildren() == null) {
      return;
    }
    for (MNode child : node.getChildren().values()) {
      setStorageGroup(path, child);
    }
  }

  /**
   * Delete storage group
   *
   * @param path path
   */
  void deleteStorageGroup(String path) throws MetadataException {
    MNode cur = getNodeByPath(path);
    if (!(cur instanceof StorageGroupMNode)) {
      throw new StorageGroupNotSetException(path);
    }
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    while (cur != null && !MetadataConstant.ROOT.equals(cur.getName())
        && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
  }

  /**
   * Check whether path is storage group or not
   *
   * @param path path
   * @apiNote :for cluster
   */
  boolean isStorageGroup(String path) {
    String[] nodeNames = MetaUtils.getNodeNames(path);
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      return false;
    }
    int i = 1;
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null || temp instanceof StorageGroupMNode) {
        return false;
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    MNode temp = cur.getChild(nodeNames[i]);
    return temp != null && temp instanceof StorageGroupMNode;
  }

  /**
   * Delete path. Notice: Path must be a complete Path from root to leaf
   *
   * @param path Format: root.node.(node)* node.
   */
  String deletePath(String path) throws MetadataException {
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

    // if the storage group node is deleted, the storageGroupName should be return
    String storageGroupName = null;
    if (cur instanceof StorageGroupMNode) {
      storageGroupName = cur.getStorageGroupName();
    }
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    while (cur != null && !MetadataConstant.ROOT.equals(cur.getName())
        && cur.getChildren().size() == 0) {
      if (cur instanceof StorageGroupMNode) {
        storageGroupName = cur.getStorageGroupName();
        return storageGroupName;
      }
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }

    return storageGroupName;
  }

  /**
   * Get ColumnSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  MeasurementSchema getSchema(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathNotExistException(path);
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!(cur instanceof LeafMNode)) {
      throw new PathNotExistException(path);
    }
    return cur.getSchema();
  }

  /**
   * Get node by path with storage group check
   */
  MNode getNodeByPathWithStorageGroupCheck(String path) throws MetadataException {
    boolean storageGroupChecked = false;
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(root.getName())) {
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
   * Get storage group node
   */
  StorageGroupMNode getStorageGroupNode(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }

    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathNotExistException(path);
      }
      cur = cur.getChild(nodes[i]);

      if (cur instanceof StorageGroupMNode) {
        return (StorageGroupMNode) cur;
      }
    }
    throw new StorageGroupNotSetException(path);
  }

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  MNode getNodeByPath(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(root.getName())) {
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
   * Get all storage groups for path. path could be only a prefix
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
      List<String> paths) {
    if (node instanceof StorageGroupMNode) {
      paths.add(node.getStorageGroupName());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findStorageGroup(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, paths);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroup(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, paths);
      }
    }
  }

  /**
   * Get all storage groups in current Metadata Tree.
   *
   * @return a list contains all distinct storage groups
   */
  List<String> getAllStorageGroupNames() {
    List<String> res = new ArrayList<>();
    if (root != null) {
      findStorageGroup(root, "root", res);
    }
    return res;
  }

  private void findStorageGroup(MNode node, String path, List<String> res) {
    if (node instanceof StorageGroupMNode) {
      res.add(path);
      return;
    }
    for (MNode childNode : node.getChildren().values()) {
      findStorageGroup(childNode, path + PATH_SEPARATOR + childNode.toString(), res);
    }
  }

  /**
   * @return all storage groups' MNodes
   */
  List<StorageGroupMNode> getAllStorageGroupNodes() {
    List<StorageGroupMNode> ret = new ArrayList<>();
    Deque<MNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      MNode current = nodeStack.pop();
      if (current instanceof StorageGroupMNode) {
        ret.add((StorageGroupMNode) current);
      } else if (current.hasChildren()) {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Get storage group name by path
   *
   * @return String storage group path
   */
  String getStorageGroupName(String path) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(path);
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new StorageGroupNotSetException(path);
      } else if (cur instanceof StorageGroupMNode) {
        return cur.getStorageGroupName();
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur instanceof StorageGroupMNode) {
      return cur.getStorageGroupName();
    }
    throw new StorageGroupNotSetException(path);
  }

  /**
   * Check the prefix of this seriesPath is storage group seriesPath.
   *
   * @return true the prefix of this seriesPath is storage group seriesPath false the prefix of this
   * seriesPath is not storage group seriesPath
   */
  boolean checkStorageGroupByPath(String path) {
    String[] nodes = MetaUtils.getNodeNames(path);
    MNode cur = root;
    for (int i = 1; i <= nodes.length; i++) {
      if (cur == null) {
        return false;
      } else if (cur instanceof StorageGroupMNode) {
        return true;
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    return false;
  }

  /**
   * Get all paths under the given path
   *
   * @param path RE in this method is formed by the amalgamation of path and character '*'.
   */
  List<String> getAllPath(String path) throws MetadataException {
    List<List<String>> res = getShowTimeseriesPath(path);
    List<String> paths = new ArrayList<>();
    for (List<String> p : res) {
      paths.add(p.get(0));
    }
    return paths;
  }

  /**
   * Get all paths under the given path
   */
  List<List<String>> getShowTimeseriesPath(String path) throws MetadataException {
    List<List<String>> res = new ArrayList<>();
    String[] nodes = MetaUtils.getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    findPath(root, nodes, 1, "", res);
    return res;
  }

  /*
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   */
  private void findPath(MNode node, String[] nodes, int idx, String parent,
      List<List<String>> res) {
    if (node instanceof LeafMNode) {
      if (nodes.length <= idx) {
        String nodeName;
        if (node.getName().contains(TsFileConstant.PATH_SEPARATOR)) {
          nodeName = "\"" + node + "\"";
        } else {
          nodeName = node.toString();
        }
        String nodePath = parent + nodeName;
        List<String> tsRow = new ArrayList<>(5);// get [name,storage group,resultDataType,encoding]
        tsRow.add(nodePath);
        MeasurementSchema measurementSchema = node.getSchema();
        tsRow.add(node.getStorageGroupName());
        tsRow.add(measurementSchema.getType().toString());
        tsRow.add(measurementSchema.getEncodingType().toString());
        tsRow.add(measurementSchema.getCompressor().toString());
        res.add(tsRow);
      }
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR,
            res);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findPath(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, res);
      }
    }
  }

  /**
   * function for getting child node path in the next level of the given path.
   *
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildNodePathInNextLevel(String path) throws MetadataException {
    Set<String> ret = new HashSet<>();
    String[] nodes = MetaUtils.getNodeNames(path);
    if (!nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path);
    }
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathNotExistException(path);
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.hasChildren()) {
      throw new PathNotExistException(path);
    }
    for (MNode child : cur.getChildren().values()) {
      ret.add(path + PATH_SEPARATOR + child.getName());
    }
    return ret;
  }

  /**
   * Get all device type in current Metadata Tree.
   *
   * @return a list contains all distinct device type
   */
  Map<String, List<String>> getDeviceIdMap() throws MetadataException {
    Map<String, List<String>> deviceIdMap = new HashMap<>();
    for (String type : root.getChildren().keySet()) {
      String path = root.getName() + PATH_SEPARATOR + type;
      getNodeByPath(path);
      HashMap<String, Integer> deviceMap = new HashMap<>();
      MNode typeNode = root.getChild(type);
      putDeviceToMap(root.getName(), typeNode, deviceMap);

      deviceIdMap.put(type, new ArrayList<>(deviceMap.keySet()));
    }
    return deviceIdMap;
  }

  private void putDeviceToMap(String path, MNode node, HashMap<String, Integer> deviceMap) {
    if (node instanceof LeafMNode) {
      deviceMap.put(path, 1);
    } else {
      for (String child : node.getChildren().keySet()) {
        String newPath = path + PATH_SEPARATOR + node.getName();
        putDeviceToMap(newPath, node.getChildren().get(child), deviceMap);
      }
    }
  }

  /**
   * Get all devices in current Metadata Tree with prefixPath.
   *
   * @return a list contains all distinct devices names
   */
  List<String> getDevices(String prefixPath) throws MetadataException {
    String[] nodes = MetaUtils.getNodeNames(prefixPath);
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath);
    }
    List<String> devices = new ArrayList<>();
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
  private void findDevices(MNode node, String[] nodes, int idx, String parent, List<String> res) {
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
   * Get all nodes at the given level in current Metadata Tree.
   *
   * @return a list contains all nodes at the given level
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
    if (node.hasChildren()) {
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
  List<MeasurementSchema> getStorageGroupSchema(String path) {
    String[] nodes = MetaUtils.getNodeNames(path);
    HashMap<String, MeasurementSchema> leafMap = new HashMap<>();
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    // cur is the storage group node
    putLeafToLeafMap(cur, leafMap);
    return new ArrayList<>(leafMap.values());
  }

  /**
   * Get schema map for the storage group
   */
  Map<String, MeasurementSchema> getStorageGroupSchemaMap(String path) {
    String[] nodes = MetaUtils.getNodeNames(path);
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    return ((StorageGroupMNode) cur).getSchemaMap();
  }

  /**
   * Recursively put leaf to leaf map
   *
   * @param node node
   * @param leafMap leaf map
   */
  private void putLeafToLeafMap(MNode node, Map<String, MeasurementSchema> leafMap) {
    if (node instanceof LeafMNode) {
      if (!leafMap.containsKey(node.getName())) {
        leafMap.put(node.getName(), node.getSchema());
      }
      return;
    }
    for (MNode child : node.getChildren().values()) {
      putLeafToLeafMap(child, leafMap);
    }
  }

  @Override
  public String toString() {
    return jsonToString(toJson());
  }

  private static String jsonToString(JSONObject jsonObject) {
    return JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat);
  }

  private JSONObject toJson() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(root.getName(), mNodeToJSON(root));
    return jsonObject;
  }

  private JSONObject mNodeToJSON(MNode node) {
    JSONObject jsonObject = new JSONObject();
    if (node.getChildren().size() > 0) {
      for (MNode child : node.getChildren().values()) {
        jsonObject.put(child.getName(), mNodeToJSON(child));
      }
    } else if (node instanceof LeafMNode) {
      jsonObject.put("DataType", node.getSchema().getType());
      jsonObject.put("Encoding", node.getSchema().getEncodingType());
      jsonObject.put("Compressor", node.getSchema().getCompressor());
      jsonObject.put("args", node.getSchema().getProps().toString());
      jsonObject.put("StorageGroup", node.getStorageGroupName());
    }
    return jsonObject;
  }

  String getRoot() {
    return root.getName();
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
}
