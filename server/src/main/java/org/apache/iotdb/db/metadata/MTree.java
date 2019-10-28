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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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
  private static final String DOUB_SEPARATOR = "\\.";
  private static final String NO_CHILD_ERROR = "Timeseries is not correct. Node[%s] "
      + "doesn't have child named:%s";
  private static final String NOT_LEAF_NODE = "Timeseries %s is not the leaf node";
  private static final String SERIES_NOT_CORRECT = "Timeseries %s is not correct";
  private static final String NOT_SERIES_PATH = "The prefix of the seriesPath %s is not one storage group seriesPath";
  private MNode root;

  MTree(String rootName) {
    this.root = new MNode(rootName, null, false);
  }

  public MTree(MNode root) {
    this.root = root;
  }

  /**
   * this is just for compatibility
   */
  void addTimeseriesPath(String timeseriesPath, String dataType, String encoding)
      throws PathErrorException {
    TSDataType tsDataType = TSDataType.valueOf(dataType);
    TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
    CompressionType compressionType = CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
    addTimeseriesPath(timeseriesPath, tsDataType, tsEncoding, compressionType,
        Collections.emptyMap());
  }

  /**
   * function for adding timeseries.It should check whether seriesPath exists.
   */
  void addTimeseriesPath(String timeseriesPath, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws PathErrorException {
    String[] nodeNames = getNodeNames(timeseriesPath);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not right.", timeseriesPath));
    }
    MNode cur = findLeafParent(nodeNames);
    String levelPath = cur.getDataFileName();

    MNode leaf = new MNode(nodeNames[nodeNames.length - 1], cur, dataType, encoding, compressor);
    if (props != null && !props.isEmpty()) {
      leaf.getSchema().setProps(props);
    }
    leaf.setDataFileName(levelPath);
    if (cur.isLeaf()) {
      throw new PathErrorException(
          String.format("The Node [%s] is left node, the timeseries %s can't be created",
              cur.getName(), timeseriesPath));
    }
    cur.addChild(nodeNames[nodeNames.length - 1], leaf);
  }

  /**
   * function for adding deviceId
   */
  MNode addDeviceId(String deviceId) throws PathErrorException {
    String[] nodeNames = getNodeNames(deviceId);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not right.", deviceId));
    }
    MNode cur = getRoot();
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
      }
      cur = cur.getChild(nodeNames[i]);
    }
    return cur;
  }

  private MNode findLeafParent(String[] nodeNames) throws PathErrorException {
    MNode cur = root;
    String levelPath = null;
    int i = 1;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur.isStorageGroup()) {
        levelPath = cur.getDataFileName();
      }
      if (!cur.hasChild(nodeName)) {
        if (cur.isLeaf()) {
          throw new PathErrorException(
              String.format("The Node [%s] is left node, the timeseries %s can't be created",
                  cur.getName(), String.join(",", nodeNames)));
        }
        cur.addChild(nodeName, new MNode(nodeName, cur, false));
      }
      cur.setDataFileName(levelPath);
      cur = cur.getChild(nodeName);
      if (levelPath == null) {
        levelPath = cur.getDataFileName();
      }
      i++;
    }
    cur.setDataFileName(levelPath);
    return cur;
  }


  /**
   * function for checking whether the given path exists.
   *
   * @param path -seriesPath not necessarily the whole seriesPath (possibly a prefix of a sequence)
   */
  boolean isPathExist(String path) {
    String[] nodeNames;
    nodeNames = getNodeNames(path);
    MNode cur = root;
    int i = 0;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur.getName().equals(nodeName)) {
        i++;
        nodeName = nodeNames[i];
        if (cur.hasChild(nodeName)) {
          cur = cur.getChild(nodeName);
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    return cur.getName().equals(nodeNames[i]);
  }

  private String[] getNodeNames(String path) {
    String[] nodeNames;
    path = path.trim();
    if(path.contains("\"") || path.contains("\'")){
      String[] deviceAndMeasurement;
      if(path.contains("\"")){
        deviceAndMeasurement = path.split("\"");
      }else {
        deviceAndMeasurement = path.split("\'");
      }
      String device = deviceAndMeasurement[0];
      String measurement = deviceAndMeasurement[1];
      int nodeNumber = device.split(DOUB_SEPARATOR).length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(device.split(DOUB_SEPARATOR), 0, nodeNames, 0, nodeNumber - 1);
      nodeNames[nodeNumber - 1] = measurement;
    }else{
      nodeNames = path.split(DOUB_SEPARATOR);
    }
    return nodeNames;
  }

  /**
   * function for checking whether the given path exists under the given mnode.
   */
  boolean isPathExist(MNode node, String path) {
    String[] nodeNames = getNodeNames(path);
    if (nodeNames.length < 1) {
      return true;
    }
    if (!node.hasChild(nodeNames[0])) {
      return false;
    }
    MNode cur = node.getChild(nodeNames[0]);

    int i = 0;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur.getName().equals(nodeName)) {
        i++;
        nodeName = nodeNames[i];
        if (cur.hasChild(nodeName)) {
          cur = cur.getChild(nodeName);
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    return cur.getName().equals(nodeNames[i]);
  }

  /**
   * make sure check seriesPath before setting storage group.
   */
  public void setStorageGroup(String path) throws StorageGroupException {
    String[] nodeNames = getNodeNames(path);
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new StorageGroupException(
          String.format("The storage group can't be set to the %s node", path));
    }
    int i = 1;
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        // add one child node
        cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
      } else if (temp.isStorageGroup()) {
        // before set storage group should check the seriesPath exist or not
        // throw exception
        throw new StorageGroupException(
            String.format("The prefix of %s has been set to the storage group.", path));
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    MNode temp = cur.getChild(nodeNames[i]);
    if (temp == null) {
      cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
    } else {
      throw new StorageGroupException(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              path));
    }
    cur = cur.getChild(nodeNames[i]);
    cur.setStorageGroup(true);
    setDataFileName(path, cur);
  }

  public void deleteStorageGroup(String path) throws PathErrorException {
    MNode cur = getNodeByPath(path);
    if (!cur.isStorageGroup()) {
      throw new PathErrorException(String.format("The path %s is not a deletable storage group", path));
    }
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    while (cur != null && !MetadataConstant.ROOT.equals(cur.getName()) && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
  }

  /**
   * Check whether the input path is storage group or not
   * @param path input path
   * @return if it is storage group, return true. Else return false
   * @apiNote :for cluster
   */
  boolean checkStorageGroup(String path) {
    String[] nodeNames = getNodeNames(path);
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      return false;
    }
    int i = 1;
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null || temp.isStorageGroup()) {
        return false;
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    MNode temp = cur.getChild(nodeNames[i]);
    return temp != null && temp.isStorageGroup();
  }

  /**
   * Check whether set file seriesPath for this node or not. If not, throw an exception
   */
  private void checkStorageGroup(MNode node) throws StorageGroupException {
    if (node.getDataFileName() != null) {
      throw new StorageGroupException(
          String.format("The storage group %s has been set", node.getDataFileName()));
    }
    if (node.getChildren() == null) {
      return;
    }
    for (MNode child : node.getChildren().values()) {
      checkStorageGroup(child);
    }
  }

  private void setDataFileName(String path, MNode node) {
    node.setDataFileName(path);
    if (node.getChildren() == null) {
      return;
    }
    for (MNode child : node.getChildren().values()) {
      setDataFileName(path, child);
    }
  }

  /**
   * Delete one seriesPath from current Metadata Tree.
   *
   * @param path Format: root.node.(node)* Notice: Path must be a complete Path from root to leaf
   * node.
   */
  String deletePath(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException("Timeseries %s is not correct." + path);
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            String.format(NO_CHILD_ERROR,cur.getName(),nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }

    // if the storage group node is deleted, the dataFileName should be
    // return
    String dataFileName = null;
    if (cur.isStorageGroup()) {
      dataFileName = cur.getDataFileName();
    }
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    while (cur != null && !MetadataConstant.ROOT.equals(cur.getName()) && cur.getChildren().size() == 0) {
      if (cur.isStorageGroup()) {
        dataFileName = cur.getDataFileName();
        return dataFileName;
      }
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }

    return dataFileName;
  }

  /**
   * Check whether the seriesPath given exists.
   */
  public boolean hasPath(String path) {
    String[] nodes = getNodeNames(path);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      return false;
    }
    return hasPath(getRoot(), nodes, 1);
  }

  private boolean hasPath(MNode node, String[] nodes, int idx) {
    if (idx >= nodes.length) {
      return true;
    }
    if (("*").equals(nodes[idx])) {
      boolean res = false;
      for (MNode child : node.getChildren().values()) {
        res |= hasPath(child, nodes, idx + 1);
      }
      return res;
    } else {
      if (node.hasChild(nodes[idx])) {
        return hasPath(node.getChild(nodes[idx]), nodes, idx + 1);
      }
      return false;
    }
  }

  /**
   * Get ColumnSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  MeasurementSchema getSchemaForOnePath(String path) throws PathErrorException {
    MNode leaf = getLeafByPath(path);
    return leaf.getSchema();
  }

  MeasurementSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {
    MNode leaf = getLeafByPath(node, path);
    return leaf.getSchema();
  }

  MeasurementSchema getSchemaForOnePathWithCheck(MNode node, String path)
      throws PathErrorException {
    MNode leaf = getLeafByPathWithCheck(node, path);
    return leaf.getSchema();
  }

  MeasurementSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {
    MNode leaf = getLeafByPathWithCheck(path);
    return leaf.getSchema();
  }

  private MNode getLeafByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] node = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i < node.length; i++) {
      cur = cur.getChild(node[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format(NOT_LEAF_NODE, path));
    }
    return cur;
  }

  private MNode getLeafByPath(MNode node, String path) throws PathErrorException {
    checkPath(node, path);
    String[] nodes = getNodeNames(path);
    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format(NOT_LEAF_NODE, path));
    }
    return cur;
  }

  private MNode getLeafByPathWithCheck(MNode node, String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length < 1 || !node.hasChild(nodes[0])) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, path));
    }

    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            String.format(NO_CHILD_ERROR,cur.getName(),nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format(NOT_LEAF_NODE, path));
    }
    return cur;
  }

  private MNode getLeafByPathWithCheck(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, path));
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            String.format(NO_CHILD_ERROR,cur.getName(),nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format(NOT_LEAF_NODE, path));
    }
    return cur;
  }

  /**
   * function for getting node by path.
   */
  MNode getNodeByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] node = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i < node.length; i++) {
      cur = cur.getChild(node[i]);
    }
    return cur;
  }

  /**
   * function for getting node by path with file level check.
   */
  MNode getNodeByPathWithFileLevelCheck(String path) throws PathErrorException, StorageGroupException {
    boolean fileLevelChecked = false;
    String[] nodes = getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, path));
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        if (!fileLevelChecked) {
          throw new StorageGroupException("Storage group is not set for current seriesPath:" + path);
        }
        throw new PathErrorException(String.format(NO_CHILD_ERROR, cur.getName(), nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
      if (cur.isStorageGroup()) {
        fileLevelChecked = true;
      }
    }
    if (!fileLevelChecked) {
      throw new StorageGroupException("Storage group is not set for current seriesPath:" + path);
    }
    return cur;
  }

  /**
   * Extract the deviceType from given seriesPath.
   *
   * @return String represents the deviceId
   */
  String getDeviceTypeByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] nodes = getNodeNames(path);
    if (nodes.length < 2) {
      throw new PathErrorException(
          String.format("Timeseries %s must have two or more nodes", path));
    }
    return nodes[0] + "." + nodes[1];
  }

  /**
   * Check whether a seriesPath is available.
   *
   * @return last node in given seriesPath if current seriesPath is available
   */
  private MNode checkPath(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, path));
    }
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException("Path: \"" + path + "\" doesn't correspond to any known time series");
      }
      cur = cur.getChild(nodes[i]);
    }
    return cur;
  }

  private void checkPath(MNode node, String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length < 1) {
      return;
    }
    MNode cur = node;
    for (String node1 : nodes) {
      if (!cur.hasChild(node1)) {
        throw new PathErrorException(
            String.format(NO_CHILD_ERROR, cur.getName(), node1));
      }
      cur = cur.getChild(node1);
    }
  }

  /**
   * Get the storage group seriesPath from the seriesPath.
   *
   * @return String storage group seriesPath
   */
  String getStorageGroupNameByPath(String path) throws StorageGroupException {

    String[] nodes = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new StorageGroupException(String.format(NOT_SERIES_PATH, path));
      } else if (cur.isStorageGroup()) {
        return cur.getDataFileName();
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur.isStorageGroup()) {
      return cur.getDataFileName();
    }
    throw new StorageGroupException(String.format(NOT_SERIES_PATH, path));
  }

  /**
   * Get all the storage group seriesPaths for one seriesPath.
   *
   * @return List storage group seriesPath list
   * @apiNote :for cluster
   */
  List<String> getAllFileNamesByPath(String pathReg) throws PathErrorException {
    ArrayList<String> fileNames = new ArrayList<>();
    String[] nodes = getNodeNames(pathReg);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, pathReg));
    }
    findFileName(getRoot(), nodes, 1, "", fileNames);
    return fileNames;
  }

  /**
   * Recursively find all fileName according to a specific path
   * @apiNote :for cluster
   */
  private void findFileName(MNode node, String[] nodes, int idx, String parent,
      ArrayList<String> paths) {
    if (node.isStorageGroup()) {
      paths.add(node.getDataFileName());
      return;
    }
    String nodeReg;
    if (idx >= nodes.length) {
      nodeReg = "*";
    } else {
      nodeReg = nodes[idx];
    }

    if (!("*").equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findFileName(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findFileName(child, nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    }
  }

  /**
   * function for getting file name by path.
   */
  String getStorageGroupNameByPath(MNode node, String path) throws StorageGroupException {

    String[] nodes = getNodeNames(path);
    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new StorageGroupException(String.format(NOT_SERIES_PATH, path));
      } else if (cur.isStorageGroup()) {
        return cur.getDataFileName();
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur.isStorageGroup()) {
      return cur.getDataFileName();
    }
    throw new StorageGroupException(String.format(NOT_SERIES_PATH, path));
  }

  /**
   * Check the prefix of this seriesPath is storage group seriesPath.
   *
   * @return true the prefix of this seriesPath is storage group seriesPath false the prefix of this
   * seriesPath is not storage group seriesPath
   */
  boolean checkFileNameByPath(String path) {
    String[] nodes = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i <= nodes.length; i++) {
      if (cur == null) {
        return false;
      } else if (cur.isStorageGroup()) {
        return true;
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    return false;
  }

  /**
   * Get all paths for given seriesPath regular expression Regular expression in this method is
   * formed by the amalgamation of seriesPath and the character '*'.
   *
   * @return A HashMap whose Keys are separated by the storage file name.
   */
  HashMap<String, ArrayList<String>> getAllPath(String pathReg) throws PathErrorException {
    HashMap<String, ArrayList<String>> paths = new HashMap<>();
    String[] nodes = getNodeNames(pathReg);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, pathReg));
    }
    findPath(getRoot(), nodes, 1, "", paths);
    return paths;
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   */
  List<List<String>> getShowTimeseriesPath(String pathReg) throws PathErrorException {
    List<List<String>> res = new ArrayList<>();
    String[] nodes = getNodeNames(pathReg);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format(SERIES_NOT_CORRECT, pathReg));
    }
    findPath(getRoot(), nodes, 1, "", res);
    return res;
  }

  /**
   * function for getting leaf node path in the next level of the given path.
   *
   * @return All leaf nodes' seriesPath(s) of given seriesPath.
   */
  List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    List<String> ret = new ArrayList<>();
    MNode cur = checkPath(path);
    for (MNode child : cur.getChildren().values()) {
      if (child.isLeaf()) {
        ret.add(path + "." + child.getName());
      }
    }
    return ret;
  }

  /**
   * function for getting all paths in list.
   */
  ArrayList<String> getAllPathInList(String path) throws PathErrorException {
    ArrayList<String> res = new ArrayList<>();
    HashMap<String, ArrayList<String>> mapRet = getAllPath(path);
    for (ArrayList<String> value : mapRet.values()) {
      res.addAll(value);
    }
    return res;
  }

  /**
   * Calculate the count of storage-level nodes included in given seriesPath.
   *
   * @return The total count of storage-level nodes.
   */
  int getFileCountForOneType(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot()
        .hasChild(nodes[1])) {
      throw new PathErrorException(
          "Timeseries must be " + getRoot().getName()
              + ". X (X is one of the nodes of root's children)");
    }
    return getFileCountForOneNode(getRoot().getChild(nodes[1]));
  }

  private int getFileCountForOneNode(MNode node) {

    if (node.isStorageGroup()) {
      return 1;
    }
    int sum = 0;
    if (!node.isLeaf()) {
      for (MNode child : node.getChildren().values()) {
        sum += getFileCountForOneNode(child);
      }
    }
    return sum;
  }

  /**
   * Get all device type in current Metadata Tree.
   *
   * @return a list contains all distinct device type
   */
  ArrayList<String> getAllType() {
    ArrayList<String> res = new ArrayList<>();
    if (getRoot() != null) {
      res.addAll(getRoot().getChildren().keySet());
    }
    return res;
  }

  /**
   * Get all storage groups in current Metadata Tree.
   *
   * @return a list contains all distinct storage groups
   */
  Set<String> getAllStorageGroup() {
    HashSet<String> res = new HashSet<>();
    MNode rootNode;
    if ((rootNode = getRoot()) != null) {
      findStorageGroup(rootNode, "root", res);
    }
    return res;
  }

  private void findStorageGroup(MNode node, String path, HashSet<String> res) {
    if (node.isStorageGroup()) {
      res.add(path);
      return;
    }
    for (MNode childNode : node.getChildren().values()) {
      findStorageGroup(childNode, path + "." + childNode.toString(), res);
    }
  }

  /**
   * Get all devices in current Metadata Tree.
   *
   * @return a list contains all distinct devices
   */
  Set<String> getAllDevices() {
    HashSet<String> devices = new HashSet<>();
    MNode node;
    if ((node = getRoot()) != null) {
      findDevices(node, SQLConstant.ROOT, devices);
    }
    return new LinkedHashSet<>(devices);
  }

  private void findDevices(MNode node, String path, HashSet<String> res) {
    if (node == null) {
      return;
    }
    if (node.isLeaf()) {
      res.add(path);
      return;
    }
    for (MNode child : node.getChildren().values()) {
      if (child.isLeaf()) {
        res.add(path);
      } else {
        findDevices(child, path + "." + child.toString(), res);
      }
    }
  }

  /**
   * Get all nodes at the given level in current Metadata Tree.
   *
   * @return a list contains all nodes at the given level
   */
  List<String> getNodesList(String schemaPattern, int nodeLevel) throws SQLException {
    List<String> res = new ArrayList<>();
    String[] nodes = schemaPattern.split("\\.");
    MNode node;
    if ((node = getRoot()) != null) {
      if (nodes[0].equals("root")) {
        for (int i = 1; i < nodes.length; i++) {
          if (node.getChild(nodes[i]) != null) {
            node = node.getChild(nodes[i]);
          } else {
            throw new SQLException(nodes[i - 1] + " does not have the child node " + nodes[i]);
          }
        }
        findNodes(node, schemaPattern, res, nodeLevel - (nodes.length - 1));
      } else {
        throw new SQLException("Incorrect root node " + nodes[0] + " selected");
      }
    }
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
        findNodes(child, path + "." + child.toString(), res, targetLevel - 1);
      }
    }
  }

  /**
   * Get all delta objects for given type.
   *
   * @param type device Type
   * @return a list contains all delta objects for given type
   */
  ArrayList<String> getDeviceForOneType(String type) throws PathErrorException {
    String path = getRoot().getName() + "." + type;
    checkPath(path);
    HashMap<String, Integer> deviceMap = new HashMap<>();
    MNode typeNode = getRoot().getChild(type);
    putDeviceToMap(getRoot().getName(), typeNode, deviceMap);
    return new ArrayList<>(deviceMap.keySet());
  }

  private void putDeviceToMap(String path, MNode node, HashMap<String, Integer> deviceMap) {
    if (node.isLeaf()) {
      deviceMap.put(path, 1);
    } else {
      for (String child : node.getChildren().keySet()) {
        String newPath = path + "." + node.getName();
        putDeviceToMap(newPath, node.getChildren().get(child), deviceMap);
      }
    }
  }

  /**
   * Get all ColumnSchemas for given delta object type.
   *
   * @param path A seriesPath represented one Delta object
   * @return a list contains all column schema
   */
  ArrayList<MeasurementSchema> getSchemaForOneType(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot()
        .hasChild(nodes[1])) {
      throw new PathErrorException(
          "Timeseries must be " + getRoot().getName()
              + ". X (X is one of the nodes of root's children)");
    }
    HashMap<String, MeasurementSchema> leafMap = new HashMap<>();
    putLeafToLeafMap(getRoot().getChild(nodes[1]), leafMap);
    return new ArrayList<>(leafMap.values());
  }

  /**
   * Get all ColumnSchemas for the storage group seriesPath.
   *
   * @return ArrayList<  ColumnSchema  > The list of the schema
   */
  ArrayList<MeasurementSchema> getSchemaForOneStorageGroup(String path) {

    String[] nodes = getNodeNames(path);
    HashMap<String, MeasurementSchema> leafMap = new HashMap<>();
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    // cur is the storage group node
    putLeafToLeafMap(cur, leafMap);
    return new ArrayList<>(leafMap.values());
  }

  /**
   * function for getting schema map for one storage group.
   */
  Map<String, MeasurementSchema> getSchemaMapForOneStorageGroup(String path) {
    String[] nodes = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    return cur.getSchemaMap();
  }

  /**
   * function for getting num schema map for one file node.
   */
  Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {
    String[] nodes = getNodeNames(path);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    return cur.getNumSchemaMap();
  }

  private void putLeafToLeafMap(MNode node, HashMap<String, MeasurementSchema> leafMap) {
    if (node.isLeaf()) {
      if (!leafMap.containsKey(node.getName())) {
        leafMap.put(node.getName(), node.getSchema());
      }
      return;
    }
    for (MNode child : node.getChildren().values()) {
      putLeafToLeafMap(child, leafMap);
    }
  }

  private void findPath(MNode node, String[] nodes, int idx, String parent,
      HashMap<String, ArrayList<String>> paths) {
    if (node.isLeaf()) {
      if (nodes.length <= idx) {
        String fileName = node.getDataFileName();
        String nodeName;
        if(node.getName().contains(TsFileConstant.PATH_SEPARATOR)){
          nodeName = "\"" + node + "\"";
        }else{
          nodeName = "" + node;
        }
        String nodePath = parent + nodeName;
        putAPath(paths, fileName, nodePath);
      }
      return;
    }
    String nodeReg;
    if (idx >= nodes.length) {
      nodeReg = "*";
    } else {
      nodeReg = nodes[idx];
    }

    if (!("*").equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findPath(child, nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    }
  }

  /*
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   */
  private void findPath(MNode node, String[] nodes, int idx, String parent,
      List<List<String>> res) {
    if (node.isLeaf()) {
      if (nodes.length <= idx) {
        String nodePath = parent + node;
        List<String> tsRow = new ArrayList<>(4);// get [name,storage group,resultDataType,encoding]
        tsRow.add(nodePath);
        MeasurementSchema measurementSchema = node.getSchema();
        tsRow.add(node.getDataFileName());
        tsRow.add(measurementSchema.getType().toString());
        tsRow.add(measurementSchema.getEncodingType().toString());
        res.add(tsRow);
      }
      return;
    }
    String nodeReg;
    if (idx >= nodes.length) {
      nodeReg = "*";
    } else {
      nodeReg = nodes[idx];
    }

    if (!("*").equals(nodeReg)) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", res);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findPath(child, nodes, idx + 1, parent + node.getName() + ".", res);
      }
    }
  }

  private void putAPath(HashMap<String, ArrayList<String>> paths, String fileName,
      String nodePath) {
    if (paths.containsKey(fileName)) {
      paths.get(fileName).add(nodePath);
    } else {
      ArrayList<String> pathList = new ArrayList<>();
      pathList.add(nodePath);
      paths.put(fileName, pathList);
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
    jsonObject.put(getRoot().getName(), mnodeToJSON(getRoot()));
    return jsonObject;
  }

  private JSONObject mnodeToJSON(MNode node) {
    JSONObject jsonObject = new JSONObject();
    if (!node.isLeaf() && node.getChildren().size() > 0) {
      for (MNode child : node.getChildren().values()) {
        jsonObject.put(child.getName(), mnodeToJSON(child));
      }
    } else if (node.isLeaf()) {
      jsonObject.put("DataType", node.getSchema().getType());
      jsonObject.put("Encoding", node.getSchema().getEncodingType());
      jsonObject.put("Compressor", node.getSchema().getCompressor());
      jsonObject.put("args", node.getSchema().getProps().toString());
      jsonObject.put("StorageGroup", node.getDataFileName());
    }
    return jsonObject;
  }

  public MNode getRoot() {
    return root;
  }

  /**
   * combine multiple metadata in string format
   */
  static String combineMetadataInStrings(String[] metadatas) {
    JSONObject[] jsonObjects = new JSONObject[metadatas.length];
    for (int i = 0; i < jsonObjects.length; i++) {
      jsonObjects[i] = JSONObject.parseObject(metadatas[i]);
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
