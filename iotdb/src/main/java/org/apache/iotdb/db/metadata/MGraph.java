/**
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;

/**
 * Metadata Graph consists of one {@code MTree} and several {@code PTree}.
 */
public class MGraph implements Serializable {

  private static final long serialVersionUID = 8214849219614352834L;
  private final String separator = "\\.";
  private MTree mtree;
  private HashMap<String, PTree> ptreeMap;

  public MGraph(String mtreeName) {
    mtree = new MTree(mtreeName);
    ptreeMap = new HashMap<>();
  }

  /**
   * Add a {@code PTree} to current {@code MGraph}.
   */
  public void addAPTree(String ptreeRootName) throws MetadataArgsErrorException {
    if (ptreeRootName.toLowerCase().equals("root")) {
      throw new MetadataArgsErrorException("Property Tree's root name should not be 'root'");
    }
    PTree ptree = new PTree(ptreeRootName, mtree);
    ptreeMap.put(ptreeRootName, ptree);
  }

  /**
   * Add a seriesPath to Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  public void addPathToMTree(String path, String dataType, String encoding, String[] args)
      throws PathErrorException, MetadataArgsErrorException {
    String[] nodes = path.trim().split(separator);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null");
    }
    mtree.addTimeseriesPath(path, dataType, encoding, args);
  }

  /**
   * Add a seriesPath to {@code PTree}.
   */
  public void addPathToPTree(String path) throws PathErrorException, MetadataArgsErrorException {
    String[] nodes = path.trim().split(separator);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null.");
    }
    String rootName = path.trim().split(separator)[0];
    if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.addPath(path);
    } else {
      throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
    }
  }

  /**
   * Delete seriesPath in current MGraph.
   *
   * @param path a seriesPath belongs to MTree or PTree
   */
  public String deletePath(String path) throws PathErrorException {
    String[] nodes = path.trim().split(separator);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null");
    }
    String rootName = path.trim().split(separator)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.deletePath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.deletePath(path);
      return null;
    } else {
      throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
    }
  }

  /**
   * Link a {@code MNode} to a {@code PNode} in current PTree.
   */
  public void linkMNodeToPTree(String path, String mpath) throws PathErrorException {
    String ptreeName = path.trim().split(separator)[0];
    if (!ptreeMap.containsKey(ptreeName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
    } else {
      ptreeMap.get(ptreeName).linkMNode(path, mpath);
    }
  }

  /**
   * Unlink a {@code MNode} from a {@code PNode} in current PTree.
   */
  public void unlinkMNodeFromPTree(String path, String mpath) throws PathErrorException {
    String ptreeName = path.trim().split(separator)[0];
    if (!ptreeMap.containsKey(ptreeName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
    } else {
      ptreeMap.get(ptreeName).unlinkMNode(path, mpath);
    }
  }

  /**
   * Set storage level for current Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  public void setStorageLevel(String path) throws PathErrorException {
    mtree.setStorageGroup(path);
  }

  /**
   * Get all paths for given seriesPath regular expression if given seriesPath belongs to MTree, or
   * get all linked seriesPath for given seriesPath if given seriesPath belongs to PTree Notice:
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @return A HashMap whose Keys are separated by the storage file name.
   */
  public HashMap<String, ArrayList<String>> getAllPathGroupByFilename(String path)
      throws PathErrorException {
    String rootName = path.trim().split(separator)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getAllPath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      return ptree.getAllLinkedPath(path);
    }
    throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   */
  public List<List<String>> getShowTimeseriesPath(String path) throws PathErrorException {
    String rootName = path.trim().split(separator)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getShowTimeseriesPath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      throw new PathErrorException(
          "PTree is not involved in the execution of the sql 'show timeseries " + path + "'");
    }
    throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
  }

  /**
   * Get all deviceId type in current Metadata Tree.
   *
   * @return a HashMap contains all distinct deviceId type separated by deviceId Type
   */
  public Map<String, List<ColumnSchema>> getSchemaForAllType() throws PathErrorException {
    Map<String, List<ColumnSchema>> res = new HashMap<>();
    List<String> typeList = mtree.getAllType();
    for (String type : typeList) {
      res.put(type, getSchemaForOneType("root." + type));
    }
    return res;
  }

  private ArrayList<String> getDeviceForOneType(String type) throws PathErrorException {
    return mtree.getDeviceForOneType(type);
  }

  /**
   * Get all delta objects group by deviceId type.
   */
  public Map<String, List<String>> getDeviceForAllType() throws PathErrorException {
    Map<String, List<String>> res = new HashMap<>();
    ArrayList<String> types = mtree.getAllType();
    for (String type : types) {
      res.put(type, getDeviceForOneType(type));
    }
    return res;
  }

  /**
   * Get the full Metadata info.
   *
   * @return A {@code Metadata} instance which stores all metadata info
   */
  public Metadata getMetadata() throws PathErrorException {
    Map<String, List<ColumnSchema>> seriesMap = getSchemaForAllType();
    Map<String, List<String>> deviceIdMap = getDeviceForAllType();
    Metadata metadata = new Metadata(seriesMap, deviceIdMap);
    return metadata;
  }

  public HashSet<String> getAllStorageGroup() throws PathErrorException {
    return mtree.getAllStorageGroup();
  }

  public List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    return mtree.getLeafNodePathInNextLevel(path);
  }

  /**
   * Get all ColumnSchemas for given delta object type.
   *
   * @param path A seriesPath represented one Delta object
   * @return a list contains all column schema
   */
  public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {
    return mtree.getSchemaForOneType(path);
  }

  /**
   * Get all ColumnSchemas for the filenode seriesPath.
   *
   * @param path the filenode seriesPath
   * @return ArrayList<'   ColumnSchema   '> The list of the schema
   */
  public ArrayList<ColumnSchema> getSchemaForOneFileNode(String path) {
    return mtree.getSchemaForOneFileNode(path);
  }

  public Map<String, ColumnSchema> getSchemaMapForOneFileNode(String path) {
    return mtree.getSchemaMapForOneFileNode(path);
  }

  public Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {
    return mtree.getNumSchemaMapForOneFileNode(path);
  }

  /**
   * Calculate the count of storage-level nodes included in given seriesPath.
   *
   * @return The total count of storage-level nodes.
   */
  public int getFileCountForOneType(String path) throws PathErrorException {
    return mtree.getFileCountForOneType(path);
  }

  /**
   * Get the file name for given seriesPath Notice: This method could be called if and only if the
   * seriesPath includes one node whose {@code isStorageLevel} is true.
   */
  public String getFileNameByPath(String path) throws PathErrorException {
    return mtree.getFileNameByPath(path);
  }

  public String getFileNameByPath(MNode node, String path) throws PathErrorException {
    return mtree.getFileNameByPath(node, path);
  }

  public String getFileNameByPathWithCheck(MNode node, String path) throws PathErrorException {
    return mtree.getFileNameByPathWithCheck(node, path);
  }

  public boolean checkFileNameByPath(String path) {
    return mtree.checkFileNameByPath(path);
  }

  /**
   * Check whether the seriesPath given exists.
   */
  public boolean pathExist(String path) {
    return mtree.isPathExist(path);
  }

  public boolean pathExist(MNode node, String path) {
    return mtree.isPathExist(node, path);
  }

  public MNode getNodeByPath(String path) throws PathErrorException {
    return mtree.getNodeByPath(path);
  }

  public MNode getNodeByPathWithCheck(String path) throws PathErrorException {
    return mtree.getNodeByPathWithFileLevelCheck(path);
  }

  /**
   * Extract the deviceId from given seriesPath.
   *
   * @return String represents the deviceId
   */
  public String getDeviceTypeByPath(String path) throws PathErrorException {
    return mtree.getDeviceTypeByPath(path);
  }

  /**
   * Get ColumnSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {
    return mtree.getSchemaForOnePath(path);
  }

  public ColumnSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {
    return mtree.getSchemaForOnePath(node, path);
  }

  public ColumnSchema getSchemaForOnePathWithCheck(MNode node, String path)
      throws PathErrorException {
    return mtree.getSchemaForOnePathWithCheck(node, path);
  }

  public ColumnSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {
    return mtree.getSchemaForOnePathWithCheck(path);
  }

  /**
   * functions for converting the mTree to a readable string in json format.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("===  Timeseries Tree  ===\n\n");
    sb.append(mtree.toString());
    return sb.toString();
  }
}
