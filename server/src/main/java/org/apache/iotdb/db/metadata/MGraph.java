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

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Metadata Graph consists of one {@code MTree} and several {@code PTree}.
 */
public class MGraph implements Serializable {

  private static final long serialVersionUID = 8214849219614352834L;
  private static final String DOUB_SEPARATOR = "\\.";
  private static final String TIME_SERIES_INCORRECT = "Timeseries's root is not Correct. RootName: ";
  private MTree mtree;
  private HashMap<String, PTree> ptreeMap;

  MGraph(String mtreeName) {
    mtree = new MTree(mtreeName);
    ptreeMap = new HashMap<>();
  }

  /**
   * Add a {@code PTree} to current {@code MGraph}.
   */
  void addAPTree(String ptreeRootName) throws MetadataErrorException {
    if (MetadataConstant.ROOT.equalsIgnoreCase(ptreeRootName)) {
      throw new MetadataErrorException("Property Tree's root name should not be 'root'");
    }
    PTree ptree = new PTree(ptreeRootName, mtree);
    ptreeMap.put(ptreeRootName, ptree);
  }

  /**
   * this is just for compatibility
   */
  public void addPathToMTree(String path, String dataType, String encoding)
      throws PathErrorException {
    TSDataType tsDataType = TSDataType.valueOf(dataType);
    TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
    CompressionType compressionType = CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor());
    addPathToMTree(path, tsDataType, tsEncoding, compressionType,
        Collections.emptyMap());
  }

  /**
   * Add a seriesPath to Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  public void addPathToMTree(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null");
    }
    mtree.addTimeseriesPath(path, dataType, encoding, compressor, props);
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
   * Add a deviceId to Metadata Tree.
   */
  public MNode addDeviceIdToMTree(String deviceId) throws PathErrorException {
    return mtree.addDeviceId(deviceId);
  }

  /**
   * Add a seriesPath to {@code PTree}.
   */
  void addPathToPTree(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null.");
    }
    String rootName = getNodeNames(path)[0];
    if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.addPath(path);
    } else {
      throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
    }
  }

  /**
   * Delete seriesPath in current MGraph.
   *
   * @param path a seriesPath belongs to MTree or PTree
   */
  String deletePath(String path) throws PathErrorException {
    String[] nodes = getNodeNames(path);
    if (nodes.length == 0) {
      throw new PathErrorException("Timeseries is null");
    }
    String rootName = getNodeNames(path)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.deletePath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.deletePath(path);
      return null;
    } else {
      throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
    }
  }

  /**
   * Link a {@code MNode} to a {@code PNode} in current PTree.
   */
  void linkMNodeToPTree(String path, String mpath) throws PathErrorException {
    String ptreeName = getNodeNames(path)[0];
    if (!ptreeMap.containsKey(ptreeName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
    } else {
      ptreeMap.get(ptreeName).linkMNode(path, mpath);
    }
  }

  /**
   * Unlink a {@code MNode} from a {@code PNode} in current PTree.
   */
  void unlinkMNodeFromPTree(String path, String mpath) throws PathErrorException {
    String ptreeName = getNodeNames(path)[0];
    if (!ptreeMap.containsKey(ptreeName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
    } else {
      ptreeMap.get(ptreeName).unlinkMNode(path, mpath);
    }
  }

  /**
   * Set storage group for current Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  void setStorageGroup(String path) throws StorageGroupException {
    mtree.setStorageGroup(path);
  }

  /**
   * Delete storage group from current Metadata Tree.
   *
   * @param path Format: root.node
   */
  void deleteStorageGroup(String path) throws PathErrorException {
    mtree.deleteStorageGroup(path);
  }
  /**
   * Check whether the input path is storage group for current Metadata Tree or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  boolean checkStorageGroup(String path) {
    return mtree.checkStorageGroup(path);
  }

  /**
   * Get all paths for given seriesPath regular expression if given seriesPath belongs to MTree, or
   * get all linked seriesPath for given seriesPath if given seriesPath belongs to PTree Notice:
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @return A HashMap whose Keys are separated by the storage file name.
   */
  HashMap<String, ArrayList<String>> getAllPathGroupByFilename(String path)
      throws PathErrorException {
    String rootName = getNodeNames(path)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getAllPath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      return ptree.getAllLinkedPath(path);
    }
    throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   */
  List<List<String>> getShowTimeseriesPath(String path) throws PathErrorException {
    String rootName = getNodeNames(path)[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getShowTimeseriesPath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      throw new PathErrorException(
          "PTree is not involved in the execution of the sql 'show timeseries " + path + "'");
    }
    throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
  }

  /**
   * Get all deviceId type in current Metadata Tree.
   *
   * @return a HashMap contains all distinct deviceId type separated by deviceId Type
   */
  Map<String, List<MeasurementSchema>> getSchemaForAllType() throws PathErrorException {
    Map<String, List<MeasurementSchema>> res = new HashMap<>();
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
  private Map<String, List<String>> getDeviceForAllType() throws PathErrorException {
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
    Map<String, List<String>> deviceIdMap = getDeviceForAllType();
    return new Metadata(deviceIdMap);
  }

  Set<String> getAllStorageGroup() {
    return mtree.getAllStorageGroup();
  }

  Set<String> getAllDevices() throws SQLException {
    return mtree.getAllDevices();
  }

  List<String> getNodesList(String schemaPattern, int nodeLevel) throws SQLException {
    return mtree.getNodesList(schemaPattern, nodeLevel);
  }

  List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    return mtree.getLeafNodePathInNextLevel(path);
  }

  /**
   * Get all ColumnSchemas for given delta object type.
   *
   * @param path A seriesPath represented one Delta object
   * @return a list contains all column schema
   */
  ArrayList<MeasurementSchema> getSchemaForOneType(String path) throws PathErrorException {
    return mtree.getSchemaForOneType(path);
  }

  /**
   * Get all ColumnSchemas for the storage group seriesPath.
   *
   * @param path the Path in a storage group
   * @return ArrayList<'   ColumnSchema   '> The list of the schema
   */
  ArrayList<MeasurementSchema> getSchemaInOneStorageGroup(String path) {
    return mtree.getSchemaForOneStorageGroup(path);
  }

  Map<String, MeasurementSchema> getSchemaMapForOneFileNode(String path) {
    return mtree.getSchemaMapForOneStorageGroup(path);
  }

  Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {
    return mtree.getNumSchemaMapForOneFileNode(path);
  }

  /**
   * Calculate the count of storage-group nodes included in given seriesPath.
   *
   * @return The total count of storage-group nodes.
   */
  int getFileCountForOneType(String path) throws PathErrorException {
    return mtree.getFileCountForOneType(path);
  }

  /**
   * Get the file name for given seriesPath Notice: This method could be called if and only if the
   * seriesPath includes one node whose {@code isStorageGroup} is true.
   */
  String getStorageGroupNameByPath(String path) throws StorageGroupException {
    return mtree.getStorageGroupNameByPath(path);
  }

  String getStorageGroupNameByPath(MNode node, String path) throws StorageGroupException {
    return mtree.getStorageGroupNameByPath(node, path);
  }

  boolean checkFileNameByPath(String path) {
    return mtree.checkFileNameByPath(path);
  }

  /**
   * Get all file names for given seriesPath
   */
  List<String> getAllFileNamesByPath(String path) throws PathErrorException {
    return mtree.getAllFileNamesByPath(path);
  }

  /**
   * Check whether the seriesPath given exists.
   */
  boolean pathExist(String path) {
    return mtree.isPathExist(path);
  }

  boolean pathExist(MNode node, String path) {
    return mtree.isPathExist(node, path);
  }

  MNode getNodeByPath(String path) throws PathErrorException {
    return mtree.getNodeByPath(path);
  }

  MNode getNodeByPathWithCheck(String path) throws PathErrorException, StorageGroupException {
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
   * Get MeasurementSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  MeasurementSchema getSchemaForOnePath(String path) throws PathErrorException {
    return mtree.getSchemaForOnePath(path);
  }

  MeasurementSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {
    return mtree.getSchemaForOnePath(node, path);
  }

  MeasurementSchema getSchemaForOnePathWithCheck(MNode node, String path)
      throws PathErrorException {
    return mtree.getSchemaForOnePathWithCheck(node, path);
  }

  MeasurementSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {
    return mtree.getSchemaForOnePathWithCheck(path);
  }

  /**
   * functions for converting the mTree to a readable string in json format.
   */
  @Override
  public String toString() {
    return mtree.toString();
  }

  /**
   * combine multiple metadata in string format
   */
  static String combineMetadataInStrings(String[] metadatas) {
    return MTree.combineMetadataInStrings(metadatas);
  }

  /**
   * @return storage group name -> the series number
   */
  Map<String, Integer> countSeriesNumberInEachStorageGroup() throws PathErrorException {
    Map<String, Integer> res = new HashMap<>();
    Set<String> storageGroups = this.getAllStorageGroup();
    for (String sg : storageGroups) {
      MNode node = mtree.getNodeByPath(sg);
      res.put(sg, node.getLeafCount());
    }
    return res;
  }
}
