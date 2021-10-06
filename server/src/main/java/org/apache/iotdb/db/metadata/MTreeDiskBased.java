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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.IllegalParameterOfPathException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.metadisk.IMetadataAccess;
import org.apache.iotdb.db.metadata.metadisk.MetadataDiskManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_DEV;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_MAX_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_MIN_TIME;

public class MTreeDiskBased implements IMTree {

  public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger logger = LoggerFactory.getLogger(MTreeDiskBased.class);
  private static final String NO_CHILDNODE_MSG = " does not have the child node ";

  private final String rootName = PATH_ROOT;
  private IMetadataAccess metadataDiskManager;
  private static final int DEFAULT_MAX_CAPACITY =
      IoTDBDescriptor.getInstance().getConfig().getMetaDiskMNodeCacheSize();

  private static transient ThreadLocal<Integer> limit = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> offset = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> count = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> curOffset = new ThreadLocal<>();

  public MTreeDiskBased() throws IOException {
    this(DEFAULT_MAX_CAPACITY);
  }

  public MTreeDiskBased(int cacheSize) throws IOException {
    metadataDiskManager = new MetadataDiskManager(cacheSize);
  }

  public MTreeDiskBased(int cahceSize, String metaFilePath) throws IOException {
    metadataDiskManager = new MetadataDiskManager(cahceSize, metaFilePath);
  }

  static long getLastTimeStamp(MeasurementMNode node, QueryContext queryContext) {
    TimeValuePair last = node.getCachedLast();
    if (last != null) {
      return node.getCachedLast().getTimestamp();
    } else {
      try {
        QueryDataSource dataSource =
            QueryResourceManager.getInstance()
                .getQueryDataSource(node.getPartialPath(), queryContext, null);
        Set<String> measurementSet = new HashSet<>();
        measurementSet.add(node.getPartialPath().getFullPath());
        LastPointReader lastReader =
            new LastPointReader(
                node.getPartialPath(),
                node.getSchema().getType(),
                measurementSet,
                queryContext,
                dataSource,
                Long.MAX_VALUE,
                null);
        last = lastReader.readLastPoint();
        return (last != null ? last.getTimestamp() : Long.MIN_VALUE);
      } catch (Exception e) {
        logger.error(
            "Something wrong happened while trying to get last time value pair of {}",
            node.getFullPath(),
            e);
        return Long.MIN_VALUE;
      }
    }
  }

  @Override
  public MeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2 || !nodeNames[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    checkTimeseries(path);
    IMNode cur = metadataDiskManager.getRoot();
    boolean hasSetStorageGroup = false;
    Template upperTemplate = cur.getDeviceTemplate();
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length - 1; i++) {
      String nodeName = nodeNames[i];
      if (cur.isStorageGroup()) {
        hasSetStorageGroup = true;
      }
      if (!cur.hasChild(nodeName)) {
        if (!hasSetStorageGroup) {
          unlockMNodePath(cur);
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        metadataDiskManager.addChild(cur, nodeName, new InternalMNode(cur, nodeName), true);
        cur = metadataDiskManager.getChild(cur, nodeName);
      } else {
        cur = metadataDiskManager.getChild(cur, nodeName, true);
      }
      if (cur.getDeviceTemplate() != null) {
        upperTemplate = cur.getDeviceTemplate();
      }
    }

    if (upperTemplate != null && !upperTemplate.isCompatible(path)) {
      unlockMNodePath(cur);
      throw new PathAlreadyExistException(
          path.getFullPath() + " ( which is incompatible with template )");
    }

    if (props != null && props.containsKey(LOSS) && props.get(LOSS).equals(SDT)) {
      try {
        checkSDTFormat(path.getFullPath(), props);
      } catch (IllegalParameterOfPathException e) {
        unlockMNodePath(cur);
        throw e;
      }
    }

    String leafName = nodeNames[nodeNames.length - 1];

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      IMNode child = metadataDiskManager.getChild(cur, leafName, true);
      if (child != null && (child.isMeasurement() || child.isStorageGroup())) {
        unlockMNodePath(child);
        throw new PathAlreadyExistException(path.getFullPath());
      }

      if (alias != null) {
        IMNode childByAlias = metadataDiskManager.getChild(cur, alias);
        if (childByAlias != null && childByAlias.isMeasurement()) {
          unlockMNodePath(child);
          throw new AliasAlreadyExistException(path.getFullPath(), alias);
        }
      }

      // this measurementMNode could be a leaf or not.
      MeasurementMNode measurementMNode =
          new MeasurementMNode(cur, leafName, alias, dataType, encoding, compressor, props);
      if (child != null) {
        unlockMNode(child);
        metadataDiskManager.replaceChild(cur, measurementMNode.getName(), measurementMNode, true);
      } else {
        metadataDiskManager.addChild(cur, leafName, measurementMNode, true);
      }

      // link alias to LeafMNode
      if (alias != null) {
        metadataDiskManager.addAlias(cur, alias, measurementMNode);
      }

      unlockMNodePath(measurementMNode.getParent());
      return measurementMNode;
    }
  }

  private void checkTimeseries(PartialPath timeseries) throws MetadataException {
    if (!IoTDBConfig.NODE_PATTERN.matcher(timeseries.getFullPath()).matches()) {
      throw new IllegalPathException(
          String.format("The timeseries name contains unsupported character. %s", timeseries));
    }

    // filter special id, including "time" and "timeseries"
    for (String nodeName : timeseries.getNodes()) {
      nodeName = nodeName.trim().toLowerCase(Locale.ENGLISH);
      if ("time".equals(nodeName) || "timestamp".equals(nodeName)) {
        throw new IllegalPathException(timeseries.getFullPath());
      }
    }

    String measurementId = timeseries.getMeasurement();
    // check measurementId syntax
    // only measurementId may be named separately from fullPath by user via API
    if (measurementId.contains(".")
        && !(measurementId.startsWith("\"") && measurementId.endsWith("\""))) {
      throw new MetadataException(String.format("%s is an illegal measurementId", measurementId));
    }
  }

  // check if sdt parameters are valid
  private void checkSDTFormat(String path, Map<String, String> props)
      throws IllegalParameterOfPathException {
    if (!props.containsKey(SDT_COMP_DEV)) {
      throw new IllegalParameterOfPathException("SDT compression deviation is required", path);
    }

    try {
      double d = Double.parseDouble(props.get(SDT_COMP_DEV));
      if (d < 0) {
        throw new IllegalParameterOfPathException(
            "SDT compression deviation cannot be negative", path);
      }
    } catch (NumberFormatException e) {
      throw new IllegalParameterOfPathException("SDT compression deviation formatting error", path);
    }

    long compMinTime = sdtCompressionTimeFormat(SDT_COMP_MIN_TIME, props, path);
    long compMaxTime = sdtCompressionTimeFormat(SDT_COMP_MAX_TIME, props, path);

    if (compMaxTime <= compMinTime) {
      throw new IllegalParameterOfPathException(
          "SDT compression maximum time needs to be greater than compression minimum time", path);
    }
  }

  private long sdtCompressionTimeFormat(String compTime, Map<String, String> props, String path)
      throws IllegalParameterOfPathException {
    boolean isCompMaxTime = compTime.equals(SDT_COMP_MAX_TIME);
    long time = isCompMaxTime ? Long.MAX_VALUE : 0;
    String s = isCompMaxTime ? "maximum" : "minimum";
    if (props.containsKey(compTime)) {
      try {
        time = Long.parseLong(props.get(compTime));
        if (time < 0) {
          throw new IllegalParameterOfPathException(
              String.format("SDT compression %s time cannot be negative", s), path);
        }
      } catch (IllegalParameterOfPathException e) {
        throw new IllegalParameterOfPathException(
            String.format("SDT compression %s time formatting error", s), path);
      }
    } else {
      logger.info("{} enabled SDT but did not set compression {} time", path, s);
    }
    return time;
  }

  @Override
  public Pair<IMNode, Template> getDeviceNodeWithAutoCreating(PartialPath deviceId, int sgLevel)
      throws MetadataException {
    String[] nodeNames = deviceId.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(rootName)) {
      throw new IllegalPathException(deviceId.getFullPath());
    }
    IMNode cur = metadataDiskManager.getRoot();
    Template upperTemplate = null;
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        if (i == sgLevel) {
          metadataDiskManager.addChild(
              cur,
              nodeNames[i],
              new StorageGroupMNode(
                  cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL()),
              true);
        } else {
          metadataDiskManager.addChild(
              cur, nodeNames[i], new InternalMNode(cur, nodeNames[i]), true);
        }
        cur = metadataDiskManager.getChild(cur, nodeNames[i]);
      } else {
        cur = metadataDiskManager.getChild(cur, nodeNames[i], true);
      }
      // update upper template
      upperTemplate = cur.getDeviceTemplate() == null ? upperTemplate : cur.getDeviceTemplate();
      cur = cur.getChild(nodeNames[i]);
    }
    unlockMNodePath(cur.getParent());
    return new Pair<>(cur, upperTemplate);
  }

  @Override
  public boolean isPathExist(PartialPath path) {
    String[] nodeNames = path.getNodes();
    if (!nodeNames[0].equals(rootName)) {
      return false;
    }
    IMNode cur;
    try {
      cur = metadataDiskManager.getRoot();
    } catch (MetadataException e) {
      return false;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      String childName = nodeNames[i];
      try {
        cur = metadataDiskManager.getChild(cur, childName);
      } catch (MetadataException e) {
        e.printStackTrace();
        return false;
      }
      if (cur == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    checkStorageGroup(path.getFullPath());
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = metadataDiskManager.getRoot();
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      //      System.out.println(nodeNames[i]+" "+cur.isStorageGroup());
      if (!cur.hasChild(nodeNames[i])) {
        metadataDiskManager.addChild(cur, nodeNames[i], new InternalMNode(cur, nodeNames[i]), true);
        cur = metadataDiskManager.getChild(cur, nodeNames[i]);
      } else {
        cur = metadataDiskManager.getChild(cur, nodeNames[i], true);
      }

      if (cur.isStorageGroup()) {
        unlockMNodePath(cur);
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(cur.getFullPath());
      }

      i++;
    }

    synchronized (this) {
      if (cur.hasChild(nodeNames[i])) {
        // node b has child sg
        cur = metadataDiskManager.getChild(cur, nodeNames[i], true);
        unlockMNodePath(cur);
        if (cur.isStorageGroup()) {
          throw new StorageGroupAlreadySetException(path.getFullPath());
        } else {
          throw new StorageGroupAlreadySetException(path.getFullPath(), true);
        }
      } else {
        StorageGroupMNode storageGroupMNode =
            new StorageGroupMNode(
                cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
        metadataDiskManager.addChild(cur, nodeNames[i], storageGroupMNode, true);
        unlockMNodePath(storageGroupMNode);
      }
    }
  }

  private void checkStorageGroup(String storageGroup) throws IllegalPathException {
    if (!IoTDBConfig.STORAGE_GROUP_PATTERN.matcher(storageGroup).matches()) {
      throw new IllegalPathException(
          String.format(
              "The storage group name can only be characters, numbers and underscores. %s",
              storageGroup));
    }
  }

  @Override
  public List<MeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    // get the whole path with memory lock
    IMNode cur = getNodeByPathWithPathMemoryLock(path);
    if (!(cur.isStorageGroup())) {
      unlockMNodePath(cur);
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    IMNode parent = cur.getParent();
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    unlockMNode(cur);
    cur = metadataDiskManager.deleteChild(parent, cur.getName());

    // collect all the LeafMNode in this storage group
    List<MeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<IMNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      IMNode node = queue.poll();
      for (IMNode child :
          node.getChildren().values()) { // all the deleted Nodes are in memory until GC
        if (child.isMeasurement()) {
          leafMNodes.add((MeasurementMNode) child);
        } else {
          queue.add(child);
        }
      }
    }

    cur = parent;
    // delete node b while retain root.a.sg2
    while (!rootName.equals(cur.getName()) && cur.getChildren().size() == 0) {
      parent = cur.getParent();
      unlockMNode(cur);
      metadataDiskManager.deleteChild(parent, cur.getName());
      cur = parent;
    }
    unlockMNodePath(cur);
    return leafMNodes;
  }

  @Override
  public boolean isStorageGroup(PartialPath path) {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(PATH_ROOT)) {
      return false;
    }
    IMNode cur;
    try {
      cur = metadataDiskManager.getRoot();
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return false;
    }
    int i = 1;
    while (i < nodeNames.length - 1) {
      try {
        cur = metadataDiskManager.getChild(cur, nodeNames[i]);
      } catch (MetadataException e) {
        logger.error(e.getMessage());
        return false;
      }
      if (cur == null || cur.isStorageGroup()) {
        return false;
      }
      i++;
    }
    try {
      cur = metadataDiskManager.getChild(cur, nodeNames[i]);
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return false;
    }
    return cur != null && cur.isStorageGroup();
  }

  @Override
  public Pair<PartialPath, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(
      PartialPath path) throws MetadataException {
    IMNode curNode = getNodeByPathWithPathMemoryLock(path);
    if (!(curNode.isMeasurement())) {
      unlockMNodePath(curNode);
      throw new PathNotExistException(path.getFullPath());
    }
    IMNode parent = curNode.getParent();

    // delete the last node of path
    unlockMNode(curNode);
    MeasurementMNode deletedNode =
        (MeasurementMNode) metadataDiskManager.deleteChild(parent, curNode.getName());
    if (deletedNode.getAlias() != null) {
      metadataDiskManager.deleteAliasChild(parent, ((MeasurementMNode) curNode).getAlias());
    }
    curNode = parent;
    // delete all empty ancestors except storage group and MeasurementMNode
    while (!PATH_ROOT.equals(curNode.getName())
        && !(curNode.isMeasurement())
        && curNode.getChildren().size() == 0) {
      // if current storage group has no time series, return the storage group name
      if (curNode.isStorageGroup()) {
        unlockMNodePath(curNode);
        return new Pair<>(curNode.getPartialPath(), deletedNode);
      }
      parent = curNode.getParent();
      unlockMNode(curNode);
      metadataDiskManager.deleteChild(parent, curNode.getName());
      curNode = parent;
    }
    unlockMNodePath(curNode);
    return new Pair<>(null, deletedNode);
  }

  @Override
  public MeasurementSchema getSchema(PartialPath path) throws MetadataException {
    MeasurementMNode node = (MeasurementMNode) getNodeByPath(path);
    return node.getSchema();
  }

  @Override
  public Pair<IMNode, Template> getNodeByPathWithStorageGroupCheck(PartialPath path)
      throws MetadataException {
    boolean storageGroupChecked = false;
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMNode cur = metadataDiskManager.getRoot();
    Template upperTemplate = null;
    for (int i = 1; i < nodes.length; i++) {
      upperTemplate = cur.getDeviceTemplate() == null ? upperTemplate : cur.getDeviceTemplate();
      cur = metadataDiskManager.getChild(cur, nodes[i]);
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
    return new Pair<>(cur, upperTemplate);
  }

  @Override
  public Pair<IMNode, Template> getNodeByPathWithStorageGroupCheckAndMemoryLock(PartialPath path)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    boolean storageGroupChecked = false;
    IMNode cur = metadataDiskManager.getRoot();
    Template upperTemplate = null;
    for (int i = 1; i < nodes.length; i++) {
      upperTemplate = cur.getDeviceTemplate() == null ? upperTemplate : cur.getDeviceTemplate();
      if (!cur.hasChild(nodes[i])) {
        unlockMNodePath(cur);
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        throw new PathNotExistException(path.getFullPath());
      }
      cur = metadataDiskManager.getChild(cur, nodes[i], true);
      if (cur.isStorageGroup()) {
        storageGroupChecked = true;
      }
    }

    if (!storageGroupChecked) {
      unlockMNodePath(cur);
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    unlockMNodePath(cur.getParent());
    return new Pair<>(cur, null);
  }

  @Override
  public StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    IMNode node = getNodeByPath(path);
    if (node.isStorageGroup()) {
      return (StorageGroupMNode) node;
    } else {
      throw new StorageGroupNotSetException(path.getFullPath(), true);
    }
  }

  @Override
  public StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = metadataDiskManager.getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        break;
      }
      cur = metadataDiskManager.getChild(cur, nodes[i]);
      if (cur.isStorageGroup()) {
        return (StorageGroupMNode) cur;
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  @Override
  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException {
    IMNode node = getNodeByPathWithMemoryLock(storageGroup);
    if (!node.isStorageGroup()) {
      unlockMNode(node);
      throw new StorageGroupNotSetException(storageGroup.getFullPath(), true);
    }
    StorageGroupMNode storageGroupMNode = (StorageGroupMNode) node;
    storageGroupMNode.setDataTTL(dataTTL);
    metadataDiskManager.updateMNode(storageGroupMNode);
    unlockMNode(storageGroupMNode);
  }

  @Override
  public IMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = metadataDiskManager.getRoot();
    Template upperTemplate = cur.getDeviceTemplate();
    for (int i = 1; i < nodes.length; i++) {
      if (cur.getDeviceTemplate() != null) {
        upperTemplate = cur.getDeviceTemplate();
      }
      IMNode next = metadataDiskManager.getChild(cur, nodes[i]);
      if (next == null) {
        if (upperTemplate == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        }

        String realName = nodes[i];
        MeasurementSchema schema = upperTemplate.getSchemaMap().get(realName);
        if (schema == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        }
        return new MeasurementMNode(cur, schema.getMeasurementId(), schema, null);
      }
      cur = next;
    }
    return cur;
  }

  @Override
  public IMNode getNodeByPathWithMemoryLock(PartialPath path) throws MetadataException {
    IMNode cur = getNodeByPathWithPathMemoryLock(path);
    unlockMNodePath(cur.getParent());
    return cur;
  }

  private IMNode getNodeByPathWithPathMemoryLock(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    IMNode cur = metadataDiskManager.getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        unlockMNodePath(cur);
        throw new PathNotExistException(path.getFullPath(), true);
      }
      cur = metadataDiskManager.getChild(cur, nodes[i], true);
    }
    return cur;
  }

  @Override
  public Map<String, IMNode> getChildrenOfNodeByPath(PartialPath path) throws MetadataException {
    Map<String, IMNode> result = new HashMap<>();
    IMNode node = getNodeByPath(path);
    for (String key : node.getChildren().keySet()) {
      result.put(key, metadataDiskManager.getChild(node, key));
    }
    for (String key : node.getAliasChildren().keySet()) {
      result.put(key, metadataDiskManager.getChild(node, key));
    }
    return result;
  }

  @Override
  public IMNode getChildMNodeInDeviceWithMemoryLock(IMNode deviceNode, String childName)
      throws MetadataException {
    if (deviceNode.isLockedInMemory()) {
      return metadataDiskManager.getChild(deviceNode, childName, true);
    } else {
      return getNodeByPathWithMemoryLock(deviceNode.getPartialPath().concatNode(childName));
    }
  }

  @Override
  public IMNode getNodeDeepClone(IMNode mNode) throws MetadataException {
    return processMNodeReturn(mNode);
  }

  private IMNode processMNodeReturn(IMNode mNode) throws MetadataException {
    IMNode result = mNode.clone();
    Map<String, IMNode> children = result.getChildren();
    for (Map.Entry<String, IMNode> entry : children.entrySet()) {
      if (!entry.getValue().isLoaded()) {
        entry.setValue(metadataDiskManager.getChild(mNode, entry.getKey()));
      }
    }
    Map<String, IMNode> aliasChildren = result.getAliasChildren();
    for (Map.Entry<String, IMNode> entry : aliasChildren.entrySet()) {
      if (!entry.getValue().isLoaded()) {
        entry.setValue(metadataDiskManager.getChild(mNode, entry.getKey()));
      }
    }
    return result;
  }

  @Override
  public List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    List<String> storageGroups = new ArrayList<>();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    findStorageGroup(metadataDiskManager.getRoot(), nodes, 1, "", storageGroups);
    return storageGroups;
  }

  /**
   * Recursively find all storage group according to a specific path
   *
   * @apiNote :for cluster
   */
  private void findStorageGroup(
      IMNode node, String[] nodes, int idx, String parent, List<String> storageGroupNames)
      throws MetadataException {
    if (node.isStorageGroup()) {
      storageGroupNames.add(node.getFullPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        findStorageGroup(
            next, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    } else {
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        findStorageGroup(
            child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    }
  }

  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    List<PartialPath> res = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    try {
      nodeStack.add(metadataDiskManager.getRoot());
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return res;
    }

    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        res.add(current.getPartialPath());
      } else {
        try {
          nodeStack.addAll(metadataDiskManager.getChildren(current).values());
        } catch (MetadataException e) {
          e.printStackTrace();
          break;
        }
      }
    }
    return res;
  }

  @Override
  public List<PartialPath> searchAllRelatedStorageGroups(PartialPath path)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    List<PartialPath> storageGroupPaths = new ArrayList<>();
    findStorageGroupPaths(metadataDiskManager.getRoot(), nodes, 1, "", storageGroupPaths, false);
    return storageGroupPaths;
  }

  @Override
  public List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    List<PartialPath> storageGroupPaths = new ArrayList<>();
    findStorageGroupPaths(metadataDiskManager.getRoot(), nodes, 1, "", storageGroupPaths, true);
    return storageGroupPaths;
  }

  /**
   * Traverse the MTree to match all storage group with prefix path. When trying to find storage
   * groups via a path, we divide into two cases: 1. This path is only regarded as a prefix, in
   * other words, this path is part of the result storage groups. 2. This path is a full path and we
   * use this method to find its belonged storage group. When prefixOnly is set to true, storage
   * group paths in 1 is only added into result, otherwise, both 1 and 2 are returned.
   *
   * @param node the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx the current index of array nodes
   * @param parent current parent path
   * @param storageGroupPaths store all matched storage group names
   * @param prefixOnly only return storage groups that start with this prefix path
   */
  private void findStorageGroupPaths(
      IMNode node,
      String[] nodes,
      int idx,
      String parent,
      List<PartialPath> storageGroupPaths,
      boolean prefixOnly)
      throws MetadataException {
    if (node.isStorageGroup() && (!prefixOnly || idx >= nodes.length)) {
      storageGroupPaths.add(node.getPartialPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        findStorageGroupPaths(
            next,
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            storageGroupPaths,
            prefixOnly);
      }
    } else {
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        findStorageGroupPaths(
            child,
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            storageGroupPaths,
            prefixOnly);
      }
    }
  }

  @Override
  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    List<StorageGroupMNode> ret = new ArrayList<>();
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    try {
      nodeStack.add(metadataDiskManager.getRoot());
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return ret;
    }
    while (!nodeStack.isEmpty()) {
      IMNode current = nodeStack.pop();
      if (current.isStorageGroup()) {
        ret.add((StorageGroupMNode) current);
      } else {
        try {
          nodeStack.addAll(metadataDiskManager.getChildren(current).values());
        } catch (MetadataException e) {
          logger.error(e.getMessage());
          break;
        }
      }
    }
    return ret;
  }

  @Override
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    String[] nodes = path.getNodes();
    IMNode cur;
    try {
      cur = metadataDiskManager.getRoot();
    } catch (MetadataException e) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    for (int i = 1; i < nodes.length; i++) {
      try {
        cur = metadataDiskManager.getChild(cur, nodes[i]);
      } catch (MetadataException e) {
        throw new StorageGroupNotSetException(path.getFullPath());
      }
      if (cur == null) {
        throw new StorageGroupNotSetException(path.getFullPath());
      } else if (cur.isStorageGroup()) {
        return cur.getPartialPath();
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    IMNode cur;
    try {
      cur = metadataDiskManager.getRoot();
    } catch (MetadataException e) {
      return false;
    }
    for (int i = 1; i < nodes.length; i++) {
      try {
        cur = metadataDiskManager.getChild(cur, nodes[i]);
      } catch (MetadataException e) {
        return false;
      }
      if (cur == null) {
        return false;
      } else if (cur.isStorageGroup()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prefixPath);
    List<Pair<PartialPath, String[]>> res = getAllMeasurementSchema(plan);
    List<PartialPath> paths = new ArrayList<>();
    for (Pair<PartialPath, String[]> p : res) {
      paths.add(p.left);
    }
    return paths;
  }

  @Override
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath prefixPath, int limit, int offset) throws MetadataException {
    PartialPath prePath = new PartialPath(prefixPath.getNodes());
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prefixPath);
    plan.setLimit(limit);
    plan.setOffset(offset);
    List<Pair<PartialPath, String[]>> res = getAllMeasurementSchema(plan, false);
    List<PartialPath> paths = new ArrayList<>();
    for (Pair<PartialPath, String[]> p : res) {
      if (prePath.getMeasurement().equals(p.right[0])) {
        p.left.setMeasurementAlias(p.right[0]);
      }
      paths.add(p.left);
    }
    if (curOffset.get() == null) {
      offset = 0;
    } else {
      offset = curOffset.get() + 1;
    }
    curOffset.remove();
    return new Pair<>(paths, offset);
  }

  @Override
  public int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    try {
      return getCount(metadataDiskManager.getRoot(), nodes, 1, false);
    } catch (PathNotExistException e) {
      throw new PathNotExistException(prefixPath.getFullPath());
    }
  }

  /** Traverse the MTree to get the count of timeseries. */
  private int getCount(IMNode node, String[] nodes, int idx, boolean wildcard)
      throws MetadataException {
    if (idx < nodes.length) {
      if (PATH_WILDCARD.equals(nodes[idx])) {
        int sum = 0;
        for (IMNode child : metadataDiskManager.getChildren(node).values()) {
          sum += getCount(child, nodes, idx + 1, true);
        }
        return sum;
      } else {
        IMNode child = metadataDiskManager.getChild(node, nodes[idx]);
        if (child == null) {
          if (node.isUseTemplate()
              && node.getUpperTemplate().getSchemaMap().containsKey(nodes[idx])) {
            return 1;
          }
          if (!wildcard) {
            throw new PathNotExistException(node.getName() + NO_CHILDNODE_MSG + nodes[idx]);
          } else {
            return 0;
          }
        }
        return getCount(child, nodes, idx + 1, wildcard);
      }
    } else {
      int sum = node.isMeasurement() ? 1 : 0;
      if (node.isUseTemplate()) {
        sum += node.getUpperTemplate().getSchemaMap().size();
      }
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        sum += getCount(child, nodes, idx + 1, wildcard);
      }
      return sum;
    }
  }

  @Override
  public int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    return getDevicesCount(metadataDiskManager.getRoot(), nodes, 1);
  }

  /** Traverse the MTree to get the count of devices. */
  private int getDevicesCount(IMNode node, String[] nodes, int idx) throws MetadataException {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    boolean curIsDevice = node.isUseTemplate();
    int cnt = curIsDevice ? 1 : 0;
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        if (next.isMeasurement() && idx >= nodes.length && !curIsDevice) {
          cnt++;
        } else {
          cnt += getDevicesCount(next, nodes, idx + 1);
        }
      }
    } else {
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        if (child.isMeasurement() && !curIsDevice && idx >= nodes.length) {
          cnt++;
          curIsDevice = true;
        }
        cnt += getDevicesCount(child, nodes, idx + 1);
      }
    }
    return cnt;
  }

  @Override
  public int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    return getStorageGroupCount(metadataDiskManager.getRoot(), nodes, 1, "");
  }

  /** Traverse the MTree to get the count of storage group. */
  private int getStorageGroupCount(IMNode node, String[] nodes, int idx, String parent)
      throws MetadataException {
    int cnt = 0;
    if (node.isStorageGroup() && idx >= nodes.length) {
      cnt++;
      return cnt;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        cnt += getStorageGroupCount(next, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR);
      }
    } else {
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        cnt +=
            getStorageGroupCount(child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR);
      }
    }
    return cnt;
  }

  @Override
  public int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    IMNode node = metadataDiskManager.getRoot();
    int i;
    for (i = 1; i < nodes.length; i++) {
      if (nodes[i].equals("*")) {
        break;
      }
      if (node.hasChild(nodes[i])) {
        node = metadataDiskManager.getChild(node, nodes[i]);
      } else {
        throw new MetadataException(nodes[i - 1] + NO_CHILDNODE_MSG + nodes[i]);
      }
    }
    return getCountInGivenLevel(node, level - (i - 1));
  }

  /**
   * Traverse the MTree to get the count of timeseries in the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private int getCountInGivenLevel(IMNode node, int targetLevel) throws MetadataException {
    if (targetLevel == 0) {
      return 1;
    }
    int cnt = 0;
    for (IMNode child : metadataDiskManager.getChildren(node).values()) {
      cnt += getCountInGivenLevel(child, targetLevel - 1);
    }
    return cnt;
  }

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException {
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    List<Pair<PartialPath, String[]>> allMatchedNodes = new ArrayList<>();

    findPath(
        metadataDiskManager.getRoot(), nodes, 1, allMatchedNodes, false, true, queryContext, null);

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

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    return getAllMeasurementSchema(plan, true);
  }

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(
      ShowTimeSeriesPlan plan, boolean removeCurrentOffset) throws MetadataException {
    List<Pair<PartialPath, String[]>> res = new LinkedList<>();
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    findPath(
        metadataDiskManager.getRoot(),
        nodes,
        1,
        res,
        offset.get() != 0 || limit.get() != 0,
        false,
        null,
        null);
    // avoid memory leaks
    limit.remove();
    offset.remove();
    if (removeCurrentOffset) {
      curOffset.remove();
    }
    count.remove();
    return res;
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   *
   * @param needLast if false, lastTimeStamp in timeseriesSchemaList will be null
   * @param timeseriesSchemaList List<timeseriesSchema> result: [name, alias, storage group,
   *     dataType, encoding, compression, offset, lastTimeStamp]
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void findPath(
      IMNode node,
      String[] nodes,
      int idx,
      List<Pair<PartialPath, String[]>> timeseriesSchemaList,
      boolean hasLimit,
      boolean needLast,
      QueryContext queryContext,
      Template upperTemplate)
      throws MetadataException {
    if (node.isMeasurement() && nodes.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }

      addMeasurementSchema(
          node,
          timeseriesSchemaList,
          needLast,
          queryContext,
          ((MeasurementMNode) node).getSchema(),
          "*");

      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (node.getDeviceTemplate() != null) {
      upperTemplate = node.getDeviceTemplate();
    }

    if (!nodeReg.contains(PATH_WILDCARD)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        findPath(
            next,
            nodes,
            idx + 1,
            timeseriesSchemaList,
            hasLimit,
            needLast,
            queryContext,
            upperTemplate);
      }
    } else {
      for (IMNode child : metadataDiskManager.getChildren(node).values()) {
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findPath(
            child,
            nodes,
            idx + 1,
            timeseriesSchemaList,
            hasLimit,
            needLast,
            queryContext,
            upperTemplate);
        if (hasLimit && count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
    }

    // template part
    if (!(node instanceof MeasurementMNode) && node.isUseTemplate()) {
      if (upperTemplate != null) {
        HashSet<MeasurementSchema> set = new HashSet<>();
        for (MeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
          if (set.add(schema)) {
            addMeasurementSchema(
                new MeasurementMNode(node, schema.getMeasurementId(), schema, null),
                timeseriesSchemaList,
                needLast,
                queryContext,
                schema,
                nodeReg);
          }
        }
      }
    }
  }

  private void addMeasurementSchema(
      IMNode node,
      List<Pair<PartialPath, String[]>> timeseriesSchemaList,
      boolean needLast,
      QueryContext queryContext,
      MeasurementSchema measurementSchema,
      String reg)
      throws StorageGroupNotSetException {
    if (Pattern.matches(reg.replace("*", ".*"), measurementSchema.getMeasurementId())) {
      PartialPath nodePath = node.getPartialPath();
      String[] tsRow = new String[7];
      tsRow[0] = ((MeasurementMNode) node).getAlias();
      tsRow[1] = getStorageGroupPath(nodePath).getFullPath();
      tsRow[2] = measurementSchema.getType().toString();
      tsRow[3] = measurementSchema.getEncodingType().toString();
      tsRow[4] = measurementSchema.getCompressor().toString();
      tsRow[5] = String.valueOf(((MeasurementMNode) node).getOffset());
      tsRow[6] =
          needLast ? String.valueOf(getLastTimeStamp((MeasurementMNode) node, queryContext)) : null;
      Pair<PartialPath, String[]> temp = new Pair<>(nodePath, tsRow);
      timeseriesSchemaList.add(temp);
    }
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    Set<String> childNodePaths = new TreeSet<>();
    findChildNodePathInNextLevel(
        metadataDiskManager.getRoot(), nodes, 1, "", childNodePaths, nodes.length + 1);
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
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void findChildNodePathInNextLevel(
      IMNode node, String[] nodes, int idx, String parent, Set<String> res, int length)
      throws MetadataException {
    if (node == null) {
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(parent + node.getName());
      } else {
        findChildNodePathInNextLevel(
            metadataDiskManager.getChild(node, nodeReg),
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            res,
            length);
      }
    } else {
      if (node.getChildren().size() > 0) {
        for (IMNode child : metadataDiskManager.getChildren(node).values()) {
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
        String nodeName = node.getName();
        res.add(parent + nodeName);
      }
    }
  }

  @Override
  public Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    Set<String> childNodes = new TreeSet<>();
    findChildNodeInNextLevel(
        metadataDiskManager.getRoot(), nodes, 1, "", childNodes, nodes.length + 1);
    return childNodes;
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
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void findChildNodeInNextLevel(
      IMNode node, String[] nodes, int idx, String parent, Set<String> res, int length)
      throws MetadataException {
    if (node == null) {
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(node.getName());
      } else {
        findChildNodeInNextLevel(
            metadataDiskManager.getChild(node, nodeReg),
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            res,
            length);
      }
    } else {
      if (node.getChildren().size() > 0) {
        for (IMNode child : metadataDiskManager.getChildren(node).values()) {
          if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
            continue;
          }
          if (idx == length) {
            res.add(node.getName());
          } else {
            findChildNodeInNextLevel(
                child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, res, length);
          }
        }
      } else if (idx == length) {
        String nodeName = node.getName();
        res.add(nodeName);
      }
    }
  }

  @Override
  public Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    Set<PartialPath> devices = new TreeSet<>();
    findDevices(metadataDiskManager.getRoot(), nodes, 1, devices, false, null);
    return devices;
  }

  @Override
  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    Set<PartialPath> devices = new TreeSet<>();
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    findDevices(
        metadataDiskManager.getRoot(),
        nodes,
        1,
        devices,
        offset.get() != 0 || limit.get() != 0,
        null);
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
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

  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx the current index of array nodes
   * @param res store all matched device names
   */
  @SuppressWarnings("squid:S3776")
  private void findDevices(
      IMNode node,
      String[] nodes,
      int idx,
      Set<PartialPath> res,
      boolean hasLimit,
      Template upperTemplate)
      throws MetadataException {
    upperTemplate = node.getDeviceTemplate() == null ? upperTemplate : node.getDeviceTemplate();
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    // the node path doesn't contains '*'
    if (!nodeReg.contains(PATH_WILDCARD)) {
      IMNode next = metadataDiskManager.getChild(node, nodeReg);
      if (next != null) {
        if (next.isMeasurement() && idx >= nodes.length) {
          if (hasLimit) {
            curOffset.set(curOffset.get() + 1);
            if (curOffset.get() < offset.get()
                || count.get().intValue() == limit.get().intValue()) {
              return;
            }
            count.set(count.get() + 1);
          }
          res.add(node.getPartialPath());
        } else {
          findDevices(next, nodes, idx + 1, res, hasLimit, upperTemplate);
        }
      }
    } else { // the node path contains '*'
      boolean deviceAdded = false;
      List<IMNode> children = new ArrayList<>(metadataDiskManager.getChildren(node).values());
      // template part
      if (upperTemplate != null && node.isUseTemplate()) {
        children.addAll(upperTemplate.getMeasurementMNode());
      }
      for (IMNode child : children) {
        // use '.*' to replace '*' to form a regex to match
        // if the match failed, skip it.
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        if (child.isMeasurement() && !deviceAdded && idx >= nodes.length) {
          if (hasLimit) {
            curOffset.set(curOffset.get() + 1);
            if (curOffset.get() < offset.get()
                || count.get().intValue() == limit.get().intValue()) {
              return;
            }
            count.set(count.get() + 1);
          }
          res.add(node.getPartialPath());
          deviceAdded = true;
        }
        findDevices(child, nodes, idx + 1, res, hasLimit, upperTemplate);
      }
    }
  }

  @Override
  public List<PartialPath> getNodesList(PartialPath path, int nodeLevel) throws MetadataException {
    return getNodesList(path, nodeLevel, null);
  }

  @Override
  public List<PartialPath> getNodesList(PartialPath path, int nodeLevel, StorageGroupFilter filter)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (!nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }
    List<PartialPath> res = new ArrayList<>();
    IMNode node = metadataDiskManager.getRoot();
    for (int i = 1; i < nodes.length; i++) {
      node = metadataDiskManager.getChild(node, nodes[i]);
      if (node != null) {
        if (node.isStorageGroup() && filter != null && !filter.satisfy(node.getFullPath())) {
          return res;
        }
      } else {
        throw new MetadataException(nodes[i - 1] + NO_CHILDNODE_MSG + nodes[i]);
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
  private void findNodes(
      IMNode node,
      PartialPath path,
      List<PartialPath> res,
      int targetLevel,
      StorageGroupFilter filter)
      throws MetadataException {
    if (node == null
        || node.isStorageGroup() && filter != null && !filter.satisfy(node.getFullPath())) {
      return;
    }
    if (targetLevel == 0) {
      res.add(path);
      return;
    }
    for (IMNode child : metadataDiskManager.getChildren(node).values()) {
      findNodes(child, path.concatNode(child.toString()), res, targetLevel - 1, filter);
    }
  }

  @Override
  public String toString() {
    String result = "";
    try {
      JsonObject jsonObject = new JsonObject();
      jsonObject.add(rootName, mNodeToJSON(metadataDiskManager.getRoot(), null));
      result = GSON.toJson(jsonObject);
    } catch (MetadataException e) {
      logger.warn("Failed to serialize MTree to string because ", e);
    }
    return result;
  }

  private JsonObject mNodeToJSON(IMNode node, String storageGroupName) throws MetadataException {
    JsonObject jsonObject = new JsonObject();
    if (node.getChildren().size() > 0) {
      if (node.isStorageGroup()) {
        storageGroupName = node.getFullPath();
      }
      for (String childName : node.getChildren().keySet()) {
        jsonObject.add(
            childName,
            mNodeToJSON(metadataDiskManager.getChild(node, childName), storageGroupName));
      }
    } else if (node.isMeasurement()) {
      MeasurementMNode leafMNode = (MeasurementMNode) node;
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

  @Override
  public Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException {
    Map<String, String> paths = new HashMap<>();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(rootName)) {
      throw new IllegalPathException(path.getFullPath());
    }

    Deque<IMNode> nodeStack = new ArrayDeque<>();
    Deque<Integer> depthStack = new ArrayDeque<>();
    try {
      IMNode root = metadataDiskManager.getRoot();
      if (!root.getChildren().isEmpty()) {
        nodeStack.push(root);
        depthStack.push(0);
      }
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return paths;
    }

    while (!nodeStack.isEmpty()) {
      IMNode mNode = nodeStack.removeFirst();
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
      IMNode mNode,
      Map<String, String> paths,
      Deque<IMNode> nodeStack,
      Deque<Integer> depthStack) {
    String currNode = depth >= nodes.length ? PATH_WILDCARD : nodes[depth];
    for (Map.Entry<String, IMNode> entry : mNode.getChildren().entrySet()) {
      if (!currNode.equals(PATH_WILDCARD) && !currNode.equals(entry.getKey())) {
        continue;
      }
      // this child is desired
      IMNode child = entry.getValue();
      if (child.isStorageGroup()) {
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

  @Override
  public void serializeTo(String snapshotPath) throws IOException {
    try (MLogWriter mLogWriter = new MLogWriter(snapshotPath)) {
      metadataDiskManager.getRoot().serializeTo(mLogWriter);
    } catch (MetadataException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public int getMeasurementMNodeCount(PartialPath path) throws MetadataException {
    IMNode mNode = getNodeByPath(path);
    return getMeasurementMNodeCount(mNode);
  }

  @Override
  public Collection<MeasurementMNode> collectMeasurementMNode(IMNode startingNode) {
    Collection<MeasurementMNode> measurementMNodes = new LinkedList<>();
    Deque<IMNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      IMNode node = nodeDeque.removeFirst();
      if (node instanceof MeasurementMNode) {
        measurementMNodes.add((MeasurementMNode) node);
      } else {
        try {
          Map<String, IMNode> children = metadataDiskManager.getChildren(node);
          if (!children.isEmpty()) {
            nodeDeque.addAll(children.values());
          }
        } catch (MetadataException e) {
          logger.warn("Failed to get children of {} because ", node.getFullPath(), e);
        }
      }
    }
    return measurementMNodes;
  }

  @Override
  public void checkTemplateOnPath(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = metadataDiskManager.getRoot();
    if (!nodeNames[0].equals(rootName)) {
      return;
    }
    if (cur.getDeviceTemplate() != null) {
      throw new MetadataException("Template already exists on " + cur.getFullPath());
    }
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        return;
      }
      cur = metadataDiskManager.getChild(cur, nodeNames[i]);
      if (cur.getDeviceTemplate() != null) {
        throw new MetadataException("Template already exists on " + cur.getFullPath());
      }
    }

    checkTemplateOnSubtree(cur);
  }

  // traverse  all the  descendant of the given path node
  private void checkTemplateOnSubtree(IMNode node) throws MetadataException {
    for (IMNode child : metadataDiskManager.getChildren(node).values()) {
      if (child.getDeviceTemplate() != null) {
        throw new MetadataException("Template already exists on " + child.getFullPath());
      }
      checkTemplateOnSubtree(child);
    }
  }

  @Override
  public void updateMNode(IMNode mNode) throws MetadataException {
    metadataDiskManager.updateMNode(mNode);
  }

  @Override
  public IMNode lockMNode(IMNode mNode) throws MetadataException {
    if (mNode == null) {
      return null;
    }
    try {
      metadataDiskManager.lockMNodeInMemory(mNode);
      return mNode;
    } catch (MetadataException e) {
      return getNodeByPathWithMemoryLock(mNode.getPartialPath());
    }
  }

  @Override
  public void unlockMNode(IMNode mNode) throws MetadataException {
    metadataDiskManager.releaseMNodeMemoryLock(mNode);
  }

  public void unlockMNodePath(IMNode mNode) throws MetadataException {
    while (mNode != null) {
      metadataDiskManager.releaseMNodeMemoryLock(mNode);
      mNode = mNode.getParent();
    }
  }

  private int getMeasurementMNodeCount(IMNode mNode) throws MetadataException {
    int measurementMNodeCount = 0;
    if (mNode.isMeasurement()) {
      measurementMNodeCount += 1; // current node itself may be MeasurementMNode
    }
    IMNode child;
    for (String childName : mNode.getChildren().keySet()) {
      child = metadataDiskManager.getChild(mNode, childName);
      measurementMNodeCount += getMeasurementMNodeCount(child);
    }
    return measurementMNodeCount;
  }

  @Override
  public void createSnapshot() throws IOException {
    metadataDiskManager.createSnapshot();
  }

  @Override
  public void clear() {
    try {
      metadataDiskManager.clear();
    } catch (IOException e) {
      logger.error("We can not clear mtreediskbased because ", e);
    }
  }
}
