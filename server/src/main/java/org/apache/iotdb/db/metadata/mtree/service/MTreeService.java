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
package org.apache.iotdb.db.metadata.mtree.service;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.service.traverser.collector.CollectorTraverser;
import org.apache.iotdb.db.metadata.mtree.service.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.service.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.service.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.service.traverser.collector.StorageGroupCollector;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.MNodeLevelCounter;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.MeasurementGroupByLevelCounter;
import org.apache.iotdb.db.metadata.mtree.service.traverser.counter.StorageGroupCounter;
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_MATCH_PATTERN;
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
 * </ol>
 */
public class MTreeService implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(MTreeService.class);
  private IMNode root;
  private IMTreeStore store;

  // region MTree initialization, clear and serialization
  public MTreeService() {}

  public void init() throws IOException {
    if (IoTDBDescriptor.getInstance().getConfig().isEnablePersistentSchema()) {
      store = new CachedMTreeStore();
    } else {
      store = new MemMTreeStore();
    }

    store.init();
    this.root = store.getRoot();
  }

  public void clear() {
    store.clear();
    root = store.getRoot();
  }

  public void createSnapshot() throws IOException {
    store.createSnapshot();
  }

  @Override
  public String toString() {
    return store.toString();
  }
  // endregion

  // region Timeseries operation, including create and delete

  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    unPinMNode(
        createTimeseriesWithPinnedReturn(path, dataType, encoding, compressor, props, alias));
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
  public IMeasurementMNode createTimeseriesWithPinnedReturn(
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
    Pair<IMNode, Template> pair = checkAndAutoCreateInternalPath(path.getDevicePath());
    IMNode device = pair.left;
    Template upperTemplate = pair.right;

    try {
      MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

      String leafName = path.getMeasurement();

      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        if (store.hasChild(device, leafName)) {
          throw new PathAlreadyExistException(path.getFullPath());
        }

        if (alias != null && store.hasChild(device, alias)) {
          throw new AliasAlreadyExistException(path.getFullPath(), alias);
        }

        if (upperTemplate != null
            && (upperTemplate.getDirectNode(leafName) != null
                || upperTemplate.getDirectNode(alias) != null)) {
          throw new TemplateImcompatibeException(path.getFullPath(), upperTemplate.getName());
        }

        if (device.isEntity() && device.getAsEntityMNode().isAligned()) {
          throw new AlignedTimeseriesException(
              "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
              device.getFullPath());
        }

        IEntityMNode entityMNode;
        if (device.isEntity()) {
          entityMNode = device.getAsEntityMNode();
        } else {
          entityMNode = MNodeUtils.setToEntity(device);
          store.updateMNode(entityMNode);
          device = entityMNode;
        }

        IMeasurementMNode measurementMNode =
            MeasurementMNode.getMeasurementMNode(
                entityMNode,
                leafName,
                new MeasurementSchema(leafName, dataType, encoding, compressor, props),
                alias);
        store.addChild(entityMNode, leafName, measurementMNode);
        // link alias to LeafMNode
        if (alias != null) {
          store.addAlias(entityMNode, alias, measurementMNode);
        }
        return measurementMNode;
      }
    } finally {
      unPinMNode(device);
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
    IMNode device = pair.left;
    Template upperTemplate = pair.right;

    try {
      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        for (String measurement : measurements) {
          if (store.hasChild(device, measurement)) {
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

        if (device.isEntity() && !device.getAsEntityMNode().isAligned()) {
          throw new AlignedTimeseriesException(
              "Timeseries under this entity is not aligned, please use createTimeseries or change entity.",
              devicePath.getFullPath());
        }

        IEntityMNode entityMNode;
        if (device.isEntity()) {
          entityMNode = device.getAsEntityMNode();
        } else {
          entityMNode = MNodeUtils.setToEntity(device);
          entityMNode.setAligned(true);
          store.updateMNode(entityMNode);
          device = entityMNode;
        }

        for (int i = 0; i < measurements.size(); i++) {
          IMeasurementMNode measurementMNode =
              MeasurementMNode.getMeasurementMNode(
                  entityMNode,
                  measurements.get(i),
                  new MeasurementSchema(
                      measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i)),
                  null);
          store.addChild(entityMNode, measurements.get(i), measurementMNode);
          unPinMNode(measurementMNode);
        }
      }
    } finally {
      unPinMNode(device);
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
    IMNode child;
    boolean hasSetStorageGroup = false;
    Template upperTemplate = cur.getSchemaTemplate();
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length; i++) {
      String childName = nodeNames[i];
      child = store.getChild(cur, childName);
      if (child == null) {
        if (!hasSetStorageGroup) {
          unPinPath(cur);
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        if (upperTemplate != null && upperTemplate.getDirectNode(childName) != null) {
          unPinPath(cur);
          throw new TemplateImcompatibeException(
              devicePath.getFullPath(), upperTemplate.getName(), childName);
        }
        child = new InternalMNode(cur, childName);
        store.addChild(cur, childName, child);
      }
      cur = child;

      if (cur.isMeasurement()) {
        unPinPath(cur);
        throw new PathAlreadyExistException(cur.getFullPath());
      }
      if (cur.isStorageGroup()) {
        hasSetStorageGroup = true;
      }

      if (cur.getSchemaTemplate() != null) {
        upperTemplate = cur.getSchemaTemplate();
      }
    }
    unPinPath(cur.getParent());
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
      throw new MetadataException("Cannot delete a timeseries inside a template: " + path);
    }

    IMeasurementMNode deletedNode = getMeasurementMNode(path);
    IEntityMNode parent = deletedNode.getParent();
    // delete the last node of path
    store.deleteChild(parent, path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      store.deleteAliasChild(parent, deletedNode.getAlias());
    }
    IMNode curNode = parent;
    if (!parent.isUseTemplate()) {
      boolean hasMeasurement = false;
      IMNode child;
      IMNodeIterator iterator = store.getChildrenIterator(parent);
      try {
        while (iterator.hasNext()) {
          child = iterator.next();
          unPinMNode(child);
          if (child.isMeasurement()) {
            hasMeasurement = true;
            break;
          }
        }
      } finally {
        iterator.close();
      }

      if (!hasMeasurement) {
        synchronized (this) {
          curNode = MNodeUtils.setToInternal(parent);
          store.updateMNode(curNode);
        }
      }
    }

    // delete all empty ancestors except storage group and MeasurementMNode
    while (isEmptyInternalMNode(curNode)) {
      // if current storage group has no time series, return the storage group name
      if (curNode.isStorageGroup()) {
        return new Pair<>(curNode.getPartialPath(), deletedNode);
      }
      store.deleteChild(curNode.getParent(), curNode.getName());
      curNode = curNode.getParent();
    }
    unPinMNode(curNode);
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
    IMNode child;
    Template upperTemplate = cur.getSchemaTemplate();
    for (int i = 1; i < nodeNames.length; i++) {
      child = store.getChild(cur, nodeNames[i]);
      if (child == null) {
        if (cur.isUseTemplate() && upperTemplate.getDirectNode(nodeNames[i]) != null) {
          unPinPath(cur);
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        if (i == sgLevel) {
          child =
              new StorageGroupMNode(
                  cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
        } else {
          child = new InternalMNode(cur, nodeNames[i]);
        }
        store.addChild(cur, nodeNames[i], child);
      }
      cur = child;
      // update upper template
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
    }
    unPinPath(cur.getParent());
    return cur;
  }

  public IEntityMNode setToEntity(IMNode node) {
    // synchronize check and replace, we need replaceChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      IEntityMNode entityMNode = MNodeUtils.setToEntity(node);
      store.updateMNode(entityMNode);
      return entityMNode;
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
    IMNode child;
    Template upperTemplate = cur.getSchemaTemplate();
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      child = store.getChild(cur, nodeNames[i]);
      if (child == null) {
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          unPinPath(cur);
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        child = new InternalMNode(cur, nodeNames[i]);
        store.addChild(cur, nodeNames[i], child);
      } else if (child.isStorageGroup()) {
        // before set storage group, check whether the exists or not
        unPinPath(cur);
        throw new StorageGroupAlreadySetException(child.getFullPath());
      }
      cur = child;
      upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
      i++;
    }

    // synchronize check and add, we need addChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      child = store.getChild(cur, nodeNames[i]);
      if (child != null) {
        unPinPath(child);
        // node b has child sg
        if (child.isStorageGroup()) {
          throw new StorageGroupAlreadySetException(path.getFullPath());
        } else {
          throw new StorageGroupAlreadySetException(path.getFullPath(), true);
        }
      } else {
        if (cur.isUseTemplate() && upperTemplate.hasSchema(nodeNames[i])) {
          unPinPath(cur);
          throw new PathAlreadyExistException(
              cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
        }
        IStorageGroupMNode storageGroupMNode =
            new StorageGroupMNode(
                cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
        store.addChild(cur, nodeNames[i], storageGroupMNode);
        unPinPath(storageGroupMNode);
      }
    }
  }

  /** Delete a storage group */
  public List<IMeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    IMNode cur = getNodeByPath(path);
    if (!(cur.isStorageGroup())) {
      unPinMNode(cur);
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    List<IMeasurementMNode> leafMNodes = store.deleteChild(cur.getParent(), cur.getName());

    cur = cur.getParent();
    // delete node b while retain root.a.sg2
    while (isEmptyInternalMNode(cur)) {
      store.deleteChild(cur.getParent(), cur.getName());
      cur = cur.getParent();
    }
    unPinMNode(cur);
    return leafMNodes;
  }

  public boolean isEmptyInternalMNode(IMNode node) {
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      return !IoTDBConstant.PATH_ROOT.equals(node.getName())
          && !node.isStorageGroup()
          && !node.isMeasurement()
          && node.getSchemaTemplate() == null
          && !node.isUseTemplate()
          && !iterator.hasNext();
    } finally {
      iterator.close();
    }
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
    if (!nodeNames[0].equals(root.getName())) {
      return false;
    }
    IMNode cur = root;
    IMNode child;
    Template upperTemplate = null;
    boolean isInTemplate = false;
    for (int i = 1; i < nodeNames.length; i++) {
      if (isInTemplate) {
        child = cur.getChild(nodeNames[i]);
        if (child == null) {
          return false;
        }
        if (child.isMeasurement()) {
          return i == nodeNames.length - 1;
        }
      } else {
        upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          unPinPath(cur);
          if (upperTemplate == null
              || !cur.isUseTemplate()
              || upperTemplate.getDirectNode(nodeNames[i]) == null) {
            return false;
          }
          child = upperTemplate.getDirectNode(nodeNames[i]);
          isInTemplate = true;
          if (child.isMeasurement()) {
            return i == nodeNames.length - 1;
          }
        } else {
          if (child.isMeasurement()) {
            unPinPath(cur);
            return i == nodeNames.length - 1;
          }
        }
      }
      cur = child;
    }
    if (!isInTemplate) {
      unPinPath(cur);
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
    IMNode next = null;
    int i = 1;
    while (i < nodeNames.length - 1) {
      next = store.getChild(cur, nodeNames[i]);
      if (next == null) {
        unPinPath(cur);
        return false;
      } else if (next.isStorageGroup()) {
        unPinPath(next);
        return false;
      }
      i++;
      cur = next;
    }
    next = store.getChild(cur, nodeNames[i]);
    if (next == null) {
      unPinPath(cur);
      return false;
    } else {
      unPinPath(next);
      return next.isStorageGroup();
    }
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    IMNode cur = root;
    IMNode next = null;
    for (int i = 1; i < nodes.length; i++) {
      next = store.getChild(cur, nodes[i]);
      if (next == null) {
        unPinPath(cur);
        return false;
      } else if (next.isStorageGroup()) {
        unPinPath(next);
        return true;
      }
      cur = next;
    }
    unPinPath(cur);
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
    IMNode next = null;
    for (int i = 1; i < nodes.length; i++) {
      next = store.getChild(cur, nodes[i]);
      if (next == null) {
        unPinPath(cur);
        throw new StorageGroupNotSetException(path.getFullPath());
      } else if (next.isStorageGroup()) {
        unPinPath(next);
        return next.getPartialPath();
      }
      cur = next;
    }
    unPinPath(cur);
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
        new StorageGroupCollector<List<PartialPath>>(root, pathPattern, store) {
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
  public List<PartialPath> getAllStorageGroupPaths() throws MetadataException {
    return getMatchedStorageGroups(ALL_MATCH_PATTERN, false);
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
        new StorageGroupCollector<Map<String, String>>(root, path, store) {
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
        new EntityCollector<Set<PartialPath>>(root, pathPattern, store) {
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
            root, plan.getPath(), store, plan.getLimit(), plan.getOffset()) {
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
        new MeasurementCollector<Set<PartialPath>>(root, timeseries, store) {
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
        new MeasurementCollector<List<PartialPath>>(root, pathPattern, store, limit, offset) {
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
            root, pathPattern, store, limit, offset) {
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

  public Map<PartialPath, IMeasurementSchema> getAllMeasurementSchemaByPrefix(
      PartialPath prefixPath) throws MetadataException {
    Map<PartialPath, IMeasurementSchema> result = new HashMap<>();
    MeasurementCollector<List<IMeasurementSchema>> collector =
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath, store) {
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
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath, store) {
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
        new MeasurementCollector<List<IMeasurementSchema>>(root, prefixPath, store) {
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
          new MNodeCollector<Set<String>>(
              root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
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
          new MNodeCollector<Set<String>>(
              root, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
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
        new MNodeCollector<List<PartialPath>>(root, pathPattern, store) {
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
    CounterTraverser counter = new MeasurementCounter(root, pathPattern, store);
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
    CounterTraverser counter = new EntityCounter(root, pathPattern, store);
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
    CounterTraverser counter = new StorageGroupCounter(root, pathPattern, store);
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
    MNodeLevelCounter counter = new MNodeLevelCounter(root, pathPattern, store, level);
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
        new MeasurementGroupByLevelCounter(root, pathPattern, store, level);
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
    IMNode next;
    Template upperTemplate = cur.getSchemaTemplate();
    boolean isInTemplate = false;
    for (int i = 1; i < nodes.length; i++) {
      if (isInTemplate) {
        next = cur.getChild(nodes[i]);
        if (next == null) {
          throw new PathNotExistException(path.getFullPath(), true);
        } else if (next.isMeasurement()) {
          if (i == nodes.length - 1) {
            return next;
          } else {
            throw new PathNotExistException(path.getFullPath(), true);
          }
        }
      } else {
        if (cur.getSchemaTemplate() != null) {
          upperTemplate = cur.getSchemaTemplate();
        }
        next = store.getChild(cur, nodes[i]);
        if (next == null) {
          unPinPath(cur);
          if (upperTemplate == null
              || !cur.isUseTemplate()
              || upperTemplate.getDirectNode(nodes[i]) == null) {
            throw new PathNotExistException(path.getFullPath(), true);
          }
          next = upperTemplate.getDirectNode(nodes[i]);
          isInTemplate = true;
        } else if (next.isMeasurement()) {
          unPinPath(cur);
          if (i == nodes.length - 1) {
            return next;
          } else {
            throw new PathNotExistException(path.getFullPath(), true);
          }
        }
      }
      cur = next;
    }
    if (!isInTemplate) {
      unPinPath(cur.getParent());
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
    IMNode child;
    Template upperTemplate = null;
    boolean isInTemplate = false;
    for (int i = 1; i < nodes.length; i++) {
      if (isInTemplate) {
        child = cur.getChild(nodes[i]);
        if (child == null) {
          throw new PathNotExistException(path.getFullPath());
        }
      } else {
        if (cur.getSchemaTemplate() != null) {
          upperTemplate = cur.getSchemaTemplate();
        }
        child = store.getChild(cur, nodes[i]);
        if (child == null) {
          unPinPath(cur);
          // seek child in template
          if (!storageGroupChecked) {
            throw new StorageGroupNotSetException(path.getFullPath());
          }

          if (upperTemplate == null
              || !cur.isUseTemplate()
              || upperTemplate.getDirectNode(nodes[i]) == null) {
            throw new PathNotExistException(path.getFullPath());
          }

          child = upperTemplate.getDirectNode(nodes[i]);
          isInTemplate = true;
        } else if (child.isStorageGroup()) {
          storageGroupChecked = true;
        }
      }
      cur = child;
    }

    if (!storageGroupChecked) {
      unPinPath(cur);
      throw new StorageGroupNotSetException(path.getFullPath());
    }

    if (!isInTemplate) {
      unPinPath(cur.getParent());
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
    IMNode next = null;
    for (int i = 1; i < nodes.length; i++) {
      next = store.getChild(cur, nodes[i]);
      if (next == null) {
        unPinPath(cur);
        throw new StorageGroupNotSetException(path.getFullPath());
      }
      if (next.isStorageGroup()) {
        unPinPath(next);
        return next.getAsStorageGroupMNode();
      }
      cur = next;
    }
    unPinPath(cur);
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /** Get all storage group MNodes */
  public List<IStorageGroupMNode> getAllStorageGroupNodes() throws MetadataException {
    List<IStorageGroupMNode> result = new LinkedList<>();
    StorageGroupCollector<List<IStorageGroupMNode>> collector =
        new StorageGroupCollector<List<IStorageGroupMNode>>(root, ALL_MATCH_PATTERN, store) {
          @Override
          protected void collectStorageGroup(IStorageGroupMNode node) {
            result.add(node);
          }
        };
    collector.traverse();
    return result;
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath path) throws MetadataException {
    IMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      unPinMNode(node);
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
    if (!nodeNames[0].equals(root.getName())) {
      return;
    }
    IMNode cur = root;
    IMNode child;
    for (int i = 1; i < nodeNames.length; i++) {
      child = store.getChild(cur, nodeNames[i]);
      if (child == null) {
        unPinPath(cur);
        return;
      }
      cur = child;
      if (cur.getSchemaTemplate() != null) {
        unPinPath(cur);
        throw new MetadataException("Template already exists on " + cur.getFullPath());
      }
      if (cur.isMeasurement()) {
        unPinPath(cur);
        return;
      }
    }

    try {
      checkTemplateOnSubtree(cur);
    } finally {
      unPinPath(cur);
    }
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
        IMNode child;
        IMNodeIterator iterator = store.getChildrenIterator(mountedNode);
        try {
          while (iterator.hasNext()) {
            child = iterator.next();
            unPinMNode(child);
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
        } finally {
          iterator.close();
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
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
          if (child.isMeasurement()) {
            continue;
          }
          if (child.getSchemaTemplate() != null) {
            throw new MetadataException("Template already exists on " + child.getFullPath());
          }
          checkTemplateOnSubtree(child);
        } finally {
          unPinMNode(child);
        }
      }
    } finally {
      iterator.close();
    }
  }

  public void checkIsTemplateCompatibleWithChild(IMNode node, Template template)
      throws MetadataException {
    for (String measurementPath : template.getSchemaMap().keySet()) {
      String directNodeName = MetaUtils.splitPathToDetachedPath(measurementPath)[0];
      if (store.hasChild(node, directNodeName)) {
        throw new MetadataException(
            "Node name "
                + directNodeName
                + " in template has conflict with node's child "
                + (node.getFullPath() + "." + directNodeName));
      }
    }
  }

  public void checkTemplateInUseOnLowerNode(IMNode node) throws TemplateIsInUseException {
    if (node.isMeasurement()) {
      return;
    }
    IMNode child;
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      while (iterator.hasNext()) {
        child = iterator.next();
        try {
          if (child.isMeasurement()) {
            continue;
          }
          if (child.isUseTemplate()) {
            throw new TemplateIsInUseException(child.getFullPath());
          }
          checkTemplateInUseOnLowerNode(child);
        } finally {
          unPinMNode(child);
        }
      }
    } finally {
      iterator.close();
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
    IMNode child;
    Template upperTemplate = cur.getUpperTemplate();
    for (int i = 1; i < pathNodes.length; i++) {
      child = store.getChild(cur, pathNodes[i]);
      if (child != null) {
        cur = child;
        if (cur.isMeasurement()) {
          unPinPath(cur);
          return false;
        }
        upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
      } else {
        unPinPath(cur);
        if (upperTemplate != null) {
          String suffixPath =
              new PartialPath(Arrays.copyOfRange(pathNodes, i, pathNodes.length)).toString();
          // has template, whether match
          return upperTemplate.hasSchema(suffixPath);
        } else {
          // no child and no template
          return false;
        }
      }
    }
    unPinPath(cur);
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
    if (!root.getName().equals(fullPathNodes[0])) {
      throw new IllegalPathException(measurementPath.toString());
    }

    IMNode cur = root;
    IMNode child;
    Template upperTemplate = cur.getSchemaTemplate();
    for (int index = 1; index < fullPathNodes.length; index++) {
      upperTemplate = cur.getSchemaTemplate() != null ? cur.getSchemaTemplate() : upperTemplate;
      child = store.getChild(cur, fullPathNodes[index]);
      if (child == null) {
        unPinPath(cur);
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
        }

        // no matched child, no template, need to create device node as logical device path
        return fullPathNodes.length - 1;
      } else {
        // has child on MTree
        cur = child;
      }
    }
    unPinPath(cur);
    // all nodes on path exist in MTree, device node should be the penultimate one
    return fullPathNodes.length - 1;
  }

  public List<String> getPathsSetOnTemplate(String templateName) throws MetadataException {
    List<String> resSet = new ArrayList<>();
    CollectorTraverser<Set<String>> setTemplatePaths =
        new CollectorTraverser<Set<String>>(
            root, root.getPartialPath().concatNode(MULTI_LEVEL_PATH_WILDCARD), store) {
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
            root, root.getPartialPath().concatNode(MULTI_LEVEL_PATH_WILDCARD), store) {
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

  // region Interfaces and Implementation for Pin/UnPin MNode or Path
  public void pinMNode(IMNode node) {
    store.pin(node);
  }

  public void unPinPath(IMNode node) {
    while (node.getParent() != null) {
      store.unPin(node);
      node = node.getParent();
    }
  }

  public void unPinMNode(IMNode node) {
    store.unPin(node);
  }

  public void updateMNode(IMNode node) {
    store.updateMNode(node);
  }

  public IMNode getChildFromPinnedMNode(IMNode parent, String measurement) {
    return store.getChild(parent, measurement);
  }

  // endregion

}
