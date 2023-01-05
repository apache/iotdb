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

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.AboveDatabaseMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.TraverserWithLimitOffsetWrapper;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementGroupByLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.MeasurementUpdater;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowDevicesResult;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowTimeSeriesResult;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

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
 *   <li>Interfaces and Implementation for metadata info Query
 *       <ol>
 *         <li>Interfaces for Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *         <li>Interfaces for Level Node info Query
 *         <li>Interfaces and Implementation for metadata count
 *       </ol>
 *   <li>Interfaces and Implementation for MNode Query
 *   <li>Interfaces and Implementation for Template check
 * </ol>
 */
public class MTreeBelowSGMemoryImpl implements IMTreeBelowSG {

  // this implementation is based on memory, thus only MTree write operation must invoke MTreeStore
  private MemMTreeStore store;
  private volatile IStorageGroupMNode storageGroupMNode;
  private volatile IMNode rootNode;
  private final Function<IMeasurementMNode, Map<String, String>> tagGetter;
  private int levelOfSG;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGMemoryImpl(
      PartialPath storageGroupPath,
      Function<IMeasurementMNode, Map<String, String>> tagGetter,
      int schemaRegionId) {
    store = new MemMTreeStore(storageGroupPath, true);
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.rootNode = generatePrefix(storageGroupPath, this.storageGroupMNode);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
  }

  private MTreeBelowSGMemoryImpl(
      PartialPath storageGroupPath,
      MemMTreeStore store,
      Function<IMeasurementMNode, Map<String, String>> tagGetter,
      int schemaRegionId) {
    this.store = store;
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.rootNode = generatePrefix(storageGroupPath, this.storageGroupMNode);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
  }

  /**
   * Generate the ancestor nodes of storageGroupNode
   *
   * @return root node
   */
  private IMNode generatePrefix(
      PartialPath storageGroupPath, IStorageGroupMNode storageGroupMNode) {
    String[] nodes = storageGroupPath.getNodes();
    // nodes[0] must be root
    IMNode root = new AboveDatabaseMNode(null, nodes[0]);
    IMNode cur = root;
    IMNode child;
    for (int i = 1; i < nodes.length - 1; i++) {
      child = new AboveDatabaseMNode(cur, nodes[i]);
      cur.addChild(nodes[i], child);
      cur = child;
    }
    storageGroupMNode.setParent(cur);
    cur.addChild(storageGroupMNode);
    return root;
  }

  @Override
  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  protected void replaceStorageGroupMNode(IStorageGroupMNode newMNode) {
    this.storageGroupMNode.getParent().replaceChild(this.storageGroupMNode.getName(), newMNode);
    this.storageGroupMNode = newMNode;
  }

  @Override
  public synchronized boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGMemoryImpl loadFromSnapshot(
      File snapshotDir,
      String storageGroupFullPath,
      int schemaRegionId,
      Consumer<IMeasurementMNode> measurementProcess,
      Function<IMeasurementMNode, Map<String, String>> tagGetter)
      throws IOException, IllegalPathException {
    return new MTreeBelowSGMemoryImpl(
        new PartialPath(storageGroupFullPath),
        MemMTreeStore.loadFromSnapshot(snapshotDir, measurementProcess),
        tagGetter,
        schemaRegionId);
  }

  // endregion

  // region Timeseries operation, including create and delete

  /**
   * Create a timeseries with a full path from root to leaf node. Before creating a timeseries, the
   * database should be set first, throw exception otherwise
   *
   * @param path timeseries path
   * @param dataType data type
   * @param encoding encoding
   * @param compressor compressor
   * @param props props
   * @param alias alias of measurement
   */
  @Override
  public IMeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2) {
      throw new IllegalPathException(path.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(path);
    PartialPath devicePath = path.getDevicePath();
    IMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      IMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

      String leafName = path.getMeasurement();

      if (alias != null && device.hasChild(alias)) {
        throw new AliasAlreadyExistException(path.getFullPath(), alias);
      }

      if (device.hasChild(leafName)) {
        IMNode node = device.getChild(leafName);
        if (node.isMeasurement()) {
          if (node.getAsMeasurementMNode().isPreDeleted()) {
            throw new MeasurementInBlackListException(path);
          } else {
            throw new MeasurementAlreadyExistException(
                path.getFullPath(), node.getAsMeasurementMNode().getMeasurementPath());
          }
        } else {
          throw new PathAlreadyExistException(path.getFullPath());
        }
      }

      if (device.isEntity() && device.getAsEntityMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
            device.getFullPath());
      }

      IEntityMNode entityMNode;
      if (device.isEntity()) {
        entityMNode = device.getAsEntityMNode();
      } else {
        entityMNode = store.setToEntity(device);
        if (entityMNode.isStorageGroup()) {
          replaceStorageGroupMNode(entityMNode.getAsStorageGroupMNode());
        }
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
        entityMNode.addAlias(alias, measurementMNode);
      }
      return measurementMNode;
    }
  }

  /**
   * Create aligned timeseries with full paths from root to one leaf node. Before creating
   * timeseries, the * database should be set first, throw exception otherwise
   *
   * @param devicePath device path
   * @param measurements measurements list
   * @param dataTypes data types list
   * @param encodings encodings list
   * @param compressors compressor
   */
  @Override
  public List<IMeasurementMNode> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException {
    List<IMeasurementMNode> measurementMNodeList = new ArrayList<>();
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    IMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild operation be atomic.
    // only write operations on mtree will be synchronized
    synchronized (this) {
      IMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      for (int i = 0; i < measurements.size(); i++) {
        if (device.hasChild(measurements.get(i))) {
          IMNode node = device.getChild(measurements.get(i));
          if (node.isMeasurement()) {
            if (node.getAsMeasurementMNode().isPreDeleted()) {
              throw new MeasurementInBlackListException(devicePath.concatNode(measurements.get(i)));
            } else {
              throw new MeasurementAlreadyExistException(
                  devicePath.getFullPath() + "." + measurements.get(i),
                  node.getAsMeasurementMNode().getMeasurementPath());
            }
          } else {
            throw new PathAlreadyExistException(
                devicePath.getFullPath() + "." + measurements.get(i));
          }
        }
        if (aliasList != null && aliasList.get(i) != null && device.hasChild(aliasList.get(i))) {
          throw new AliasAlreadyExistException(
              devicePath.getFullPath() + "." + measurements.get(i), aliasList.get(i));
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
        entityMNode = store.setToEntity(device);
        entityMNode.setAligned(true);
        if (entityMNode.isStorageGroup()) {
          replaceStorageGroupMNode(entityMNode.getAsStorageGroupMNode());
        }
      }

      for (int i = 0; i < measurements.size(); i++) {
        IMeasurementMNode measurementMNode =
            MeasurementMNode.getMeasurementMNode(
                entityMNode,
                measurements.get(i),
                new MeasurementSchema(
                    measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i)),
                aliasList == null ? null : aliasList.get(i));
        store.addChild(entityMNode, measurements.get(i), measurementMNode);
        if (aliasList != null && aliasList.get(i) != null) {
          entityMNode.addAlias(aliasList.get(i), measurementMNode);
        }
        measurementMNodeList.add(measurementMNode);
      }
      return measurementMNodeList;
    }
  }

  private IMNode checkAndAutoCreateInternalPath(PartialPath devicePath) throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return null;
    }
    IMNode cur = storageGroupMNode;
    IMNode child;
    String childName;
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
    for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
      childName = nodeNames[i];
      child = cur.getChild(childName);
      if (child == null) {
        child = store.addChild(cur, childName, new InternalMNode(cur, childName));
      }
      cur = child;

      if (cur.isMeasurement()) {
        throw new PathAlreadyExistException(cur.getFullPath());
      }
    }
    return cur;
  }

  private IMNode checkAndAutoCreateDeviceNode(String deviceName, IMNode deviceParent)
      throws PathAlreadyExistException {
    if (deviceParent == null) {
      // device is sg
      return storageGroupMNode;
    }
    IMNode device = store.getChild(deviceParent, deviceName);
    if (device == null) {
      device =
          store.addChild(deviceParent, deviceName, new InternalMNode(deviceParent, deviceName));
    }

    if (device.isMeasurement()) {
      throw new PathAlreadyExistException(device.getFullPath());
    }
    return device;
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    IMNode device = null;
    try {
      device = getNodeByPath(devicePath);
    } catch (PathNotExistException e) {
      return Collections.emptyMap();
    }

    if (!device.isEntity()) {
      return Collections.emptyMap();
    }
    Map<Integer, MetadataException> failingMeasurementMap = new HashMap<>();
    for (int i = 0; i < measurementList.size(); i++) {
      if (device.hasChild(measurementList.get(i))) {
        IMNode node = device.getChild(measurementList.get(i));
        if (node.isMeasurement()) {
          if (node.getAsMeasurementMNode().isPreDeleted()) {
            failingMeasurementMap.put(
                i,
                new MeasurementInBlackListException(devicePath.concatNode(measurementList.get(i))));
          } else {
            failingMeasurementMap.put(
                i,
                new MeasurementAlreadyExistException(
                    devicePath.getFullPath() + "." + measurementList.get(i),
                    node.getAsMeasurementMNode().getMeasurementPath()));
          }
        } else {
          failingMeasurementMap.put(
              i,
              new PathAlreadyExistException(
                  devicePath.getFullPath() + "." + measurementList.get(i)));
        }
      }
      if (aliasList != null && aliasList.get(i) != null && device.hasChild(aliasList.get(i))) {
        failingMeasurementMap.put(
            i,
            new AliasAlreadyExistException(
                devicePath.getFullPath() + "." + measurementList.get(i), aliasList.get(i)));
      }
    }
    return failingMeasurementMap;
  }
  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  @Override
  public Pair<PartialPath, IMeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(
      PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMeasurementMNode deletedNode = getMeasurementMNode(path);
    IEntityMNode parent = deletedNode.getParent();
    // delete the last node of path
    store.deleteChild(parent, path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      parent.addAlias(deletedNode.getAlias(), deletedNode);
    }
    return new Pair<>(deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(parent), deletedNode);
  }

  /** Used when delete timeseries or deactivate template */
  public PartialPath deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(IEntityMNode entityMNode) {
    IMNode curNode = entityMNode;
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      IMNode child;
      IMNodeIterator iterator = store.getChildrenIterator(entityMNode);
      while (iterator.hasNext()) {
        child = iterator.next();
        if (child.isMeasurement()) {
          hasMeasurement = true;
          break;
        }
      }

      if (!hasMeasurement) {
        synchronized (this) {
          curNode = store.setToInternal(entityMNode);
          if (curNode.isStorageGroup()) {
            replaceStorageGroupMNode(curNode.getAsStorageGroupMNode());
          }
        }
      }
    }

    // delete all empty ancestors except database and MeasurementMNode
    while (isEmptyInternalMNode(curNode)) {
      // if current database has no time series, return the database name
      if (curNode.isStorageGroup()) {
        return curNode.getPartialPath();
      }
      store.deleteChild(curNode.getParent(), curNode.getName());
      curNode = curNode.getParent();
    }
    return null;
  }

  @Override
  public boolean isEmptyInternalMNode(IMNode node) {
    return !IoTDBConstant.PATH_ROOT.equals(node.getName())
        && !node.isMeasurement()
        && !node.isUseTemplate()
        && node.getChildren().isEmpty();
  }

  @Override
  public List<PartialPath> constructSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater updater =
        new MeasurementUpdater(rootNode, pathPattern, store, false) {
          @Override
          protected void updateMeasurement(IMeasurementMNode node) {
            node.setPreDeleted(true);
            result.add(getCurrentPartialPath());
          }
        }) {
      updater.update();
    }
    return result;
  }

  @Override
  public List<PartialPath> rollbackSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater updater =
        new MeasurementUpdater(rootNode, pathPattern, store, false) {
          @Override
          protected void updateMeasurement(IMeasurementMNode node) {
            node.setPreDeleted(false);
            result.add(getCurrentPartialPath());
          }
        }) {
      updater.update();
    }
    return result;
  }

  @Override
  public List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            if (node.isPreDeleted()) {
              result.add(getCurrentPartialPath());
            }
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  @Override
  public Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    try (MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            if (node.isPreDeleted()) {
              result.add(getCurrentPartialPath().getDevicePath());
            }
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  @Override
  public void setAlias(IMeasurementMNode measurementMNode, String alias) throws MetadataException {
    store.setAlias(measurementMNode, alias);
  }

  // endregion

  // region Entity/Device operation
  // including device auto creation and transform from InternalMNode to EntityMNode
  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  @Override
  public IMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException {
    MetaFormatUtils.checkTimeseries(deviceId);
    String[] nodeNames = deviceId.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;
    for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
      child = cur.getChild(nodeNames[i]);
      if (child == null) {
        child = store.addChild(cur, nodeNames[i], new InternalMNode(cur, nodeNames[i]));
      }
      cur = child;
    }
    return cur;
  }
  // endregion

  // region Interfaces and Implementation for metadata info Query

  // region Interfaces for Device info Query
  /**
   * Get all devices matching the given path pattern. If isPrefixMatch, then the devices under the
   * paths matching given path pattern will be collected too.
   *
   * @return a list contains all distinct devices names
   */
  @Override
  public Set<PartialPath> getDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    Set<PartialPath> result = new TreeSet<>();
    try (EntityCollector<Set<PartialPath>> collector =
        new EntityCollector<Set<PartialPath>>(rootNode, pathPattern, store, isPrefixMatch) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            result.add(getCurrentPartialPath());
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  @Override
  public List<ShowDevicesResult> getDevices(IShowDevicesPlan plan) throws MetadataException {
    List<ShowDevicesResult> res = new ArrayList<>();
    try (EntityCollector<List<ShowDevicesResult>> collector =
        new EntityCollector<List<ShowDevicesResult>>(
            rootNode, plan.getPath(), store, plan.isPrefixMatch()) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            PartialPath device = getCurrentPartialPath();
            res.add(new ShowDevicesResult(device.getFullPath(), node.isAligned()));
          }
        }) {
      if (plan.usingSchemaTemplate()) {
        collector.setSchemaTemplateFilter(plan.getSchemaTemplateId());
      }
      TraverserWithLimitOffsetWrapper<?> traverser =
          new TraverserWithLimitOffsetWrapper<>(collector, plan.getLimit(), plan.getOffset());
      traverser.traverse();
    }
    return res;
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
  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch, false).left;
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
  @Override
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(rootNode, pathPattern, store, isPrefixMatch) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
            if (nodes[nodes.length - 1].equals(node.getAlias())) {
              // only when user query with alias, the alias in path will be set
              path.setMeasurementAlias(node.getAlias());
            }
            if (withTags) {
              path.setTagMap(tagGetter.apply(node));
            }
            result.add(path);
          }
        }) {
      TraverserWithLimitOffsetWrapper<?> traverser =
          new TraverserWithLimitOffsetWrapper<>(collector, limit, offset);
      traverser.traverse();
      return new Pair<>(result, traverser.getNextOffset());
    }
  }

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
            if (nodes[nodes.length - 1].equals(node.getAlias())) {
              // only when user query with alias, the alias in path will be set
              path.setMeasurementAlias(node.getAlias());
            }
            if (withTags) {
              path.setTagMap(tagGetter.apply(node));
            }
            result.add(path);
          }
        }) {
      collector.setTemplateMap(templateMap);
      collector.setSkipPreDeletedSchema(true);
      collector.traverse();
    }
    return result;
  }

  public List<ShowTimeSeriesResult> getAllMeasurementSchema(
      IShowTimeSeriesPlan plan,
      Function<Long, Pair<Map<String, String>, Map<String, String>>> tagAndAttributeProvider)
      throws MetadataException {
    List<ShowTimeSeriesResult> result = new LinkedList<>();
    try (MeasurementCollector<List<ShowTimeSeriesResult>> collector =
        new MeasurementCollector<List<ShowTimeSeriesResult>>(
            rootNode, plan.getPath(), store, plan.isPrefixMatch()) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            Pair<Map<String, String>, Map<String, String>> tagAndAttribute =
                tagAndAttributeProvider.apply(node.getOffset());
            result.add(
                new ShowTimeSeriesResult(
                    getCurrentPartialPath().getFullPath(),
                    node.getAlias(),
                    (MeasurementSchema) node.getSchema(),
                    tagAndAttribute.left,
                    tagAndAttribute.right));
          }
        }) {
      collector.setTemplateMap(plan.getRelatedTemplate());
      TraverserWithLimitOffsetWrapper<?> traverser =
          new TraverserWithLimitOffsetWrapper<>(collector, plan.getLimit(), plan.getOffset());
      traverser.traverse();
    }
    return result;
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
  @Override
  public Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    Set<TSchemaNode> result = new TreeSet<>();
    try (MNodeCollector<Set<TSchemaNode>> collector =
        new MNodeCollector<Set<TSchemaNode>>(
            rootNode, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store, false) {
          @Override
          protected void transferToResult(IMNode node) {
            result.add(
                new TSchemaNode(
                    getCurrentPartialPath().getFullPath(), node.getMNodeType(false).getNodeType()));
          }
        }) {
      collector.traverse();
    } catch (IllegalPathException e) {
      throw new IllegalPathException(pathPattern.getFullPath());
    }
    return result;
  }

  /** Get all paths from root to the given level */
  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (MNodeCollector<List<PartialPath>> collector =
        new MNodeCollector<List<PartialPath>>(rootNode, pathPattern, store, isPrefixMatch) {
          @Override
          protected void transferToResult(IMNode node) {
            result.add(getCurrentPartialPath());
          }
        }) {
      collector.setTargetLevel(nodeLevel);
      collector.traverse();
    }
    return result;
  }
  // endregion

  // region Interfaces and Implementation for metadata count

  @Override
  public Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    MeasurementGroupByLevelCounter counter =
        new MeasurementGroupByLevelCounter(rootNode, pathPattern, store, isPrefixMatch, level);
    counter.traverse();
    return counter.getResult();
  }

  @Override
  public Map<PartialPath, Long> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      List<String> timeseries,
      boolean hasTag)
      throws MetadataException {
    MeasurementGroupByLevelCounter counter =
        new MeasurementGroupByLevelCounter(
            rootNode, pathPattern, store, isPrefixMatch, level, timeseries, hasTag);
    counter.traverse();
    return counter.getResult();
  }

  // endregion

  // endregion

  // region Interfaces and Implementation for MNode Query
  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  @Override
  public IMNode getNodeByPath(PartialPath path) throws PathNotExistException {
    String[] nodes = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode next;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
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
      cur = next;
    }
    return cur;
  }

  @Override
  public IMeasurementMNode getMeasurementMNode(PartialPath path) throws MetadataException {
    IMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.MEASUREMENT_MNODE_TYPE);
    }
  }

  @Override
  public List<IMeasurementMNode> getAllMeasurementMNode() throws MetadataException {
    IMNode cur = storageGroupMNode;
    // collect all the LeafMNode in this database
    List<IMeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<IMNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      IMNode node = queue.poll();
      IMNodeIterator iterator = store.getChildrenIterator(node);
      IMNode child;
      while (iterator.hasNext()) {
        child = iterator.next();
        if (child.isMeasurement()) {
          leafMNodes.add(child.getAsMeasurementMNode());
        } else {
          queue.add(child);
        }
      }
    }
    return leafMNodes;
  }

  // endregion

  // region Interfaces and Implementation for Template check and query

  @Override
  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    IMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    IEntityMNode entityMNode;

    synchronized (this) {
      for (String measurement : template.getSchemaMap().keySet()) {
        if (cur.hasChild(measurement)) {
          throw new TemplateImcompatibeException(
              activatePath.concatNode(measurement).getFullPath(), template.getName());
        }
      }

      if (cur.isUseTemplate()) {
        throw new TemplateIsInUseException(cur.getFullPath());
      }

      if (cur.isEntity()) {
        entityMNode = cur.getAsEntityMNode();
      } else {
        entityMNode = store.setToEntity(cur);
        if (entityMNode.isStorageGroup()) {
          replaceStorageGroupMNode(entityMNode.getAsStorageGroupMNode());
        }
      }
    }

    if (!entityMNode.isAligned()) {
      entityMNode.setAligned(template.isDirectAligned());
    }
    entityMNode.setUseTemplate(true);
    entityMNode.setSchemaTemplateId(template.getId());
  }

  public void activateTemplateWithoutCheck(
      PartialPath activatePath, int templateId, boolean isAligned) {
    String[] nodes = activatePath.getNodes();
    IMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    IEntityMNode entityMNode;
    if (cur.isEntity()) {
      entityMNode = cur.getAsEntityMNode();
    } else {
      entityMNode = store.setToEntity(cur);
      if (entityMNode.isStorageGroup()) {
        replaceStorageGroupMNode(entityMNode.getAsStorageGroupMNode());
      }
    }

    if (!entityMNode.isAligned()) {
      entityMNode.setAligned(isAligned);
    }
    entityMNode.setUseTemplate(true);
    entityMNode.setSchemaTemplateId(templateId);
  }

  public List<IEntityMNode> getDeviceMNodeUsingTargetTemplate(
      PartialPath pathPattern, List<Integer> templateIdList) throws MetadataException {
    List<IEntityMNode> result = new ArrayList<>();
    try (EntityCollector<List<IEntityMNode>> collector =
        new EntityCollector<List<IEntityMNode>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            if (templateIdList.contains(node.getSchemaTemplateId())) {
              result.add(node);
            }
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  public List<IEntityMNode> getPreDeactivatedDeviceMNode(
      PartialPath pathPattern, List<Integer> templateIdList) throws MetadataException {
    List<IEntityMNode> result = new ArrayList<>();
    try (EntityCollector<List<IEntityMNode>> collector =
        new EntityCollector<List<IEntityMNode>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            if (templateIdList.contains(node.getSchemaTemplateId())
                && node.isPreDeactivateTemplate()) {
              result.add(node);
            }
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  @Override
  public long countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    // TODO: delete it
    final int[] count = {0};
    EntityCollector<Void> collector =
        new EntityCollector<Void>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            if (node.isEntity() && node.getAsEntityMNode().getSchemaTemplateId() == templateId) {
              count[0]++;
            }
          }
        };
    collector.traverse();
    return count[0];
  }

  // endregion
}
