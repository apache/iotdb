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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.template.DifferentTemplateException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.exception.quota.ExceedQuotaException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.mem.IMemMNode;
import org.apache.iotdb.db.metadata.mnode.mem.factory.MemMNodeFactory;
import org.apache.iotdb.db.metadata.mnode.mem.impl.LogicalViewSchema;
import org.apache.iotdb.db.metadata.mnode.mem.info.LogicalViewInfo;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;
import org.apache.iotdb.db.metadata.mtree.traverser.TraverserWithLimitOffsetWrapper;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.EntityUpdater;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.MeasurementUpdater;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowDevicesResult;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowNodesResult;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.quotas.DataNodeSpaceQuotaManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
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
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

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
 *   <li>Interfaces and Implementation for MNode Query
 *   <li>Interfaces and Implementation for Template check
 * </ol>
 */
public class MTreeBelowSGMemoryImpl {

  // this implementation is based on memory, thus only MTree write operation must invoke MTreeStore
  private final MemMTreeStore store;
  private volatile IMemMNode storageGroupMNode;
  private final IMemMNode rootNode;
  private final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter;
  private final IMNodeFactory<IMemMNode> nodeFactory = MemMNodeFactory.getInstance();
  private final int levelOfSG;
  private final MemSchemaRegionStatistics regionStatistics;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGMemoryImpl(
      PartialPath storageGroupPath,
      Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter,
      MemSchemaRegionStatistics regionStatistics) {
    store = new MemMTreeStore(storageGroupPath, regionStatistics);
    this.regionStatistics = regionStatistics;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
  }

  private MTreeBelowSGMemoryImpl(
      PartialPath storageGroupPath,
      MemMTreeStore store,
      Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter,
      MemSchemaRegionStatistics regionStatistics) {
    this.store = store;
    this.regionStatistics = regionStatistics;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
  }

  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  protected void replaceStorageGroupMNode(IDatabaseMNode<IMemMNode> newMNode) {
    this.storageGroupMNode
        .getParent()
        .replaceChild(this.storageGroupMNode.getName(), newMNode.getAsMNode());
    this.storageGroupMNode = newMNode.getAsMNode();
  }

  public synchronized boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGMemoryImpl loadFromSnapshot(
      File snapshotDir,
      String storageGroupFullPath,
      MemSchemaRegionStatistics regionStatistics,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter)
      throws IOException, IllegalPathException {
    return new MTreeBelowSGMemoryImpl(
        new PartialPath(storageGroupFullPath),
        MemMTreeStore.loadFromSnapshot(
            snapshotDir, measurementProcess, deviceProcess, regionStatistics),
        tagGetter,
        regionStatistics);
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
  public IMeasurementMNode<IMemMNode> createTimeseries(
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
    IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

      String leafName = path.getMeasurement();

      if (alias != null && device.hasChild(alias)) {
        throw new AliasAlreadyExistException(path.getFullPath(), alias);
      }

      if (device.hasChild(leafName)) {
        IMemMNode node = device.getChild(leafName);
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

      if (device.isDevice() && device.getAsDeviceMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
            device.getFullPath());
      }

      IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
        if (entityMNode.isDatabase()) {
          replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
        }
      }

      IMeasurementMNode<IMemMNode> measurementMNode =
          nodeFactory.createMeasurementMNode(
              entityMNode,
              leafName,
              new MeasurementSchema(leafName, dataType, encoding, compressor, props),
              alias);
      store.addChild(entityMNode.getAsMNode(), leafName, measurementMNode.getAsMNode());
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
  public List<IMeasurementMNode<IMemMNode>> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException {
    List<IMeasurementMNode<IMemMNode>> measurementMNodeList = new ArrayList<>();
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild operation be atomic.
    // only write operations on mtree will be synchronized
    synchronized (this) {
      IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      for (int i = 0; i < measurements.size(); i++) {
        if (device.hasChild(measurements.get(i))) {
          IMemMNode node = device.getChild(measurements.get(i));
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

      if (device.isDevice() && !device.getAsDeviceMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is not aligned, please use createTimeseries or change entity.",
            devicePath.getFullPath());
      }

      IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
        entityMNode.setAligned(true);
        if (entityMNode.isDatabase()) {
          replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
        }
      }

      for (int i = 0; i < measurements.size(); i++) {
        IMeasurementMNode<IMemMNode> measurementMNode =
            nodeFactory.createMeasurementMNode(
                entityMNode,
                measurements.get(i),
                new MeasurementSchema(
                    measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i)),
                aliasList == null ? null : aliasList.get(i));
        store.addChild(
            entityMNode.getAsMNode(), measurements.get(i), measurementMNode.getAsMNode());
        if (aliasList != null && aliasList.get(i) != null) {
          entityMNode.addAlias(aliasList.get(i), measurementMNode);
        }
        measurementMNodeList.add(measurementMNode);
      }
      return measurementMNodeList;
    }
  }

  private IMemMNode checkAndAutoCreateInternalPath(PartialPath devicePath)
      throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return null;
    }
    IMemMNode cur = storageGroupMNode;
    IMemMNode child;
    String childName;
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
    for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
      childName = nodeNames[i];
      child = cur.getChild(childName);
      if (child == null) {
        child = store.addChild(cur, childName, nodeFactory.createInternalMNode(cur, childName));
      }
      cur = child;

      if (cur.isMeasurement()) {
        throw new PathAlreadyExistException(cur.getFullPath());
      }
    }
    return cur;
  }

  private IMemMNode checkAndAutoCreateDeviceNode(String deviceName, IMemMNode deviceParent)
      throws PathAlreadyExistException, ExceedQuotaException {
    if (deviceParent == null) {
      // device is sg
      return storageGroupMNode;
    }
    IMemMNode device = store.getChild(deviceParent, deviceName);
    if (device == null) {
      if (IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
        if (!DataNodeSpaceQuotaManager.getInstance()
            .checkDeviceLimit(storageGroupMNode.getName())) {
          throw new ExceedQuotaException(
              "The number of devices has reached the upper limit",
              TSStatusCode.SPACE_QUOTA_EXCEEDED.getStatusCode());
        }
      }
      device =
          store.addChild(
              deviceParent, deviceName, nodeFactory.createInternalMNode(deviceParent, deviceName));
    }

    if (device.isMeasurement()) {
      throw new PathAlreadyExistException(device.getFullPath());
    }
    return device;
  }

  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    IMemMNode device;
    try {
      device = getNodeByPath(devicePath);
    } catch (PathNotExistException e) {
      return Collections.emptyMap();
    }

    if (!device.isDevice()) {
      return Collections.emptyMap();
    }
    Map<Integer, MetadataException> failingMeasurementMap = new HashMap<>();
    for (int i = 0; i < measurementList.size(); i++) {
      if (device.hasChild(measurementList.get(i))) {
        IMemMNode node = device.getChild(measurementList.get(i));
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
      if (IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
        if (!DataNodeSpaceQuotaManager.getInstance()
            .checkTimeSeriesNum(storageGroupMNode.getName())) {
          failingMeasurementMap.put(
              i,
              new ExceedQuotaException(
                  "The number of timeSeries has reached the upper limit",
                  TSStatusCode.SPACE_QUOTA_EXCEEDED.getStatusCode()));
        }
      }
    }
    return failingMeasurementMap;
  }
  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  public IMeasurementMNode<IMemMNode> deleteTimeseries(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMeasurementMNode<IMemMNode> deletedNode = getMeasurementMNode(path);
    IMemMNode parent = deletedNode.getParent();
    // delete the last node of path
    store.deleteChild(parent, path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      parent.getAsDeviceMNode().deleteAliasChild(deletedNode.getAlias());
    }
    deleteEmptyInternalMNode(parent.getAsDeviceMNode());
    return deletedNode;
  }

  /** Used when delete timeseries or deactivate template */
  public void deleteEmptyInternalMNode(IDeviceMNode<IMemMNode> entityMNode) {
    IMemMNode curNode = entityMNode.getAsMNode();
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      IMemMNode child;
      IMNodeIterator<IMemMNode> iterator = store.getChildrenIterator(curNode);
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
          if (curNode.isDatabase()) {
            replaceStorageGroupMNode(curNode.getAsDatabaseMNode());
          }
        }
      }
    }

    // delete all empty ancestors except database and MeasurementMNode
    while (isEmptyInternalMNode(curNode)) {
      // if current database has no time series, return the database name
      if (curNode.isDatabase()) {
        return;
      }
      store.deleteChild(curNode.getParent(), curNode.getName());
      curNode = curNode.getParent();
    }
  }

  private boolean isEmptyInternalMNode(IMemMNode node) {
    return !IoTDBConstant.PATH_ROOT.equals(node.getName())
        && !node.isMeasurement()
        && !(node.isDevice() && node.getAsDeviceMNode().isUseTemplate())
        && node.getChildren().isEmpty();
  }

  public List<PartialPath> constructSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(rootNode, pathPattern, store, false) {

          protected void updateMeasurement(IMeasurementMNode<IMemMNode> node) {
            node.setPreDeleted(true);
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> rollbackSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(rootNode, pathPattern, store, false) {

          protected void updateMeasurement(IMeasurementMNode<IMemMNode> node) {
            node.setPreDeleted(false);
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            if (node.isPreDeleted()) {
              result.add(getPartialPathFromRootToNode(node.getAsMNode()));
            }
            return null;
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  public Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    try (MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            if (node.isPreDeleted()) {
              result.add(getPartialPathFromRootToNode(node.getAsMNode()).getDevicePath());
            }
            return null;
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  public void setAlias(IMeasurementMNode<IMemMNode> measurementMNode, String alias)
      throws MetadataException {
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
  public IMemMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException {
    MetaFormatUtils.checkTimeseries(deviceId);
    String[] nodeNames = deviceId.getNodes();
    IMemMNode cur = storageGroupMNode;
    IMemMNode child;
    for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
      child = cur.getChild(nodeNames[i]);
      if (child == null) {
        child =
            store.addChild(cur, nodeNames[i], nodeFactory.createInternalMNode(cur, nodeNames[i]));
      }
      cur = child;
    }
    return cur;
  }
  // endregion

  // region Interfaces and Implementation for metadata info Query

  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
            if (nodes[nodes.length - 1].equals(node.getAlias())) {
              // only when user query with alias, the alias in path will be set
              path.setMeasurementAlias(node.getAlias());
            }
            if (withTags) {
              path.setTagMap(tagGetter.apply(node));
            }
            result.add(path);
            return null;
          }
        }) {
      collector.setTemplateMap(templateMap, nodeFactory);
      collector.setSkipPreDeletedSchema(true);
      collector.traverse();
    }
    return result;
  }

  // endregion

  // region Interfaces and Implementation for MNode Query
  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  public IMemMNode getNodeByPath(PartialPath path) throws PathNotExistException {
    String[] nodes = path.getNodes();
    IMemMNode cur = storageGroupMNode;
    IMemMNode next;
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

  public IMeasurementMNode<IMemMNode> getMeasurementMNode(PartialPath path)
      throws MetadataException {
    IMemMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.MEASUREMENT_MNODE_TYPE);
    }
  }

  public long countAllMeasurement() throws MetadataException {
    try (MeasurementCounter<IMemMNode> measurementCounter =
        new MeasurementCounter<>(rootNode, MetadataConstant.ALL_MATCH_PATTERN, store, false)) {
      return measurementCounter.count();
    }
  }

  // endregion

  // region Interfaces and Implementation for Template check and query

  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    IMemMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    IDeviceMNode<IMemMNode> entityMNode;

    synchronized (this) {
      if (cur.isDevice()) {
        entityMNode = cur.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(cur);
        if (entityMNode.isDatabase()) {
          replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
        }
      }
    }

    if (entityMNode.isUseTemplate()) {
      if (template.getId() == entityMNode.getSchemaTemplateId()) {
        throw new TemplateIsInUseException(cur.getFullPath());
      } else {
        throw new DifferentTemplateException(activatePath.getFullPath(), template.getName());
      }
    }

    if (!entityMNode.isAligned()) {
      entityMNode.setAligned(template.isDirectAligned());
    }
    entityMNode.setUseTemplate(true);
    entityMNode.setSchemaTemplateId(template.getId());
    regionStatistics.activateTemplate(template.getId());
  }

  public Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<?> updater =
          new EntityUpdater<IMemMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.preDeactivateTemplate();
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<IMemMNode> updater =
          new EntityUpdater<IMemMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.rollbackPreDeactivateTemplate();
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<?> collector =
          new EntityUpdater<IMemMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                regionStatistics.deactivateTemplate(node.getSchemaTemplateId());
                node.deactivateTemplate();
                deleteEmptyInternalMNode(node);
              }
            }
          }) {
        collector.traverse();
      }
    }
    return resultTemplateSetInfo;
  }

  public void activateTemplateWithoutCheck(
      PartialPath activatePath, int templateId, boolean isAligned) {
    String[] nodes = activatePath.getNodes();
    IMemMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    IDeviceMNode<IMemMNode> entityMNode;
    if (cur.isDevice()) {
      entityMNode = cur.getAsDeviceMNode();
    } else {
      entityMNode = store.setToEntity(cur);
      if (entityMNode.isDatabase()) {
        replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
      }
    }

    if (!entityMNode.isAligned()) {
      entityMNode.setAligned(isAligned);
    }
    entityMNode.setUseTemplate(true);
    entityMNode.setSchemaTemplateId(templateId);
    regionStatistics.activateTemplate(templateId);
  }

  public long countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    try (EntityCounter<IMemMNode> counter =
        new EntityCounter<>(rootNode, pathPattern, store, false)) {
      counter.setSchemaTemplateFilter(templateId);
      return counter.count();
    }
  }

  // endregion

  // region Interfaces for schema reader

  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    EntityCollector<IDeviceSchemaInfo, IMemMNode> collector =
        new EntityCollector<IDeviceSchemaInfo, IMemMNode>(
            rootNode, showDevicesPlan.getPath(), store, showDevicesPlan.isPrefixMatch()) {

          protected IDeviceSchemaInfo collectEntity(IDeviceMNode<IMemMNode> node) {
            PartialPath device = getPartialPathFromRootToNode(node.getAsMNode());
            return new ShowDevicesResult(device.getFullPath(), node.isAligned());
          }
        };
    if (showDevicesPlan.usingSchemaTemplate()) {
      collector.setSchemaTemplateFilter(showDevicesPlan.getSchemaTemplateId());
    }
    TraverserWithLimitOffsetWrapper<IDeviceSchemaInfo, IMemMNode> traverser =
        new TraverserWithLimitOffsetWrapper<>(
            collector, showDevicesPlan.getLimit(), showDevicesPlan.getOffset());
    return new ISchemaReader<IDeviceSchemaInfo>() {

      public boolean isSuccess() {
        return traverser.isSuccess();
      }

      public Throwable getFailure() {
        return traverser.getFailure();
      }

      public void close() {
        traverser.close();
      }

      public boolean hasNext() {
        return traverser.hasNext();
      }

      public IDeviceSchemaInfo next() {
        return traverser.next();
      }
    };
  }

  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      IShowTimeSeriesPlan showTimeSeriesPlan,
      Function<Long, Pair<Map<String, String>, Map<String, String>>> tagAndAttributeProvider)
      throws MetadataException {
    MeasurementCollector<ITimeSeriesSchemaInfo, IMemMNode> collector =
        new MeasurementCollector<ITimeSeriesSchemaInfo, IMemMNode>(
            rootNode, showTimeSeriesPlan.getPath(), store, showTimeSeriesPlan.isPrefixMatch()) {

          protected ITimeSeriesSchemaInfo collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            return new ITimeSeriesSchemaInfo() {

              private Pair<Map<String, String>, Map<String, String>> tagAndAttribute = null;

              public String getAlias() {
                return node.getAlias();
              }

              public IMeasurementSchema getSchema() {
                return node.getSchema();
              }

              public Map<String, String> getTags() {
                if (tagAndAttribute == null) {
                  tagAndAttribute = tagAndAttributeProvider.apply(node.getOffset());
                }
                return tagAndAttribute.left;
              }

              public Map<String, String> getAttributes() {
                if (tagAndAttribute == null) {
                  tagAndAttribute = tagAndAttributeProvider.apply(node.getOffset());
                }
                return tagAndAttribute.right;
              }

              public boolean isUnderAlignedDevice() {
                return getParentOfNextMatchedNode().getAsDeviceMNode().isAligned();
              }

              @Override
              public boolean isLogicalView() {
                return node.isLogicalView();
              }

              public String getFullPath() {
                return getPartialPathFromRootToNode(node.getAsMNode()).getFullPath();
              }

              public PartialPath getPartialPath() {
                return getPartialPathFromRootToNode(node.getAsMNode());
              }
            };
          }
        };
    collector.setTemplateMap(showTimeSeriesPlan.getRelatedTemplate(), nodeFactory);
    Traverser<ITimeSeriesSchemaInfo, IMemMNode> traverser;
    if (showTimeSeriesPlan.getLimit() > 0 || showTimeSeriesPlan.getOffset() > 0) {
      traverser =
          new TraverserWithLimitOffsetWrapper<>(
              collector, showTimeSeriesPlan.getLimit(), showTimeSeriesPlan.getOffset());
    } else {
      traverser = collector;
    }
    return new ISchemaReader<ITimeSeriesSchemaInfo>() {

      public boolean isSuccess() {
        return traverser.isSuccess();
      }

      public Throwable getFailure() {
        return traverser.getFailure();
      }

      public void close() {
        traverser.close();
      }

      public boolean hasNext() {
        return traverser.hasNext();
      }

      public ITimeSeriesSchemaInfo next() {
        return traverser.next();
      }
    };
  }

  public ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException {
    MNodeCollector<INodeSchemaInfo, IMemMNode> collector =
        new MNodeCollector<INodeSchemaInfo, IMemMNode>(
            rootNode, showNodesPlan.getPath(), store, showNodesPlan.isPrefixMatch()) {

          protected INodeSchemaInfo collectMNode(IMemMNode node) {
            return new ShowNodesResult(
                getPartialPathFromRootToNode(node).getFullPath(), node.getMNodeType(false));
          }
        };
    collector.setTargetLevel(showNodesPlan.getLevel());

    return new ISchemaReader<INodeSchemaInfo>() {

      public boolean isSuccess() {
        return collector.isSuccess();
      }

      public Throwable getFailure() {
        return collector.getFailure();
      }

      public void close() {
        collector.close();
      }

      public boolean hasNext() {
        return collector.hasNext();
      }

      public INodeSchemaInfo next() {
        return collector.next();
      }
    };
  }
  // endregion

  // region interfaces for logical view
  public IMeasurementMNode<IMemMNode> createLogicalView(
      PartialPath path, ViewExpression viewExpression) throws MetadataException {
    // check path
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2) {
      throw new IllegalPathException(path.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(path);
    PartialPath devicePath = path.getDevicePath();
    IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    synchronized (this) {
      IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      String leafName = path.getMeasurement();

      // no need to check alias, because logical view has no alias

      if (device.hasChild(leafName)) {
        IMemMNode node = device.getChild(leafName);
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

      if (device.isDevice() && device.getAsDeviceMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "timeseries under this entity is aligned, can not create view under this entity.",
            device.getFullPath());
      }

      IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
        if (entityMNode.isDatabase()) {
          replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
        }
      }

      IMeasurementMNode<IMemMNode> measurementMNode =
          nodeFactory.createLogicalViewMNode(
              entityMNode,
              leafName,
              new LogicalViewInfo(new LogicalViewSchema(leafName, viewExpression)));

      store.addChild(entityMNode.getAsMNode(), leafName, measurementMNode.getAsMNode());

      return measurementMNode;
    }
  }
  // endregion
}
