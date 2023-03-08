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
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.template.DifferentTemplateException;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;
import org.apache.iotdb.db.metadata.mtree.traverser.TraverserWithLimitOffsetWrapper;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.EntityUpdater;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.MeasurementUpdater;
import org.apache.iotdb.db.metadata.newnode.ICacheMNode;
import org.apache.iotdb.db.metadata.newnode.basic.CacheBasicMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.factory.IMNodeFactory;
import org.apache.iotdb.db.metadata.newnode.measurement.CacheMeasurementMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowDevicesResult;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowNodesResult;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;
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
public class MTreeBelowSGCachedImpl implements IMTreeBelowSG<ICacheMNode> {

  private final CachedMTreeStore store;
  private volatile ICacheMNode storageGroupMNode;
  private final ICacheMNode rootNode;
  private final Function<IMeasurementMNode<ICacheMNode>, Map<String, String>> tagGetter;
  private final IMNodeFactory<ICacheMNode> nodeFactory;
  private final int levelOfSG;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGCachedImpl(
      PartialPath storageGroupPath,
      Function<IMeasurementMNode<ICacheMNode>, Map<String, String>> tagGetter,
      Runnable flushCallback,
      Consumer<IMeasurementMNode<ICacheMNode>> measurementProcess,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      IMNodeFactory<ICacheMNode> nodeFactory)
      throws MetadataException, IOException {
    this.tagGetter = tagGetter;
    this.nodeFactory = nodeFactory;
    store =
        new CachedMTreeStore(
            storageGroupPath, schemaRegionId, regionStatistics, flushCallback, nodeFactory);
    this.storageGroupMNode = store.getRoot();
    this.storageGroupMNode.setParent(storageGroupMNode.getParent());
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;

    // recover measurement
    try (MeasurementCollector<Void, ICacheMNode> collector =
        new MeasurementCollector<Void, ICacheMNode>(
            this.rootNode, new PartialPath(storageGroupMNode.getFullPath()), this.store, true) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
            measurementProcess.accept(node);
            regionStatistics.addTimeseries(1L);
            return null;
          }
        }) {
      collector.traverse();
    }
  }

  /** Only used for load snapshot */
  private MTreeBelowSGCachedImpl(
      PartialPath storageGroupPath,
      CachedMTreeStore store,
      Consumer<IMeasurementMNode<ICacheMNode>> measurementProcess,
      Function<IMeasurementMNode<ICacheMNode>, Map<String, String>> tagGetter,
      IMNodeFactory<ICacheMNode> nodeFactory)
      throws MetadataException {
    this.store = store;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupMNode.getPartialPath().getNodeLength() - 1;
    this.tagGetter = tagGetter;
    this.nodeFactory = nodeFactory;

    // recover measurement
    try (MeasurementCollector<Void, ICacheMNode> collector =
        new MeasurementCollector<Void, ICacheMNode>(
            this.rootNode, new PartialPath(storageGroupMNode.getFullPath()), this.store, true) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
            measurementProcess.accept(node);
            return null;
          }
        }) {
      collector.traverse();
    }
  }

  @Override
  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  protected void replaceStorageGroupMNode(IDatabaseMNode<ICacheMNode> newMNode) {
    this.storageGroupMNode
        .getParent()
        .replaceChild(this.storageGroupMNode.getName(), newMNode.getAsMNode());
    this.storageGroupMNode = newMNode.getAsMNode();
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGCachedImpl loadFromSnapshot(
      File snapshotDir,
      String storageGroupFullPath,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Consumer<IMeasurementMNode<ICacheMNode>> measurementProcess,
      Function<IMeasurementMNode<ICacheMNode>, Map<String, String>> tagGetter,
      Runnable flushCallback,
      IMNodeFactory<ICacheMNode> nodeFactory)
      throws IOException, MetadataException {
    return new MTreeBelowSGCachedImpl(
        new PartialPath(storageGroupFullPath),
        CachedMTreeStore.loadFromSnapshot(
            snapshotDir,
            storageGroupFullPath,
            schemaRegionId,
            regionStatistics,
            flushCallback,
            nodeFactory),
        measurementProcess,
        tagGetter,
        nodeFactory);
  }

  // endregion

  // region Timeseries operation, including create and delete

  @Override
  public IMeasurementMNode<ICacheMNode> createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    IMeasurementMNode<ICacheMNode> measurementMNode =
        createTimeseriesWithPinnedReturn(path, dataType, encoding, compressor, props, alias);
    unPinMNode(measurementMNode.getAsMNode());
    return measurementMNode;
  }

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
  public IMeasurementMNode<ICacheMNode> createTimeseriesWithPinnedReturn(
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
    ICacheMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    try {
      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        ICacheMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

        try {
          MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

          String leafName = path.getMeasurement();

          if (alias != null && store.hasChild(device, alias)) {
            throw new AliasAlreadyExistException(path.getFullPath(), alias);
          }

          if (store.hasChild(device, leafName)) {
            throw new PathAlreadyExistException(path.getFullPath());
          }

          if (device.isDevice() && device.getAsDeviceMNode().isAligned()) {
            throw new AlignedTimeseriesException(
                "timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
                device.getFullPath());
          }

          IDeviceMNode<ICacheMNode> entityMNode;
          if (device.isDevice()) {
            entityMNode = device.getAsDeviceMNode();
          } else {
            entityMNode = store.setToEntity(device);
            if (entityMNode.isDatabase()) {
              replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
            }
            device = entityMNode.getAsMNode();
          }

          IMeasurementMNode<ICacheMNode> measurementMNode =
              new CacheMeasurementMNode(
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
        } finally {
          unPinMNode(device);
        }
      }
    } finally {
      if (deviceParent != null) {
        unPinMNode(deviceParent);
      }
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
  public List<IMeasurementMNode<ICacheMNode>> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException {
    List<IMeasurementMNode<ICacheMNode>> measurementMNodeList = new ArrayList<>();
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    ICacheMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    try {
      // synchronize check and add, we need addChild operation be atomic.
      // only write operations on mtree will be synchronized
      synchronized (this) {
        ICacheMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

        try {
          for (int i = 0; i < measurements.size(); i++) {
            if (store.hasChild(device, measurements.get(i))) {
              throw new PathAlreadyExistException(
                  devicePath.getFullPath() + "." + measurements.get(i));
            }
            if (aliasList != null
                && aliasList.get(i) != null
                && store.hasChild(device, aliasList.get(i))) {
              throw new AliasAlreadyExistException(
                  devicePath.getFullPath() + "." + measurements.get(i), aliasList.get(i));
            }
          }

          if (device.isDevice() && !device.getAsDeviceMNode().isAligned()) {
            throw new AlignedTimeseriesException(
                "Timeseries under this entity is not aligned, please use createTimeseries or change entity.",
                devicePath.getFullPath());
          }

          IDeviceMNode<ICacheMNode> entityMNode;
          if (device.isDevice()) {
            entityMNode = device.getAsDeviceMNode();
          } else {
            entityMNode = store.setToEntity(device);
            entityMNode.setAligned(true);
            if (entityMNode.isDatabase()) {
              replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
            }
            device = entityMNode.getAsMNode();
          }

          for (int i = 0; i < measurements.size(); i++) {
            IMeasurementMNode<ICacheMNode> measurementMNode =
                new CacheMeasurementMNode(
                    entityMNode,
                    measurements.get(i),
                    new MeasurementSchema(
                        measurements.get(i),
                        dataTypes.get(i),
                        encodings.get(i),
                        compressors.get(i)),
                    aliasList == null ? null : aliasList.get(i));
            store.addChild(
                entityMNode.getAsMNode(), measurements.get(i), measurementMNode.getAsMNode());
            if (aliasList != null && aliasList.get(i) != null) {
              entityMNode.addAlias(aliasList.get(i), measurementMNode);
            }
            measurementMNodeList.add(measurementMNode);
          }
          return measurementMNodeList;
        } finally {
          unPinMNode(device);
        }
      }
    } finally {
      if (deviceParent != null) {
        unPinMNode(deviceParent);
      }
    }
  }

  @Override
  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    ICacheMNode device;
    try {
      device = getNodeByPath(devicePath);
    } catch (MetadataException e) {
      return Collections.emptyMap();
    }
    try {
      if (!device.isDevice()) {
        return Collections.emptyMap();
      }
      Map<Integer, MetadataException> failingMeasurementMap = new HashMap<>();
      for (int i = 0; i < measurementList.size(); i++) {
        ICacheMNode node = null;
        try {
          node = store.getChild(device, measurementList.get(i));
          if (node != null) {
            if (node.isMeasurement()) {
              if (node.getAsMeasurementMNode().isPreDeleted()) {
                failingMeasurementMap.put(
                    i,
                    new MeasurementInBlackListException(
                        devicePath.concatNode(measurementList.get(i))));
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
          if (aliasList != null
              && aliasList.get(i) != null
              && store.hasChild(device, aliasList.get(i))) {
            failingMeasurementMap.put(
                i,
                new AliasAlreadyExistException(
                    devicePath.getFullPath() + "." + measurementList.get(i), aliasList.get(i)));
          }
        } catch (MetadataException e) {
          failingMeasurementMap.put(i, e);
        } finally {
          if (node != null) {
            unPinMNode(node);
          }
        }
      }
      return failingMeasurementMap;
    } finally {
      unPinMNode(device);
    }
  }

  private ICacheMNode checkAndAutoCreateInternalPath(PartialPath devicePath)
      throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return null;
    }
    ICacheMNode cur = storageGroupMNode;
    ICacheMNode child;
    String childName;
    try {
      // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
      for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
        childName = nodeNames[i];
        child = store.getChild(cur, childName);
        if (child == null) {
          child = store.addChild(cur, childName, new CacheBasicMNode(cur, childName));
        }
        cur = child;

        if (cur.isMeasurement()) {
          throw new PathAlreadyExistException(cur.getFullPath());
        }
      }
      pinMNode(cur);
      return cur;
    } finally {
      unPinPath(cur);
    }
  }

  private ICacheMNode checkAndAutoCreateDeviceNode(String deviceName, ICacheMNode deviceParent)
      throws MetadataException {
    if (deviceParent == null) {
      // device is sg
      pinMNode(storageGroupMNode);
      return storageGroupMNode;
    }
    ICacheMNode device = store.getChild(deviceParent, deviceName);
    if (device == null) {
      device =
          store.addChild(deviceParent, deviceName, new CacheBasicMNode(deviceParent, deviceName));
    }

    if (device.isMeasurement()) {
      throw new PathAlreadyExistException(device.getFullPath());
    }
    return device;
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  @Override
  public IMeasurementMNode<ICacheMNode> deleteTimeseries(PartialPath path)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMeasurementMNode<ICacheMNode> deletedNode = getMeasurementMNode(path);
    ICacheMNode parent = deletedNode.getParent();
    // delete the last node of path
    store.deleteChild(parent, path.getMeasurement());
    if (deletedNode.getAlias() != null) {
      parent.getAsDeviceMNode().deleteAliasChild(deletedNode.getAlias());
    }
    deleteAndUnpinEmptyInternalMNode(parent.getAsDeviceMNode());
    return deletedNode;
  }

  /**
   * Used when delete timeseries or deactivate template. The last survived ancestor will be
   * unpinned.
   *
   * @param entityMNode delete empty InternalMNode from entityMNode to storageGroupMNode
   */
  private void deleteAndUnpinEmptyInternalMNode(IDeviceMNode<ICacheMNode> entityMNode)
      throws MetadataException {
    ICacheMNode curNode = entityMNode.getAsMNode();
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      ICacheMNode child;
      IMNodeIterator<ICacheMNode> iterator = store.getChildrenIterator(curNode);
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
    unPinMNode(curNode);
  }

  private boolean isEmptyInternalMNode(ICacheMNode node) throws MetadataException {
    IMNodeIterator<ICacheMNode> iterator = store.getChildrenIterator(node);
    try {
      return !IoTDBConstant.PATH_ROOT.equals(node.getName())
          && !node.isMeasurement()
          && !(node.isDevice() && node.getAsDeviceMNode().isUseTemplate())
          && !iterator.hasNext();
    } finally {
      iterator.close();
    }
  }

  @Override
  public List<PartialPath> constructSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater<ICacheMNode> updater =
        new MeasurementUpdater<ICacheMNode>(rootNode, pathPattern, store, false) {
          @Override
          protected void updateMeasurement(IMeasurementMNode<ICacheMNode> node)
              throws MetadataException {
            node.setPreDeleted(true);
            store.updateMNode(node.getAsMNode());
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
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
    try (MeasurementUpdater<ICacheMNode> updater =
        new MeasurementUpdater<ICacheMNode>(rootNode, pathPattern, store, false) {
          @Override
          protected void updateMeasurement(IMeasurementMNode<ICacheMNode> node)
              throws MetadataException {
            node.setPreDeleted(false);
            store.updateMNode(node.getAsMNode());
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
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
    try (MeasurementCollector<Void, ICacheMNode> collector =
        new MeasurementCollector<Void, ICacheMNode>(rootNode, pathPattern, store, false) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
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

  @Override
  public Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    try (MeasurementCollector<Void, ICacheMNode> collector =
        new MeasurementCollector<Void, ICacheMNode>(rootNode, pathPattern, store, false) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
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

  @Override
  public void setAlias(IMeasurementMNode<ICacheMNode> measurementMNode, String alias)
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
  @Override
  public ICacheMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException {
    String[] nodeNames = deviceId.getNodes();
    MetaFormatUtils.checkTimeseries(deviceId);
    ICacheMNode cur = storageGroupMNode;
    ICacheMNode child;
    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          child = store.addChild(cur, nodeNames[i], new CacheBasicMNode(cur, nodeNames[i]));
        }
        cur = child;
      }
      pinMNode(cur);
      return cur;
    } finally {
      unPinPath(cur);
    }
  }
  // endregion

  // region Interfaces and Implementation for metadata info Query

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<Void, ICacheMNode> collector =
        new MeasurementCollector<Void, ICacheMNode>(rootNode, pathPattern, store, false) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
            if (node.isPreDeleted()) {
              return null;
            }
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
  @Override
  public ICacheMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    ICacheMNode cur = storageGroupMNode;
    ICacheMNode next;
    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
        next = store.getChild(cur, nodes[i]);
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
      pinMNode(cur);
      return cur;
    } finally {
      unPinPath(cur);
    }
  }

  @Override
  public IMeasurementMNode<ICacheMNode> getMeasurementMNode(PartialPath path)
      throws MetadataException {
    ICacheMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      unPinMNode(node);
      throw new MNodeTypeMismatchException(
          path.getFullPath(), MetadataConstant.MEASUREMENT_MNODE_TYPE);
    }
  }

  @Override
  public long countAllMeasurement() throws MetadataException {
    try (MeasurementCounter<ICacheMNode> measurementCounter =
        new MeasurementCounter<>(rootNode, MetadataConstant.ALL_MATCH_PATTERN, store, false)) {
      return measurementCounter.count();
    }
  }
  // endregion

  // region Interfaces and Implementation for Template check and query

  @Override
  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    ICacheMNode cur = storageGroupMNode;
    ICacheMNode child;
    IDeviceMNode<ICacheMNode> entityMNode;

    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
        child = store.getChild(cur, nodes[i]);
        if (child == null) {
          throw new PathNotExistException(activatePath.getFullPath());
        }
        cur = child;
      }
      synchronized (this) {
        for (String measurement : template.getSchemaMap().keySet()) {
          if (store.hasChild(cur, measurement)) {
            throw new TemplateImcompatibeException(
                activatePath.concatNode(measurement).getFullPath(), template.getName());
          }
        }

        if (cur.isDevice()) {
          entityMNode = cur.getAsDeviceMNode();
        } else {
          entityMNode = store.setToEntity(cur);
          if (entityMNode.isDatabase()) {
            replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
          }
        }

        if (entityMNode.isUseTemplate()) {
          if (template.getId() == entityMNode.getSchemaTemplateId()) {
            throw new TemplateIsInUseException(cur.getFullPath());
          } else {
            throw new DifferentTemplateException(activatePath.getFullPath(), template.getName());
          }
        }
      }

      if (!entityMNode.isAligned()) {
        entityMNode.setAligned(template.isDirectAligned());
      }
      entityMNode.setUseTemplate(true);
      entityMNode.setSchemaTemplateId(template.getId());

      store.updateMNode(entityMNode.getAsMNode());
    } finally {
      unPinPath(cur);
    }
  }

  @Override
  public Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICacheMNode> updater =
          new EntityUpdater<ICacheMNode>(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IDeviceMNode<ICacheMNode> node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.preDeactivateTemplate();
                store.updateMNode(node.getAsMNode());
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  @Override
  public Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICacheMNode> updater =
          new EntityUpdater<ICacheMNode>(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IDeviceMNode<ICacheMNode> node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.rollbackPreDeactivateTemplate();
                store.updateMNode(node.getAsMNode());
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  @Override
  public Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICacheMNode> collector =
          new EntityUpdater<ICacheMNode>(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IDeviceMNode<ICacheMNode> node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.deactivateTemplate();
                store.updateMNode(node.getAsMNode());
              }
            }
          }) {
        collector.traverse();
      }
    }
    for (PartialPath path : resultTemplateSetInfo.keySet()) {
      deleteAndUnpinEmptyInternalMNode(getNodeByPath(path).getAsDeviceMNode());
    }
    return resultTemplateSetInfo;
  }

  @Override
  public long countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    try (EntityCounter<ICacheMNode> counter =
        new EntityCounter(rootNode, pathPattern, store, false)) {
      counter.setSchemaTemplateFilter(templateId);
      return counter.count();
    }
  }

  // endregion

  // region Interfaces and Implementation for Pin/UnPin MNode or Path

  /**
   * Currently, this method is only used for pin node get from mNodeCache
   *
   * @param node
   */
  // TODO: This interface should not be exposed to SchemaRegion
  public void pinMNode(ICacheMNode node) throws MetadataException {
    store.pin(node);
  }

  // TODO: This interface should not be exposed to SchemaRegion
  public void unPinMNode(ICacheMNode node) {
    store.unPin(node);
  }

  private void unPinPath(ICacheMNode node) {
    store.unPinPath(node);
  }

  // TODO: This interface should not be exposed to SchemaRegion
  public void updateMNode(ICacheMNode node) throws MetadataException {
    store.updateMNode(node);
  }

  // endregion

  // region Interfaces for schema reader
  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    EntityCollector<IDeviceSchemaInfo, ICacheMNode> collector =
        new EntityCollector<IDeviceSchemaInfo, ICacheMNode>(
            rootNode, showDevicesPlan.getPath(), store, showDevicesPlan.isPrefixMatch()) {
          @Override
          protected IDeviceSchemaInfo collectEntity(IDeviceMNode<ICacheMNode> node) {
            PartialPath device = getPartialPathFromRootToNode(node.getAsMNode());
            return new ShowDevicesResult(device.getFullPath(), node.isAligned());
          }
        };
    if (showDevicesPlan.usingSchemaTemplate()) {
      collector.setSchemaTemplateFilter(showDevicesPlan.getSchemaTemplateId());
    }
    TraverserWithLimitOffsetWrapper<IDeviceSchemaInfo, ICacheMNode> traverser =
        new TraverserWithLimitOffsetWrapper<>(
            collector, showDevicesPlan.getLimit(), showDevicesPlan.getOffset());
    return new ISchemaReader<IDeviceSchemaInfo>() {
      @Override
      public boolean isSuccess() {
        return traverser.isSuccess();
      }

      @Override
      public Throwable getFailure() {
        return traverser.getFailure();
      }

      @Override
      public void close() {
        traverser.close();
      }

      @Override
      public boolean hasNext() {
        return traverser.hasNext();
      }

      @Override
      public IDeviceSchemaInfo next() {
        return traverser.next();
      }
    };
  }

  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      IShowTimeSeriesPlan showTimeSeriesPlan,
      Function<Long, Pair<Map<String, String>, Map<String, String>>> tagAndAttributeProvider)
      throws MetadataException {
    MeasurementCollector<ITimeSeriesSchemaInfo, ICacheMNode> collector =
        new MeasurementCollector<ITimeSeriesSchemaInfo, ICacheMNode>(
            rootNode, showTimeSeriesPlan.getPath(), store, showTimeSeriesPlan.isPrefixMatch()) {
          @Override
          protected ITimeSeriesSchemaInfo collectMeasurement(IMeasurementMNode<ICacheMNode> node) {
            return new ITimeSeriesSchemaInfo() {

              private Pair<Map<String, String>, Map<String, String>> tagAndAttribute = null;

              @Override
              public String getAlias() {
                return node.getAlias();
              }

              @Override
              public MeasurementSchema getSchema() {
                return (MeasurementSchema) node.getSchema();
              }

              @Override
              public Map<String, String> getTags() {
                if (tagAndAttribute == null) {
                  tagAndAttribute = tagAndAttributeProvider.apply(node.getOffset());
                }
                return tagAndAttribute.left;
              }

              @Override
              public Map<String, String> getAttributes() {
                if (tagAndAttribute == null) {
                  tagAndAttribute = tagAndAttributeProvider.apply(node.getOffset());
                }
                return tagAndAttribute.right;
              }

              @Override
              public boolean isUnderAlignedDevice() {
                return getParentOfNextMatchedNode().getAsDeviceMNode().isAligned();
              }

              @Override
              public String getFullPath() {
                return getPartialPathFromRootToNode(node.getAsMNode()).getFullPath();
              }

              @Override
              public PartialPath getPartialPath() {
                return getPartialPathFromRootToNode(node.getAsMNode());
              }
            };
          }
        };

    collector.setTemplateMap(showTimeSeriesPlan.getRelatedTemplate(), nodeFactory);
    Traverser<ITimeSeriesSchemaInfo, ICacheMNode> traverser;
    if (showTimeSeriesPlan.getLimit() > 0 || showTimeSeriesPlan.getOffset() > 0) {
      traverser =
          new TraverserWithLimitOffsetWrapper<>(
              collector, showTimeSeriesPlan.getLimit(), showTimeSeriesPlan.getOffset());
    } else {
      traverser = collector;
    }
    return new ISchemaReader<ITimeSeriesSchemaInfo>() {
      @Override
      public boolean isSuccess() {
        return traverser.isSuccess();
      }

      @Override
      public Throwable getFailure() {
        return traverser.getFailure();
      }

      @Override
      public void close() {
        traverser.close();
      }

      @Override
      public boolean hasNext() {
        return traverser.hasNext();
      }

      @Override
      public ITimeSeriesSchemaInfo next() {
        return traverser.next();
      }
    };
  }

  public ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException {
    MNodeCollector<INodeSchemaInfo, ICacheMNode> collector =
        new MNodeCollector<INodeSchemaInfo, ICacheMNode>(
            rootNode, showNodesPlan.getPath(), store, showNodesPlan.isPrefixMatch()) {
          @Override
          protected INodeSchemaInfo collectMNode(ICacheMNode node) {
            return new ShowNodesResult(
                getPartialPathFromRootToNode(node).getFullPath(), node.getMNodeType(false));
          }
        };
    collector.setTargetLevel(showNodesPlan.getLevel());
    return new ISchemaReader<INodeSchemaInfo>() {
      @Override
      public boolean isSuccess() {
        return collector.isSuccess();
      }

      @Override
      public Throwable getFailure() {
        return collector.getFailure();
      }

      @Override
      public void close() {
        collector.close();
      }

      @Override
      public boolean hasNext() {
        return collector.hasNext();
      }

      @Override
      public INodeSchemaInfo next() {
        return collector.next();
      }
    };
  }
  // endregion
}
