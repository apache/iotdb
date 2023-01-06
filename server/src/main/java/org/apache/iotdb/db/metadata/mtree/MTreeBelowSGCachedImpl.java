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
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.TraverserWithLimitOffsetWrapper;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.updater.EntityUpdater;
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
 *       </ol>
 *   <li>Interfaces and Implementation for MNode Query
 *   <li>Interfaces and Implementation for Template check
 * </ol>
 */
public class MTreeBelowSGCachedImpl implements IMTreeBelowSG {

  private final CachedMTreeStore store;
  private volatile IStorageGroupMNode storageGroupMNode;
  private volatile IMNode rootNode;
  private final Function<IMeasurementMNode, Map<String, String>> tagGetter;
  private final int levelOfSG;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGCachedImpl(
      PartialPath storageGroupPath,
      Function<IMeasurementMNode, Map<String, String>> tagGetter,
      int schemaRegionId)
      throws MetadataException, IOException {
    this.tagGetter = tagGetter;
    store = new CachedMTreeStore(storageGroupPath, schemaRegionId);
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.rootNode = generatePrefix(storageGroupPath, this.storageGroupMNode);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
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

  /** Only used for load snapshot */
  private MTreeBelowSGCachedImpl(
      PartialPath storageGroupPath,
      CachedMTreeStore store,
      Consumer<IMeasurementMNode> measurementProcess,
      Function<IMeasurementMNode, Map<String, String>> tagGetter)
      throws MetadataException {
    this.store = store;
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.rootNode = generatePrefix(storageGroupPath, this.storageGroupMNode);
    levelOfSG = storageGroupMNode.getPartialPath().getNodeLength() - 1;
    this.tagGetter = tagGetter;

    // recover measurement
    try (MeasurementCollector<?> collector =
        new MeasurementCollector<Void>(
            this.storageGroupMNode,
            new PartialPath(storageGroupMNode.getFullPath()),
            this.store,
            true) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            measurementProcess.accept(node);
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

  protected void replaceStorageGroupMNode(IStorageGroupMNode newMNode) {
    this.storageGroupMNode.getParent().replaceChild(this.storageGroupMNode.getName(), newMNode);
    this.storageGroupMNode = newMNode;
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGCachedImpl loadFromSnapshot(
      File snapshotDir,
      String storageGroupFullPath,
      int schemaRegionId,
      Consumer<IMeasurementMNode> measurementProcess,
      Function<IMeasurementMNode, Map<String, String>> tagGetter)
      throws IOException, MetadataException {
    return new MTreeBelowSGCachedImpl(
        new PartialPath(storageGroupFullPath),
        CachedMTreeStore.loadFromSnapshot(snapshotDir, storageGroupFullPath, schemaRegionId),
        measurementProcess,
        tagGetter);
  }

  // endregion

  // region Timeseries operation, including create and delete

  @Override
  public IMeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    IMeasurementMNode measurementMNode =
        createTimeseriesWithPinnedReturn(path, dataType, encoding, compressor, props, alias);
    unPinMNode(measurementMNode);
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
  public IMeasurementMNode createTimeseriesWithPinnedReturn(
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

    try {
      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        IMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

        try {
          MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

          String leafName = path.getMeasurement();

          if (alias != null && store.hasChild(device, alias)) {
            throw new AliasAlreadyExistException(path.getFullPath(), alias);
          }

          if (store.hasChild(device, leafName)) {
            throw new PathAlreadyExistException(path.getFullPath());
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

    try {
      // synchronize check and add, we need addChild operation be atomic.
      // only write operations on mtree will be synchronized
      synchronized (this) {
        IMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

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
            device = entityMNode;
          }

          for (int i = 0; i < measurements.size(); i++) {
            IMeasurementMNode measurementMNode =
                MeasurementMNode.getMeasurementMNode(
                    entityMNode,
                    measurements.get(i),
                    new MeasurementSchema(
                        measurements.get(i),
                        dataTypes.get(i),
                        encodings.get(i),
                        compressors.get(i)),
                    aliasList == null ? null : aliasList.get(i));
            store.addChild(entityMNode, measurements.get(i), measurementMNode);
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
    IMNode device = null;
    try {
      device = getNodeByPath(devicePath);
    } catch (MetadataException e) {
      return Collections.emptyMap();
    }
    try {
      if (!device.isEntity()) {
        return Collections.emptyMap();
      }
      Map<Integer, MetadataException> failingMeasurementMap = new HashMap<>();
      for (int i = 0; i < measurementList.size(); i++) {
        IMNode node = null;
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

  private IMNode checkAndAutoCreateInternalPath(PartialPath devicePath) throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return null;
    }
    IMNode cur = storageGroupMNode;
    IMNode child;
    String childName;
    try {
      // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
      for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
        childName = nodeNames[i];
        child = store.getChild(cur, childName);
        if (child == null) {
          child = store.addChild(cur, childName, new InternalMNode(cur, childName));
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

  private IMNode checkAndAutoCreateDeviceNode(String deviceName, IMNode deviceParent)
      throws MetadataException {
    if (deviceParent == null) {
      // device is sg
      pinMNode(storageGroupMNode);
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

  /**
   * Used when delete timeseries or deactivate template
   *
   * @param entityMNode delete empty InternalMNode from entityMNode to storageGroupMNode
   * @return After delete if MTree is empty, return SG path, otherwise return null
   */
  private PartialPath deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(IEntityMNode entityMNode)
      throws MetadataException {
    IMNode curNode = entityMNode;
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      IMNode child;
      IMNodeIterator iterator = store.getChildrenIterator(entityMNode);
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
    unPinMNode(curNode);
    return null;
  }

  @Override
  public boolean isEmptyInternalMNode(IMNode node) throws MetadataException {
    IMNodeIterator iterator = store.getChildrenIterator(node);
    try {
      return !IoTDBConstant.PATH_ROOT.equals(node.getName())
          && !node.isMeasurement()
          && !node.isUseTemplate()
          && !iterator.hasNext();
    } finally {
      iterator.close();
    }
  }

  @Override
  public List<PartialPath> constructSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater updater =
        new MeasurementUpdater(rootNode, pathPattern, store, false) {
          @Override
          protected void updateMeasurement(IMeasurementMNode node) throws MetadataException {
            node.setPreDeleted(true);
            updateMNode(node);
            result.add(getNextMatchedNodePartialPath());
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
          protected void updateMeasurement(IMeasurementMNode node) throws MetadataException {
            node.setPreDeleted(false);
            updateMNode(node);
            result.add(getNextMatchedNodePartialPath());
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
              result.add(getNextMatchedNodePartialPath());
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
              result.add(getNextMatchedNodePartialPath().getDevicePath());
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
    String[] nodeNames = deviceId.getNodes();
    MetaFormatUtils.checkTimeseries(deviceId);
    IMNode cur = storageGroupMNode;
    IMNode child;
    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          child = store.addChild(cur, nodeNames[i], new InternalMNode(cur, nodeNames[i]));
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

  // region Interfaces for Device info Query

  @Override
  public List<ShowDevicesResult> getDevices(IShowDevicesPlan plan) throws MetadataException {
    List<ShowDevicesResult> res = new ArrayList<>();
    try (EntityCollector<List<ShowDevicesResult>> collector =
        new EntityCollector<List<ShowDevicesResult>>(
            rootNode, plan.getPath(), store, plan.isPrefixMatch()) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            PartialPath device = getNextMatchedNodePartialPath();
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
                    getNextMatchedNodePartialPath().getFullPath(),
                    node.getAlias(),
                    (MeasurementSchema) node.getSchema(),
                    tagAndAttribute.left,
                    tagAndAttribute.right,
                    getParentOfNextMatchedNode().getAsEntityMNode().isAligned()));
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
                    getNextMatchedNodePartialPath().getFullPath(),
                    node.getMNodeType(false).getNodeType()));
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
            result.add(getNextMatchedNodePartialPath());
          }
        }) {
      collector.setTargetLevel(nodeLevel);
      collector.traverse();
    }
    return result;
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
  public IMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode next;
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

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(rootNode, pathPattern, store, false) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            if (node.isPreDeleted()) {
              return;
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
          }
        }) {
      collector.setTemplateMap(templateMap);
      collector.traverse();
    }
    return result;
  }

  @Override
  public List<IMeasurementMNode> getAllMeasurementMNode() throws MetadataException {
    IMNode cur = storageGroupMNode;
    // collect all the LeafMNode in this database
    List<IMeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<IMNode> queue = new LinkedList<>();
    try {
      pinMNode(cur);
      queue.add(cur);
      while (!queue.isEmpty()) {
        IMNode node = queue.poll();
        try {
          IMNodeIterator iterator = store.getChildrenIterator(node);
          try {
            IMNode child;
            while (iterator.hasNext()) {
              child = iterator.next();
              if (child.isMeasurement()) {
                leafMNodes.add(child.getAsMeasurementMNode());
                unPinMNode(child);
              } else {
                queue.add(child);
              }
            }
          } finally {
            iterator.close();
          }
        } finally {
          unPinMNode(node);
        }
      }
      return leafMNodes;
    } finally {
      while (!queue.isEmpty()) {
        unPinMNode(queue.poll());
      }
    }
  }
  // endregion

  // region Interfaces and Implementation for Template check and query

  @Override
  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;
    IEntityMNode entityMNode;

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

      store.updateMNode(entityMNode);
    } finally {
      unPinPath(cur);
    }
  }

  public Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater updater =
          new EntityUpdater(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.preDeactivateTemplate();
                store.updateMNode(node);
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
      try (EntityUpdater updater =
          new EntityUpdater(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.rollbackPreDeactivateTemplate();
                store.updateMNode(node);
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
      try (EntityUpdater collector =
          new EntityUpdater(rootNode, entry.getKey(), store, false) {
            @Override
            protected void updateEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.deactivateTemplate();
                store.updateMNode(node);
                deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(node);
              }
            }
          }) {
        collector.traverse();
      }
    }
    return resultTemplateSetInfo;
  }

  @Override
  public long countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    // TODO: replace fake counter
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

  // region Interfaces and Implementation for Pin/UnPin MNode or Path

  /**
   * Currently, this method is only used for pin node get from mNodeCache
   *
   * @param node
   */
  public void pinMNode(IMNode node) throws MetadataException {
    store.pin(node);
  }

  public void unPinMNode(IMNode node) {
    store.unPin(node);
  }

  private void unPinPath(IMNode node) {
    store.unPinPath(node);
  }

  public void updateMNode(IMNode node) throws MetadataException {
    store.updateMNode(node);
  }

  // endregion

}
