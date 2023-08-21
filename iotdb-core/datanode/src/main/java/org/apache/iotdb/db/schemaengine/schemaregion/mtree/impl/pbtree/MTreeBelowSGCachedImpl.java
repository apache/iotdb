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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.template.DifferentTemplateException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.updater.EntityUpdater;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.updater.MeasurementUpdater;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowDevicesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowNodesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowNodesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.TimeseriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.impl.SchemaReaderLimitOffsetWrapper;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.impl.TimeseriesReaderWithViewFetch;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaFormatUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.filter.DeviceFilterVisitor;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
public class MTreeBelowSGCachedImpl {

  private static final Logger logger = LoggerFactory.getLogger(MTreeBelowSGCachedImpl.class);

  private final CachedMTreeStore store;

  @SuppressWarnings("java:S3077")
  private volatile ICachedMNode storageGroupMNode;

  private final ICachedMNode rootNode;
  private final Function<IMeasurementMNode<ICachedMNode>, Map<String, String>> tagGetter;
  private final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
  private final int levelOfSG;
  private final CachedSchemaRegionStatistics regionStatistics;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGCachedImpl(
      PartialPath storageGroupPath,
      Function<IMeasurementMNode<ICachedMNode>, Map<String, String>> tagGetter,
      Runnable flushCallback,
      Consumer<IMeasurementMNode<ICachedMNode>> measurementProcess,
      Consumer<IDeviceMNode<ICachedMNode>> deviceProcess,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics)
      throws MetadataException, IOException {
    this.tagGetter = tagGetter;
    this.regionStatistics = regionStatistics;
    store = new CachedMTreeStore(storageGroupPath, schemaRegionId, regionStatistics, flushCallback);
    this.storageGroupMNode = store.getRoot();
    this.storageGroupMNode.setParent(storageGroupMNode.getParent());
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;

    // recover MNode
    try (MNodeCollector<Void, ICachedMNode> collector =
        new MNodeCollector<Void, ICachedMNode>(
            this.rootNode, new PartialPath(storageGroupMNode.getFullPath()), this.store, true) {
          @Override
          protected Void collectMNode(ICachedMNode node) {
            if (node.isMeasurement()) {
              measurementProcess.accept(node.getAsMeasurementMNode());
            } else if (node.isDevice()) {
              deviceProcess.accept(node.getAsDeviceMNode());
            }
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
      Consumer<IMeasurementMNode<ICachedMNode>> measurementProcess,
      Consumer<IDeviceMNode<ICachedMNode>> deviceProcess,
      Function<IMeasurementMNode<ICachedMNode>, Map<String, String>> tagGetter,
      CachedSchemaRegionStatistics regionStatistics)
      throws MetadataException {
    this.store = store;
    this.regionStatistics = regionStatistics;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupMNode.getPartialPath().getNodeLength() - 1;
    this.tagGetter = tagGetter;

    // recover MNode
    try (MNodeCollector<Void, ICachedMNode> collector =
        new MNodeCollector<Void, ICachedMNode>(
            this.rootNode, new PartialPath(storageGroupMNode.getFullPath()), this.store, true) {
          @Override
          protected Void collectMNode(ICachedMNode node) {
            if (node.isMeasurement()) {
              measurementProcess.accept(node.getAsMeasurementMNode());
            } else if (node.isDevice()) {
              deviceProcess.accept(node.getAsDeviceMNode());
            }
            return null;
          }
        }) {
      collector.traverse();
    }
  }

  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  protected void replaceStorageGroupMNode(IDatabaseMNode<ICachedMNode> newMNode) {
    this.storageGroupMNode
        .getParent()
        .replaceChild(this.storageGroupMNode.getName(), newMNode.getAsMNode());
    this.storageGroupMNode = newMNode.getAsMNode();
  }

  public boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGCachedImpl loadFromSnapshot(
      File snapshotDir,
      String storageGroupFullPath,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      Consumer<IMeasurementMNode<ICachedMNode>> measurementProcess,
      Consumer<IDeviceMNode<ICachedMNode>> deviceProcess,
      Function<IMeasurementMNode<ICachedMNode>, Map<String, String>> tagGetter,
      Runnable flushCallback)
      throws IOException, MetadataException {
    return new MTreeBelowSGCachedImpl(
        new PartialPath(storageGroupFullPath),
        CachedMTreeStore.loadFromSnapshot(
            snapshotDir, storageGroupFullPath, schemaRegionId, regionStatistics, flushCallback),
        measurementProcess,
        deviceProcess,
        tagGetter,
        regionStatistics);
  }

  // endregion

  // region Timeseries operation, including create and delete

  public IMeasurementMNode<ICachedMNode> createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    IMeasurementMNode<ICachedMNode> measurementMNode =
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
  public IMeasurementMNode<ICachedMNode> createTimeseriesWithPinnedReturn(
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
    ICachedMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    try {
      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        ICachedMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

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
                "timeseries under this device is aligned, please use createAlignedTimeseries or change device.",
                device.getFullPath());
          }

          IDeviceMNode<ICachedMNode> entityMNode;
          if (device.isDevice()) {
            entityMNode = device.getAsDeviceMNode();
          } else {
            entityMNode = store.setToEntity(device);
            if (entityMNode.isDatabase()) {
              replaceStorageGroupMNode(entityMNode.getAsDatabaseMNode());
            }
            device = entityMNode.getAsMNode();
          }

          IMeasurementMNode<ICachedMNode> measurementMNode =
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
  public List<IMeasurementMNode<ICachedMNode>> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException {
    List<IMeasurementMNode<ICachedMNode>> measurementMNodeList = new ArrayList<>();
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    ICachedMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    try {
      // synchronize check and add, we need addChild operation be atomic.
      // only write operations on mtree will be synchronized
      synchronized (this) {
        ICachedMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

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
                "Timeseries under this device is not aligned, please use createTimeseries or change device.",
                devicePath.getFullPath());
          }

          IDeviceMNode<ICachedMNode> entityMNode;
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
            IMeasurementMNode<ICachedMNode> measurementMNode =
                nodeFactory.createMeasurementMNode(
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

  public boolean changeAlias(String alias, PartialPath fullPath) throws MetadataException {
    IMeasurementMNode<ICachedMNode> measurementMNode = getMeasurementMNode(fullPath);
    try {
      // upsert alias
      if (alias != null && !alias.equals(measurementMNode.getAlias())) {
        synchronized (this) {
          IDeviceMNode<ICachedMNode> device = measurementMNode.getParent().getAsDeviceMNode();
          ICachedMNode cachedMNode = store.getChild(device.getAsMNode(), alias);
          if (cachedMNode != null) {
            unPinMNode(cachedMNode);
            throw new MetadataException(
                "The alias is duplicated with the name or alias of other measurement.");
          }
          if (measurementMNode.getAlias() != null) {
            device.deleteAliasChild(measurementMNode.getAlias());
          }
          device.addAlias(alias, measurementMNode);
          setAlias(measurementMNode, alias);
        }
        return true;
      }
      return false;
    } finally {
      unPinMNode(measurementMNode.getAsMNode());
    }
  }

  public Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList) {
    ICachedMNode device;
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
        ICachedMNode node = null;
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

  private ICachedMNode checkAndAutoCreateInternalPath(PartialPath devicePath)
      throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return null;
    }
    ICachedMNode cur = storageGroupMNode;
    ICachedMNode child;
    String childName;
    try {
      // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
      for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
        childName = nodeNames[i];
        child = store.getChild(cur, childName);
        if (child == null) {
          child = store.addChild(cur, childName, nodeFactory.createInternalMNode(cur, childName));
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

  private ICachedMNode checkAndAutoCreateDeviceNode(String deviceName, ICachedMNode deviceParent)
      throws MetadataException {
    if (deviceParent == null) {
      // device is sg
      pinMNode(storageGroupMNode);
      return storageGroupMNode;
    }
    ICachedMNode device = store.getChild(deviceParent, deviceName);
    if (device == null) {
      device =
          store.addChild(
              deviceParent, deviceName, nodeFactory.createInternalMNode(deviceParent, deviceName));
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
  public IMeasurementMNode<ICachedMNode> deleteTimeseries(PartialPath path)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0) {
      throw new IllegalPathException(path.getFullPath());
    }

    IMeasurementMNode<ICachedMNode> deletedNode = getMeasurementMNode(path);
    ICachedMNode parent = deletedNode.getParent();
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
  private void deleteAndUnpinEmptyInternalMNode(IDeviceMNode<ICachedMNode> entityMNode)
      throws MetadataException {
    ICachedMNode curNode = entityMNode.getAsMNode();
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      ICachedMNode child;
      IMNodeIterator<ICachedMNode> iterator = store.getChildrenIterator(curNode);
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

  private boolean isEmptyInternalMNode(ICachedMNode node) throws MetadataException {
    IMNodeIterator<ICachedMNode> iterator = store.getChildrenIterator(node);
    try {
      return !IoTDBConstant.PATH_ROOT.equals(node.getName())
          && !node.isMeasurement()
          && !(node.isDevice() && node.getAsDeviceMNode().isUseTemplate())
          && !iterator.hasNext();
    } finally {
      iterator.close();
    }
  }

  public List<PartialPath> constructSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater<ICachedMNode> updater =
        new MeasurementUpdater<ICachedMNode>(rootNode, pathPattern, store, false) {

          protected void updateMeasurement(IMeasurementMNode<ICachedMNode> node)
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

  public List<PartialPath> rollbackSchemaBlackList(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    try (MeasurementUpdater<ICachedMNode> updater =
        new MeasurementUpdater<ICachedMNode>(rootNode, pathPattern, store, false) {

          protected void updateMeasurement(IMeasurementMNode<ICachedMNode> node)
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

  public List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    try (MeasurementCollector<Void, ICachedMNode> collector =
        new MeasurementCollector<Void, ICachedMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<ICachedMNode> node) {
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
    try (MeasurementCollector<Void, ICachedMNode> collector =
        new MeasurementCollector<Void, ICachedMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<ICachedMNode> node) {
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

  public void setAlias(IMeasurementMNode<ICachedMNode> measurementMNode, String alias)
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
  public ICachedMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException {
    String[] nodeNames = deviceId.getNodes();
    MetaFormatUtils.checkTimeseries(deviceId);
    ICachedMNode cur = storageGroupMNode;
    ICachedMNode child;
    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          child =
              store.addChild(cur, nodeNames[i], nodeFactory.createInternalMNode(cur, nodeNames[i]));
        }
        cur = child;
      }
      pinMNode(cur);
      return cur;
    } finally {
      unPinPath(cur);
    }
  }

  /**
   * Check if the device node exists
   *
   * @param deviceId full path of device
   * @return true if the device node exists
   */
  public boolean checkDeviceNodeExists(PartialPath deviceId) {
    ICachedMNode deviceMNode = null;
    try {
      deviceMNode = getNodeByPath(deviceId);
      return deviceMNode.isDevice();
    } catch (MetadataException e) {
      return false;
    } finally {
      if (deviceMNode != null) {
        unPinMNode(deviceMNode);
      }
    }
  }

  // endregion

  // region Interfaces and Implementation for metadata info Query

  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    try (MeasurementCollector<Void, ICachedMNode> collector =
        new MeasurementCollector<Void, ICachedMNode>(rootNode, pathPattern, store, false) {

          protected Void collectMeasurement(IMeasurementMNode<ICachedMNode> node) {
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
  public ICachedMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    ICachedMNode cur = storageGroupMNode;
    ICachedMNode next;
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

  public IMeasurementMNode<ICachedMNode> getMeasurementMNode(PartialPath path)
      throws MetadataException {
    ICachedMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      unPinMNode(node);
      throw new MNodeTypeMismatchException(
          path.getFullPath(), SchemaConstant.MEASUREMENT_MNODE_TYPE);
    }
  }
  // endregion

  // region Interfaces and Implementation for Template check and query

  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    ICachedMNode cur = storageGroupMNode;
    ICachedMNode child;
    IDeviceMNode<ICachedMNode> entityMNode;

    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
        child = store.getChild(cur, nodes[i]);
        if (child == null) {
          throw new PathNotExistException(activatePath.getFullPath());
        }
        cur = child;
      }
      synchronized (this) {
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
      regionStatistics.activateTemplate(template.getId());
    } finally {
      unPinPath(cur);
    }
  }

  public void activateTemplateWithoutCheck(
      PartialPath activatePath, int templateId, boolean isAligned) throws MetadataException {
    String[] nodes = activatePath.getNodes();
    ICachedMNode cur = storageGroupMNode;
    ICachedMNode child;
    IDeviceMNode<ICachedMNode> entityMNode;

    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
        child = store.getChild(cur, nodes[i]);
        if (child == null) {
          throw new PathNotExistException(activatePath.getFullPath());
        }
        cur = child;
      }
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

      store.updateMNode(entityMNode.getAsMNode());
      regionStatistics.activateTemplate(templateId);
    } finally {
      unPinPath(cur);
    }
  }

  public Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICachedMNode> updater =
          new EntityUpdater<ICachedMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<ICachedMNode> node) throws MetadataException {
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

  public Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICachedMNode> updater =
          new EntityUpdater<ICachedMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<ICachedMNode> node) throws MetadataException {
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

  public Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (EntityUpdater<ICachedMNode> collector =
          new EntityUpdater<ICachedMNode>(rootNode, entry.getKey(), store, false) {

            protected void updateEntity(IDeviceMNode<ICachedMNode> node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                regionStatistics.deactivateTemplate(node.getSchemaTemplateId());
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

  public long countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    try (EntityCounter<ICachedMNode> counter =
        new EntityCounter<>(rootNode, pathPattern, store, false)) {
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
  public void pinMNode(ICachedMNode node) throws MetadataException {
    store.pin(node);
  }

  // TODO: This interface should not be exposed to SchemaRegion
  public void unPinMNode(ICachedMNode node) {
    store.unPin(node);
  }

  private void unPinPath(ICachedMNode node) {
    store.unPinPath(node);
  }

  // TODO: This interface should not be exposed to SchemaRegion
  public void updateMNode(ICachedMNode node) throws MetadataException {
    store.updateMNode(node);
  }

  // endregion

  // region Interfaces for schema reader
  @SuppressWarnings("java:S2095")
  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    EntityCollector<IDeviceSchemaInfo, ICachedMNode> collector =
        new EntityCollector<IDeviceSchemaInfo, ICachedMNode>(
            rootNode, showDevicesPlan.getPath(), store, showDevicesPlan.isPrefixMatch()) {

          protected IDeviceSchemaInfo collectEntity(IDeviceMNode<ICachedMNode> node) {
            PartialPath device = getPartialPathFromRootToNode(node.getAsMNode());
            return new ShowDevicesResult(device.getFullPath(), node.isAlignedNullable());
          }
        };
    if (showDevicesPlan.usingSchemaTemplate()) {
      collector.setSchemaTemplateFilter(showDevicesPlan.getSchemaTemplateId());
    }
    ISchemaReader<IDeviceSchemaInfo> reader =
        new ISchemaReader<IDeviceSchemaInfo>() {

          private final DeviceFilterVisitor filterVisitor = new DeviceFilterVisitor();
          private IDeviceSchemaInfo next;

          public boolean isSuccess() {
            return collector.isSuccess();
          }

          public Throwable getFailure() {
            return collector.getFailure();
          }

          public void close() {
            collector.close();
          }

          public ListenableFuture<?> isBlocked() {
            return NOT_BLOCKED;
          }

          public boolean hasNext() {
            while (next == null && collector.hasNext()) {
              IDeviceSchemaInfo temp = collector.next();
              if (filterVisitor.process(showDevicesPlan.getSchemaFilter(), temp)) {
                next = temp;
              }
            }
            return next != null;
          }

          public IDeviceSchemaInfo next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            IDeviceSchemaInfo result = next;
            next = null;
            return result;
          }
        };
    if (showDevicesPlan.getLimit() > 0 || showDevicesPlan.getOffset() > 0) {
      return new SchemaReaderLimitOffsetWrapper<>(
          reader, showDevicesPlan.getLimit(), showDevicesPlan.getOffset());
    } else {
      return reader;
    }
  }

  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      IShowTimeSeriesPlan showTimeSeriesPlan,
      Function<Long, Pair<Map<String, String>, Map<String, String>>> tagAndAttributeProvider)
      throws MetadataException {
    MeasurementCollector<ITimeSeriesSchemaInfo, ICachedMNode> collector =
        new MeasurementCollector<ITimeSeriesSchemaInfo, ICachedMNode>(
            rootNode, showTimeSeriesPlan.getPath(), store, showTimeSeriesPlan.isPrefixMatch()) {

          protected ITimeSeriesSchemaInfo collectMeasurement(IMeasurementMNode<ICachedMNode> node) {
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

              @Override
              public ITimeSeriesSchemaInfo snapshot() {
                return new TimeseriesSchemaInfo(
                    node, getPartialPath(), getTags(), getAttributes(), isUnderAlignedDevice());
              }
            };
          }
        };

    collector.setTemplateMap(showTimeSeriesPlan.getRelatedTemplate(), nodeFactory);
    ISchemaReader<ITimeSeriesSchemaInfo> reader =
        new TimeseriesReaderWithViewFetch(
            collector, showTimeSeriesPlan.getSchemaFilter(), showTimeSeriesPlan.needViewDetail());
    if (showTimeSeriesPlan.getLimit() > 0 || showTimeSeriesPlan.getOffset() > 0) {
      return new SchemaReaderLimitOffsetWrapper<>(
          reader, showTimeSeriesPlan.getLimit(), showTimeSeriesPlan.getOffset());
    } else {
      return reader;
    }
  }

  @SuppressWarnings("java:S2095")
  public ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException {
    MNodeCollector<INodeSchemaInfo, ICachedMNode> collector =
        new MNodeCollector<INodeSchemaInfo, ICachedMNode>(
            rootNode, showNodesPlan.getPath(), store, showNodesPlan.isPrefixMatch()) {

          protected INodeSchemaInfo collectMNode(ICachedMNode node) {
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

      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      public boolean hasNext() {
        return collector.hasNext();
      }

      public INodeSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return collector.next();
      }
    };
  }
  // endregion
}
