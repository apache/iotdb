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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MNodeTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementInBlackListException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor.StorageGroupFilter;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.CollectorTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.EntityCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MNodeCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.CounterTraverser;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.EntityCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MNodeLevelCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementCounter;
import org.apache.iotdb.db.metadata.mtree.traverser.counter.MeasurementGroupByLevelCounter;
import org.apache.iotdb.db.metadata.rescon.SchemaStatisticsManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
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
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;
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
public class MTreeBelowSGCachedImpl implements IMTreeBelowSG {

  private final CachedMTreeStore store;
  private volatile IStorageGroupMNode storageGroupMNode;
  private final Function<IMeasurementMNode, Map<String, String>> tagGetter;
  private final int levelOfSG;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGCachedImpl(
      IStorageGroupMNode storageGroupMNode,
      Function<IMeasurementMNode, Map<String, String>> tagGetter,
      Runnable flushCallback,
      Consumer<IMeasurementMNode> measurementProcess,
      int schemaRegionId)
      throws MetadataException, IOException {
    this.tagGetter = tagGetter;
    PartialPath storageGroup = storageGroupMNode.getPartialPath();
    store = new CachedMTreeStore(storageGroup, schemaRegionId, flushCallback);
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.storageGroupMNode.setParent(storageGroupMNode.getParent());
    levelOfSG = storageGroup.getNodeLength() - 1;

    // recover measurement
    MeasurementCollector<?> collector =
        new MeasurementCollector<Void>(
            this.storageGroupMNode, new PartialPath(storageGroupMNode.getFullPath()), this.store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            measurementProcess.accept(node);
            SchemaStatisticsManager.getInstance().addTimeseries(1);
          }
        };
    collector.setPrefixMatch(true);
    collector.traverse();
  }

  /** Only used for load snapshot */
  private MTreeBelowSGCachedImpl(
      CachedMTreeStore store,
      IStorageGroupMNode storageGroupMNode,
      Consumer<IMeasurementMNode> measurementProcess,
      Function<IMeasurementMNode, Map<String, String>> tagGetter)
      throws MetadataException {
    this.store = store;
    this.storageGroupMNode = store.getRoot().getAsStorageGroupMNode();
    this.storageGroupMNode.setParent(storageGroupMNode.getParent());
    levelOfSG = storageGroupMNode.getPartialPath().getNodeLength() - 1;
    this.tagGetter = tagGetter;

    // recover measurement
    MeasurementCollector<?> collector =
        new MeasurementCollector<Void>(
            this.storageGroupMNode, new PartialPath(storageGroupMNode.getFullPath()), this.store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            measurementProcess.accept(node);
          }
        };
    collector.setPrefixMatch(true);
    collector.traverse();
  }

  @Override
  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGCachedImpl loadFromSnapshot(
      File snapshotDir,
      IStorageGroupMNode storageGroupMNode,
      int schemaRegionId,
      Consumer<IMeasurementMNode> measurementProcess,
      Function<IMeasurementMNode, Map<String, String>> tagGetter,
      Runnable flushCallback)
      throws IOException, MetadataException {
    return new MTreeBelowSGCachedImpl(
        CachedMTreeStore.loadFromSnapshot(
            snapshotDir, storageGroupMNode.getFullPath(), schemaRegionId, flushCallback),
        storageGroupMNode,
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
    Pair<IMNode, Template> pair = checkAndAutoCreateInternalPath(devicePath);
    IMNode deviceParent = pair.left;
    Template upperTemplate = pair.right;

    try {
      // synchronize check and add, we need addChild and add Alias become atomic operation
      // only write on mtree will be synchronized
      synchronized (this) {
        pair = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent, upperTemplate);
        IMNode device = pair.left;
        upperTemplate = pair.right;

        try {
          MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

          String leafName = path.getMeasurement();

          if (alias != null && store.hasChild(device, alias)) {
            throw new AliasAlreadyExistException(path.getFullPath(), alias);
          }

          if (store.hasChild(device, leafName)) {
            throw new PathAlreadyExistException(path.getFullPath());
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
            entityMNode = store.setToEntity(device);
            if (entityMNode.isStorageGroup()) {
              this.storageGroupMNode = entityMNode.getAsStorageGroupMNode();
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
    Pair<IMNode, Template> pair = checkAndAutoCreateInternalPath(devicePath);
    IMNode deviceParent = pair.left;
    Template upperTemplate = pair.right;

    try {
      // synchronize check and add, we need addChild operation be atomic.
      // only write operations on mtree will be synchronized
      synchronized (this) {
        pair = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent, upperTemplate);
        IMNode device = pair.left;
        upperTemplate = pair.right;

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
            entityMNode = store.setToEntity(device);
            entityMNode.setAligned(true);
            if (entityMNode.isStorageGroup()) {
              this.storageGroupMNode = entityMNode.getAsStorageGroupMNode();
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

  private Pair<IMNode, Template> checkAndAutoCreateInternalPath(PartialPath devicePath)
      throws MetadataException {
    String[] nodeNames = devicePath.getNodes();
    MetaFormatUtils.checkTimeseries(devicePath);
    if (nodeNames.length == levelOfSG + 1) {
      return new Pair<>(null, null);
    }
    IMNode cur = storageGroupMNode;
    IMNode child;
    String childName;
    Template upperTemplate = cur.getSchemaTemplate();
    try {
      // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to sg node, parent of d1
      for (int i = levelOfSG + 1; i < nodeNames.length - 1; i++) {
        childName = nodeNames[i];
        child = store.getChild(cur, childName);
        if (child == null) {
          if (upperTemplate != null && upperTemplate.getDirectNode(childName) != null) {
            throw new TemplateImcompatibeException(
                devicePath.getFullPath(), upperTemplate.getName(), childName);
          }
          child = store.addChild(cur, childName, new InternalMNode(cur, childName));
        }
        cur = child;

        if (cur.isMeasurement()) {
          throw new PathAlreadyExistException(cur.getFullPath());
        }

        if (cur.getSchemaTemplate() != null) {
          upperTemplate = cur.getSchemaTemplate();
        }
      }
      pinMNode(cur);
      return new Pair<>(cur, upperTemplate);
    } finally {
      unPinPath(cur);
    }
  }

  private Pair<IMNode, Template> checkAndAutoCreateDeviceNode(
      String deviceName, IMNode deviceParent, Template upperTemplate) throws MetadataException {
    if (deviceParent == null) {
      // device is sg
      pinMNode(storageGroupMNode);
      return new Pair<>(storageGroupMNode, null);
    }
    IMNode device = store.getChild(deviceParent, deviceName);
    if (device == null) {
      if (upperTemplate != null && upperTemplate.getDirectNode(deviceName) != null) {
        throw new TemplateImcompatibeException(
            deviceParent.getPartialPath().concatNode(deviceName).getFullPath(),
            upperTemplate.getName(),
            deviceName);
      }
      device =
          store.addChild(deviceParent, deviceName, new InternalMNode(deviceParent, deviceName));
    }

    if (device.isMeasurement()) {
      throw new PathAlreadyExistException(device.getFullPath());
    }

    if (device.getSchemaTemplate() != null) {
      upperTemplate = device.getSchemaTemplate();
    }

    return new Pair<>(device, upperTemplate);
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

    if (isPathExistsWithinTemplate(path)) {
      throw new MetadataException("Cannot delete a timeseries inside a template: " + path);
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
            this.storageGroupMNode = curNode.getAsStorageGroupMNode();
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
          && node.getSchemaTemplate() == null
          && !node.isUseTemplate()
          && !iterator.hasNext();
    } finally {
      iterator.close();
    }
  }

  @Override
  public List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    List<PartialPath> result = new LinkedList<>();
    MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            if (node.isPreDeleted()) {
              result.add(getCurrentPartialPath(node));
            }
          }
        };
    collector.setResultSet(result);
    collector.setShouldTraverseTemplate(false);
    collector.traverse();
    return result;
  }

  @Override
  public Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            if (node.isPreDeleted()) {
              result.add(getCurrentPartialPath(node).getDevicePath());
            }
          }
        };
    collector.traverse();
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
    Template upperTemplate = cur.getSchemaTemplate();
    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          if (cur.isUseTemplate() && upperTemplate.getDirectNode(nodeNames[i]) != null) {
            throw new PathAlreadyExistException(
                cur.getPartialPath().concatNode(nodeNames[i]).getFullPath());
          }
          child = store.addChild(cur, nodeNames[i], new InternalMNode(cur, nodeNames[i]));
        }
        cur = child;
        // update upper template
        upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
      }
      pinMNode(cur);
      return cur;
    } finally {
      unPinPath(cur);
    }
  }

  @Override
  public IEntityMNode setToEntity(IMNode node) throws MetadataException {
    // synchronize check and replace, we need replaceChild become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      IEntityMNode entityMNode = store.setToEntity(node);
      if (entityMNode.isStorageGroup()) {
        this.storageGroupMNode = entityMNode.getAsStorageGroupMNode();
      }
      return entityMNode;
    }
  }
  // endregion

  // region Interfaces and Implementation for metadata info Query
  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;
    Template upperTemplate = cur.getSchemaTemplate();
    boolean isInTemplate = false;
    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
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

            if (upperTemplate == null
                || !cur.isUseTemplate()
                || upperTemplate.getDirectNode(nodeNames[i]) == null) {
              return false;
            }
            child = upperTemplate.getDirectNode(nodeNames[i]);
            isInTemplate = true;
            unPinPath(cur);
            if (child.isMeasurement()) {
              return i == nodeNames.length - 1;
            }
          } else {
            if (child.isMeasurement()) {
              cur = child;
              return i == nodeNames.length - 1;
            }
          }
        }
        cur = child;
      }
      return true;
    } finally {
      if (!isInTemplate) {
        unPinPath(cur);
      }
    }
  }

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
    EntityCollector<Set<PartialPath>> collector =
        new EntityCollector<Set<PartialPath>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            result.add(getCurrentPartialPath(node));
          }
        };
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    return result;
  }

  @Override
  public Pair<List<ShowDevicesResult>, Integer> getDevices(ShowDevicesPlan plan)
      throws MetadataException {
    List<ShowDevicesResult> res = new ArrayList<>();
    EntityCollector<List<ShowDevicesResult>> collector =
        new EntityCollector<List<ShowDevicesResult>>(
            storageGroupMNode, plan.getPath(), store, plan.getLimit(), plan.getOffset()) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            PartialPath device = getCurrentPartialPath(node);
            if (plan.hasSgCol()) {
              res.add(
                  new ShowDevicesResult(
                      device.getFullPath(),
                      node.isAligned(),
                      getStorageGroupNodeInTraversePath(node).getFullPath()));
            } else {
              res.add(new ShowDevicesResult(device.getFullPath(), node.isAligned()));
            }
          }
        };
    collector.setPrefixMatch(plan.isPrefixMatch());
    collector.traverse();

    return new Pair<>(res, collector.getCurOffset() + 1);
  }

  @Override
  public Set<PartialPath> getDevicesByTimeseries(PartialPath timeseries) throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    MeasurementCollector<Set<PartialPath>> collector =
        new MeasurementCollector<Set<PartialPath>>(storageGroupMNode, timeseries, store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
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
  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return getMeasurementPathsWithAlias(pathPattern, 0, 0, isPrefixMatch, false).left;
  }

  /**
   * Get all measurement paths matching the given path pattern
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard.
   */
  @Override
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
  @Override
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> result = new LinkedList<>();
    MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(
            storageGroupMNode, pathPattern, store, limit, offset) {
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
        };
    collector.setPrefixMatch(isPrefixMatch);
    collector.traverse();
    offset = collector.getCurOffset() + 1;
    return new Pair<>(result, offset);
  }

  /**
   * Get all measurement schema matching the given path pattern
   *
   * <p>result: [name, alias, database, dataType, encoding, compression, offset] and the current
   * offset
   */
  @Override
  public Pair<List<Pair<PartialPath, String[]>>, Integer> getAllMeasurementSchema(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException {
    /*
     There are two conditions and 4 cases.
     1. isOrderByHeat = false && limit = 0 : just collect all results from each database
     2. isOrderByHeat = false && limit != 0 : the offset and limit should be updated by each sg after traverse, thus the final result will satisfy the constraints of limit and offset
     3. isOrderByHeat = true && limit = 0 : collect all result from each database and then sort
     4. isOrderByHeat = true && limit != 0 : collect top limit result from each sg and then sort them and collect the top limit results start from offset.
     The offset must be 0, since each sg should collect top limit results. The current limit is the sum of origin limit and offset when passed into metadata module
    */

    boolean needLast = plan.isOrderByHeat();
    int limit = needLast ? 0 : plan.getLimit();
    int offset = needLast ? 0 : plan.getOffset();

    MeasurementCollector<List<Pair<PartialPath, String[]>>> collector =
        new MeasurementCollector<List<Pair<PartialPath, String[]>>>(
            storageGroupMNode, plan.getPath(), store, limit, offset) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) {
            IMeasurementSchema measurementSchema = node.getSchema();
            String[] tsRow = new String[7];
            tsRow[0] = node.getAlias();
            tsRow[1] = getStorageGroupNodeInTraversePath(node).getFullPath();
            tsRow[2] = measurementSchema.getType().toString();
            tsRow[3] = measurementSchema.getEncodingType().toString();
            tsRow[4] = measurementSchema.getCompressor().toString();
            tsRow[5] = String.valueOf(node.getOffset());
            tsRow[6] = needLast ? String.valueOf(getLastTimeStamp(node, queryContext)) : null;
            Pair<PartialPath, String[]> temp = new Pair<>(getCurrentPartialPath(node), tsRow);
            resultSet.add(temp);
          }
        };
    collector.setPrefixMatch(plan.isPrefixMatch());
    collector.setResultSet(new LinkedList<>());
    collector.traverse();

    List<Pair<PartialPath, String[]>> result = collector.getResult();

    if (needLast) {
      Stream<Pair<PartialPath, String[]>> stream = result.stream();

      limit = plan.getLimit();
      offset = plan.getOffset();

      stream =
          stream.sorted(
              Comparator.comparingLong(
                      (Pair<PartialPath, String[]> p) -> Long.parseLong(p.right[6]))
                  .reversed()
                  .thenComparing((Pair<PartialPath, String[]> p) -> p.left));

      if (limit != 0) {
        stream = stream.skip(offset).limit(limit);
      }

      result = stream.collect(toList());
    }

    return new Pair<>(result, collector.getCurOffset() + 1);
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
    try {
      MNodeCollector<Set<TSchemaNode>> collector =
          new MNodeCollector<Set<TSchemaNode>>(
              storageGroupMNode, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
            @Override
            protected void transferToResult(IMNode node) {
              resultSet.add(
                  new TSchemaNode(
                      getCurrentPartialPath(node).getFullPath(),
                      node.getMNodeType(false).getNodeType()));
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
  @Override
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    try {
      MNodeCollector<Set<String>> collector =
          new MNodeCollector<Set<String>>(
              storageGroupMNode, pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), store) {
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
  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch, StorageGroupFilter filter)
      throws MetadataException {
    MNodeCollector<List<PartialPath>> collector =
        new MNodeCollector<List<PartialPath>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void transferToResult(IMNode node) {
            resultSet.add(getCurrentPartialPath(node));
          }
        };
    collector.setResultSet(new LinkedList<>());
    collector.setTargetLevel(nodeLevel);
    collector.setPrefixMatch(isPrefixMatch);
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
  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new MeasurementCounter(storageGroupMNode, pathPattern, store);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new MeasurementCounter(storageGroupMNode, pathPattern, store);
    counter.setPrefixMatch(isPrefixMatch);
    counter.setTemplateMap(templateMap);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of timeseries matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException {
    return getAllTimeseriesCount(pathPattern, false);
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, List<String> timeseries, boolean hasTag)
      throws MetadataException {
    CounterTraverser counter =
        new MeasurementCounter(storageGroupMNode, pathPattern, store, timeseries, hasTag);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of devices matching the given path. If using prefix match, the path pattern is
   * used to match prefix path. All timeseries start with the matched prefix path will be counted.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  @Override
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    CounterTraverser counter = new EntityCounter(storageGroupMNode, pathPattern, store);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  /**
   * Get the count of devices matching the given path.
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   */
  @Override
  public int getDevicesNum(PartialPath pathPattern) throws MetadataException {
    return getDevicesNum(pathPattern, false);
  }

  /**
   * Get the count of nodes in the given level matching the given path. If using prefix match, the
   * path pattern is used to match prefix path. All timeseries start with the matched prefix path
   * will be counted.
   */
  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    MNodeLevelCounter counter = new MNodeLevelCounter(storageGroupMNode, pathPattern, store, level);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getCount();
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    MeasurementGroupByLevelCounter counter =
        new MeasurementGroupByLevelCounter(storageGroupMNode, pathPattern, store, level);
    counter.setPrefixMatch(isPrefixMatch);
    counter.traverse();
    return counter.getResult();
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      List<String> timeseries,
      boolean hasTag)
      throws MetadataException {
    MeasurementGroupByLevelCounter counter =
        new MeasurementGroupByLevelCounter(
            storageGroupMNode, pathPattern, store, level, timeseries, hasTag);
    counter.setPrefixMatch(isPrefixMatch);
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
  public IMNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode next;
    Template upperTemplate = cur.getSchemaTemplate();
    boolean isInTemplate = false;
    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
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
            if (upperTemplate == null
                || !cur.isUseTemplate()
                || upperTemplate.getDirectNode(nodes[i]) == null) {
              throw new PathNotExistException(path.getFullPath(), true);
            }
            next = upperTemplate.getDirectNode(nodes[i]);
            isInTemplate = true;
            unPinPath(cur);
          } else if (next.isMeasurement()) {
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
        pinMNode(cur);
      }
      return cur;
    } finally {
      if (!isInTemplate) {
        unPinPath(cur);
      }
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
    MeasurementCollector<List<PartialPath>> collector =
        new MeasurementCollector<List<PartialPath>>(storageGroupMNode, pathPattern, store) {
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
        };
    collector.setTemplateMap(templateMap);
    collector.traverse();
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

  @Override
  public List<IMeasurementMNode> getMatchedMeasurementMNode(PartialPath pathPattern)
      throws MetadataException {
    List<IMeasurementMNode> result = new ArrayList<>();
    MeasurementCollector<List<IMeasurementMNode>> collector =
        new MeasurementCollector<List<IMeasurementMNode>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            pinMNode(node);
            result.add(node);
          }
        };
    collector.setShouldTraverseTemplate(false);
    collector.traverse();
    return result;
  }

  // endregion

  // region Interfaces and Implementation for Template check and query
  /**
   * check whether there is template on given path and the subTree has template return true,
   * otherwise false
   */
  @Override
  public void checkTemplateOnPath(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;

    if (cur.getSchemaTemplate() != null) {
      throw new MetadataException("Template already exists on " + cur.getFullPath());
    }

    try {
      for (int i = levelOfSG + 1; i < nodeNames.length; i++) {
        child = store.getChild(cur, nodeNames[i]);
        if (child == null) {
          return;
        }
        cur = child;
        if (cur.getSchemaTemplate() != null) {
          throw new MetadataException("Template already exists on " + cur.getFullPath());
        }
        if (cur.isMeasurement()) {
          return;
        }
      }

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
   * <ul>
   *   <p>route 3.1: mounted node has no measurement child, then its alignment will be set as the
   *   template.
   *   <p>route 3.2: mounted node has measurement child, then alignment of it and template should be
   *   identical, otherwise cast a exception.
   * </ul>
   *
   * @return return the node competent to be mounted.
   */
  @Override
  public IMNode checkTemplateAlignmentWithMountedNode(IMNode mountedNode, Template template)
      throws MetadataException {
    boolean hasDirectMeasurement = false;
    for (IMNode child : template.getDirectNodes()) {
      if (child.isMeasurement()) {
        hasDirectMeasurement = true;
      }
    }
    if (!hasDirectMeasurement) {
      return mountedNode;
    }
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
      updateMNode(mountedNode);
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

  @Override
  public void checkIsTemplateCompatibleWithChild(IMNode node, Template template)
      throws MetadataException {
    for (String measurementPath : template.getSchemaMap().keySet()) {
      String directNodeName = PathUtils.splitPathToDetachedNodes(measurementPath)[0];
      if (store.hasChild(node, directNodeName)) {
        throw new MetadataException(
            "Node name "
                + directNodeName
                + " in template has conflict with node's child "
                + (node.getFullPath() + "." + directNodeName));
      }
    }
  }

  @Override
  public void checkTemplateInUseOnLowerNode(IMNode node) throws MetadataException {
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
   * Check that each node set with tarTemplate and its descendants have overlapping nodes with
   * appending measurements
   */
  @Override
  public boolean isTemplateAppendable(Template tarTemplate, List<String> appendMeasurements)
      throws MetadataException {
    List<String> setPaths = getPathsSetOnTemplate(tarTemplate.getName());
    if (setPaths.size() == 0) {
      return true;
    }
    Deque<IMNode> setNodes = new ArrayDeque<>();
    try {
      for (String path : setPaths) {
        setNodes.add(getNodeByPath(new PartialPath(path)));
      }
      // since overlap of template and MTree is not allowed, it is sufficient to check on the first
      // node
      Set<String> overlapSet = new HashSet<>();
      for (String path : appendMeasurements) {
        overlapSet.add(PathUtils.splitPathToDetachedNodes(path)[0]);
      }

      while (setNodes.size() != 0) {
        IMNode cur = setNodes.pop();
        try {
          IMNodeIterator iterator = store.getChildrenIterator(cur);
          try {
            IMNode child;
            while (iterator.hasNext()) {
              child = iterator.next();
              if (overlapSet.contains(child.getName())) {
                unPinMNode(child);
                return false;
              }
              if (!child.isMeasurement()) {
                setNodes.push(child);
              } else {
                unPinMNode(child);
              }
            }
          } finally {
            iterator.close();
          }
        } finally {
          unPinMNode(cur);
        }
      }
      return true;
    } finally {
      while (!setNodes.isEmpty()) {
        unPinMNode(setNodes.pop());
      }
    }
  }

  /**
   * Note that template and MTree cannot have overlap paths.
   *
   * @return true iff path corresponding to a measurement inside a template, whether using or not.
   */
  @Override
  public boolean isPathExistsWithinTemplate(PartialPath path) throws MetadataException {
    String[] pathNodes = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;
    Template upperTemplate = cur.getUpperTemplate();
    try {
      for (int i = levelOfSG + 1; i < pathNodes.length; i++) {
        child = store.getChild(cur, pathNodes[i]);
        if (child != null) {
          cur = child;
          if (cur.isMeasurement()) {
            return false;
          }
          upperTemplate = cur.getSchemaTemplate() == null ? upperTemplate : cur.getSchemaTemplate();
        } else {
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
      return false;
    } finally {
      unPinPath(cur);
    }
  }

  /**
   * Check measurement path and return the mounted node index on path. The node could have not
   * created yet. The result is used for getDeviceNodeWithAutoCreate, which return corresponding
   * IMNode on MTree.
   *
   * @return index on full path of the node which matches all measurements path with its
   *     upperTemplate.
   */
  public int getMountedNodeIndexOnMeasurementPath(PartialPath devicePath, String[] measurements)
      throws MetadataException {
    String[] nodes = devicePath.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;
    Template upperTemplate = cur.getSchemaTemplate();
    int index = levelOfSG + 1;
    boolean attemptToUseTemplate = false;

    try {
      // If there are nodes of target path on MTree, use it as possible.
      for (; index < nodes.length; index++) {
        upperTemplate = cur.getSchemaTemplate() != null ? cur.getSchemaTemplate() : upperTemplate;
        child = store.getChild(cur, nodes[index]);
        if (child == null) {
          if (upperTemplate == null) {
            // no matched child, no template, need to create device node as logical device path
            return nodes.length;
          } else {
            attemptToUseTemplate = true;
            break;
          }
        } else {
          // has child on MTree
          cur = child;
        }
      }
    } finally {
      if (index > levelOfSG + 1) {
        unPinPath(cur);
      }
    }

    if (!attemptToUseTemplate) {
      // all nodes on path exist in MTree, device node should be the penultimate one
      return nodes.length;
    }

    // The resting part of target path not exists on MTree, thus try to use template.
    for (; index < nodes.length; index++) {
      int fullPathLength = nodes.length - index + 1;
      String[] suffixNodes = new String[fullPathLength];
      System.arraycopy(nodes, index, suffixNodes, 0, nodes.length - index);
      boolean hasAllMeasurements = true;

      for (String measurement : measurements) {
        // for this fullPath, cur is the last node on MTree
        // since upperTemplate exists, need to find the matched suffix path of fullPath and
        // template
        suffixNodes[fullPathLength - 1] = measurement;
        String suffixPath = String.join(String.valueOf(PATH_SEPARATOR), suffixNodes);

        if (upperTemplate.hasSchema(suffixPath)) {
          continue;
        }

        // if suffix doesn't match, but first node name matched, it's an overlap with template
        // cast exception for now
        if (upperTemplate.getDirectNode(nodes[index]) != null) {
          throw new TemplateImcompatibeException(
              devicePath.concatNode(measurement).getFullPath(),
              upperTemplate.getName(),
              nodes[index]);
        }

        hasAllMeasurements = false;
      }

      if (hasAllMeasurements) {
        return index - 1;
      }
    }

    return nodes.length;
  }

  @Override
  public List<String> getPathsSetOnTemplate(String templateName) throws MetadataException {
    List<String> resSet = new ArrayList<>();
    CollectorTraverser<Set<String>> setTemplatePaths =
        new CollectorTraverser<Set<String>>(
            storageGroupMNode, new PartialPath(ALL_RESULT_NODES), store) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
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
            if (node.getSchemaTemplate() != null) {
              // if set template, and equals to target or target for all, add to result
              if (templateName.equals(ONE_LEVEL_PATH_WILDCARD)
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

  @Override
  public List<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    List<String> result = new ArrayList<>();

    CollectorTraverser<Set<String>> usingTemplatePaths =
        new CollectorTraverser<Set<String>>(
            storageGroupMNode, new PartialPath(ALL_RESULT_NODES), store) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
            // will never get here, implement for placeholder
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
            // shall not traverse nodes inside template
            if (!node.getPartialPath().equals(getCurrentPartialPath(node))) {
              return true;
            }

            if (node.getUpperTemplate() != null) {
              // this node and its descendants are set other template, exit from this branch
              if (!templateName.equals(ONE_LEVEL_PATH_WILDCARD)
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

  /**
   * Get template name on give path if any node of it has been set a template
   *
   * @return null if no template has been set on path
   */
  @Override
  public String getTemplateOnPath(PartialPath path) throws MetadataException {
    String[] pathNodes = path.getNodes();
    IMNode cur = storageGroupMNode;
    IMNode child;

    if (cur.getSchemaTemplate() != null) {
      return cur.getSchemaTemplate().getName();
    }

    try {
      for (int i = levelOfSG + 1; i < pathNodes.length; i++) {
        child = store.getChild(cur, pathNodes[i]);
        if (child == null) {
          return null;
        }
        cur = child;

        if (cur.isMeasurement()) {
          return null;
        }

        if (cur.getSchemaTemplate() != null) {
          return cur.getSchemaTemplate().getName();
        }
      }

      return null;
    } finally {
      unPinPath(cur);
    }
  }

  @Override
  public void activateTemplate(PartialPath activatePath, Template template)
      throws MetadataException {
    String[] nodes = activatePath.getNodes();
    IMNode cur = storageGroupMNode;
    List<IMNode> pinnedNodes = new ArrayList<>();
    IEntityMNode entityMNode = null;

    try {
      for (int i = levelOfSG + 1; i < nodes.length; i++) {
        cur = store.getChild(cur, nodes[i]);
        pinnedNodes.add(cur);
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
            this.storageGroupMNode = entityMNode.getAsStorageGroupMNode();
          }
        }
      }

      if (!entityMNode.isAligned()) {
        entityMNode.setAligned(template.isDirectAligned());
      }
      entityMNode.setUseTemplate(true);
      entityMNode.setSchemaTemplateId(template.getId());
    } finally {
      if (entityMNode != null) {
        store.updateMNode(entityMNode);
      }
      for (IMNode node : pinnedNodes) {
        store.unPin(node);
      }
    }
  }

  @Override
  public List<String> getPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    Set<String> result = new HashSet<>();

    EntityCollector<Set<String>> collector =
        new EntityCollector<Set<String>>(storageGroupMNode, pathPattern, store) {
          @Override
          protected void collectEntity(IEntityMNode node) {
            if (node.getSchemaTemplateId() == templateId) {
              result.add(node.getFullPath());
            }
          }
        };
    collector.traverse();
    return new ArrayList<>(result);
  }

  public Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      EntityCollector<List<IEntityMNode>> collector =
          new EntityCollector<List<IEntityMNode>>(storageGroupMNode, entry.getKey(), store) {
            @Override
            protected void collectEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.preDeactivateTemplate();
                store.updateMNode(node);
              }
            }
          };
      collector.traverse();
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      EntityCollector<List<IEntityMNode>> collector =
          new EntityCollector<List<IEntityMNode>>(storageGroupMNode, entry.getKey(), store) {
            @Override
            protected void collectEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.rollbackPreDeactivateTemplate();
                store.updateMNode(node);
              }
            }
          };
      collector.traverse();
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      EntityCollector<List<IEntityMNode>> collector =
          new EntityCollector<List<IEntityMNode>>(storageGroupMNode, entry.getKey(), store) {
            @Override
            protected void collectEntity(IEntityMNode node) throws MetadataException {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.deactivateTemplate();
                store.updateMNode(node);
                deleteEmptyInternalMNodeAndReturnEmptyStorageGroup(node);
              }
            }
          };
      collector.traverse();
    }
    return resultTemplateSetInfo;
  }

  @Override
  public int countPathsUsingTemplate(PartialPath pathPattern, int templateId)
      throws MetadataException {
    CounterTraverser counterTraverser =
        new CounterTraverser(storageGroupMNode, pathPattern, store) {
          @Override
          protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
            return false;
          }

          @Override
          protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
            if (node.isEntity() && node.getAsEntityMNode().getSchemaTemplateId() == templateId) {
              count++;
            }
            return false;
          }
        };
    counterTraverser.traverse();
    return counterTraverser.getCount();
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

  public IMNode getChildFromPinnedMNode(IMNode parent, String measurement)
      throws MetadataException {
    return store.getChild(parent, measurement);
  }

  // endregion

}
