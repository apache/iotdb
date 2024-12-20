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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
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
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DeviceAttributeUpdater;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DeviceBlackListConstructor;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionMemMetric;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
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
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeSpaceQuotaManager;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(MTreeBelowSGMemoryImpl.class);

  // this implementation is based on memory, thus only MTree write operation must invoke MTreeStore
  private final MemMTreeStore store;

  @SuppressWarnings("java:S3077")
  private volatile IMemMNode storageGroupMNode;

  private final IMemMNode rootNode;
  private final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter;
  private final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> attributeGetter;
  private final IMNodeFactory<IMemMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();
  private final int levelOfSG;
  private final MemSchemaRegionStatistics regionStatistics;

  // region MTree initialization, clear and serialization
  public MTreeBelowSGMemoryImpl(
      final PartialPath storageGroupPath,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> attributeGetter,
      final MemSchemaRegionStatistics regionStatistics,
      final SchemaRegionMemMetric metric) {
    store = new MemMTreeStore(storageGroupPath, regionStatistics, metric);
    this.regionStatistics = regionStatistics;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
    this.attributeGetter = attributeGetter;
  }

  private MTreeBelowSGMemoryImpl(
      final PartialPath storageGroupPath,
      final MemMTreeStore store,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> attributeGetter,
      final MemSchemaRegionStatistics regionStatistics) {
    this.store = store;
    this.regionStatistics = regionStatistics;
    this.storageGroupMNode = store.getRoot();
    this.rootNode = store.generatePrefix(storageGroupPath);
    levelOfSG = storageGroupPath.getNodeLength() - 1;
    this.tagGetter = tagGetter;
    this.attributeGetter = attributeGetter;
  }

  public void clear() {
    store.clear();
    storageGroupMNode = null;
  }

  public synchronized boolean createSnapshot(final File snapshotDir) {
    return store.createSnapshot(snapshotDir);
  }

  public static MTreeBelowSGMemoryImpl loadFromSnapshot(
      final File snapshotDir,
      final String storageGroupFullPath,
      final MemSchemaRegionStatistics regionStatistics,
      final SchemaRegionMemMetric metric,
      final Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      final Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      final BiConsumer<IDeviceMNode<IMemMNode>, String> tableDeviceProcess,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> tagGetter,
      final Function<IMeasurementMNode<IMemMNode>, Map<String, String>> attributeGetter)
      throws IOException, IllegalPathException {
    return new MTreeBelowSGMemoryImpl(
        PartialPath.getDatabasePath(storageGroupFullPath),
        MemMTreeStore.loadFromSnapshot(
            snapshotDir,
            measurementProcess,
            deviceProcess,
            tableDeviceProcess,
            regionStatistics,
            metric),
        tagGetter,
        attributeGetter,
        regionStatistics);
  }

  // endregion

  // region time series operation, including creation and deletion

  /**
   * Create a time series with a full path from root to leaf node. Before creating a time series,
   * the database should be set first, throw exception otherwise
   *
   * @param path time series path
   * @param dataType data type
   * @param encoding encoding
   * @param compressor compressor
   * @param props props
   * @param alias alias of measurement
   */
  public IMeasurementMNode<IMemMNode> createTimeSeries(
      final PartialPath path,
      final TSDataType dataType,
      final TSEncoding encoding,
      final CompressionType compressor,
      final Map<String, String> props,
      final String alias,
      final boolean withMerge)
      throws MetadataException {
    final String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2) {
      throw new IllegalPathException(path.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(path);
    final PartialPath devicePath = path.getDevicePath();
    final IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mTree will be synchronized
    synchronized (this) {
      final IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), props);

      final String leafName = path.getMeasurement();

      if (!withMerge && alias != null && device.hasChild(alias)) {
        throw new AliasAlreadyExistException(path.getFullPath(), alias);
      }

      if (device.hasChild(leafName)) {
        final IMemMNode node = device.getChild(leafName);
        if (node.isMeasurement()) {
          final IMeasurementMNode<IMemMNode> measurementNode = node.getAsMeasurementMNode();
          if (measurementNode.isPreDeleted()) {
            throw new MeasurementInBlackListException(path);
          } else if (!withMerge || measurementNode.getDataType() != dataType) {
            // Report conflict if the types are different
            throw new MeasurementAlreadyExistException(
                path.getFullPath(), node.getAsMeasurementMNode().getMeasurementPath());
          } else {
            // Return null iff we should merge the time series with the existing one
            return null;
          }
        } else {
          throw new PathAlreadyExistException(path.getFullPath());
        }
      }

      if (device.isDevice()
          && device.getAsDeviceMNode().isAlignedNullable() != null
          && device.getAsDeviceMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "time series under this device is aligned, please use createAlignedTimeseries or change device.",
            device.getFullPath());
      }

      final IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
      }

      // create a non-aligned time series
      if (entityMNode.isAlignedNullable() == null) {
        entityMNode.setAligned(false);
      }

      final IMeasurementMNode<IMemMNode> measurementMNode =
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
   * Create aligned time series with full paths from root to one leaf node. Before creating time
   * series, the * database should be set first, throw exception otherwise
   *
   * @param devicePath device path
   * @param measurements measurements list
   * @param dataTypes data types list
   * @param encodings encodings list
   * @param compressors compressor
   */
  public List<IMeasurementMNode<IMemMNode>> createAlignedTimeSeries(
      final PartialPath devicePath,
      final List<String> measurements,
      final List<TSDataType> dataTypes,
      final List<TSEncoding> encodings,
      final List<CompressionType> compressors,
      final List<String> aliasList,
      final boolean withMerge,
      final Set<Integer> existingMeasurementIndexes)
      throws MetadataException {
    final List<IMeasurementMNode<IMemMNode>> measurementMNodeList = new ArrayList<>();
    MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    final IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    // synchronize check and add, we need addChild operation be atomic.
    // only write operations on mTree will be synchronized
    synchronized (this) {
      final IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      for (int i = 0; i < measurements.size(); i++) {
        if (device.hasChild(measurements.get(i))) {
          final IMemMNode node = device.getChild(measurements.get(i));
          if (node.isMeasurement()) {
            final IMeasurementMNode<IMemMNode> measurementNode = node.getAsMeasurementMNode();
            if (node.getAsMeasurementMNode().isPreDeleted()) {
              throw new MeasurementInBlackListException(
                  devicePath.concatAsMeasurementPath(measurements.get(i)));
            } else if (!withMerge || measurementNode.getDataType() != dataTypes.get(i)) {
              throw new MeasurementAlreadyExistException(
                  devicePath.getFullPath() + "." + measurements.get(i),
                  node.getAsMeasurementMNode().getMeasurementPath());
            } else {
              existingMeasurementIndexes.add(i);
              continue;
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

      if (device.isDevice()
          && device.getAsDeviceMNode().isAlignedNullable() != null
          && !device.getAsDeviceMNode().isAligned()) {
        throw new AlignedTimeseriesException(
            "Time series under this device is not aligned, please use createTimeSeries or change device.",
            devicePath.getFullPath());
      }

      final IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
        entityMNode.setAligned(true);
      }

      // create an aligned time series
      if (entityMNode.isAlignedNullable() == null) {
        entityMNode.setAligned(true);
      }

      for (int i = 0; i < measurements.size(); i++) {
        if (existingMeasurementIndexes.contains(i)) {
          continue;
        }

        final IMeasurementMNode<IMemMNode> measurementMNode =
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

  private IMemMNode checkAndAutoCreateInternalPath(final PartialPath devicePath)
      throws MetadataException {
    final String[] nodeNames = devicePath.getNodes();
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

  private IMemMNode checkAndAutoCreateDeviceNode(
      final String deviceName, final IMemMNode deviceParent)
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
      final PartialPath devicePath,
      final List<String> measurementList,
      final List<String> aliasList) {
    final IMemMNode device;
    try {
      device = getNodeByPath(devicePath);
    } catch (final PathNotExistException e) {
      return Collections.emptyMap();
    }

    if (!device.isDevice()) {
      return Collections.emptyMap();
    }
    final Map<Integer, MetadataException> failingMeasurementMap = new HashMap<>();
    for (int i = 0; i < measurementList.size(); i++) {
      if (device.hasChild(measurementList.get(i))) {
        final IMemMNode node = device.getChild(measurementList.get(i));
        if (node.isMeasurement()) {
          if (node.getAsMeasurementMNode().isPreDeleted()) {
            failingMeasurementMap.put(
                i,
                new MeasurementInBlackListException(
                    devicePath.concatAsMeasurementPath(measurementList.get(i))));
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
                  "The number of timeseries has reached the upper limit",
                  TSStatusCode.SPACE_QUOTA_EXCEEDED.getStatusCode()));
        }
      }
    }
    return failingMeasurementMap;
  }

  public boolean changeAlias(final String alias, final PartialPath fullPath)
      throws MetadataException {
    final IMeasurementMNode<IMemMNode> measurementMNode = getMeasurementMNode(fullPath);
    // upsert alias
    if (alias != null && !alias.equals(measurementMNode.getAlias())) {
      synchronized (this) {
        final IDeviceMNode<IMemMNode> device = measurementMNode.getParent().getAsDeviceMNode();
        final IMemMNode memMNode = store.getChild(device.getAsMNode(), alias);
        if (memMNode != null) {
          throw new MetadataException(
              "The alias is duplicated with the name or alias of other measurement, alias: "
                  + alias
                  + ", fullPath: "
                  + fullPath
                  + ", otherMeasurement: "
                  + memMNode.getFullPath());
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
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  public IMeasurementMNode<IMemMNode> deleteTimeSeries(final PartialPath path)
      throws MetadataException {
    final String[] nodes = path.getNodes();
    if (nodes.length == 0) {
      throw new IllegalPathException(path.getFullPath());
    }

    final IMeasurementMNode<IMemMNode> deletedNode = getMeasurementMNode(path);
    final IMemMNode parent = deletedNode.getParent();
    // delete the last node of path
    synchronized (this) {
      store.deleteChild(parent, path.getMeasurement());
      if (deletedNode.getAlias() != null) {
        parent.getAsDeviceMNode().deleteAliasChild(deletedNode.getAlias());
      }
    }
    deleteEmptyInternalMNode(parent.getAsDeviceMNode());
    return deletedNode;
  }

  /** Used when delete timeseries or deactivate template */
  public void deleteEmptyInternalMNode(final IDeviceMNode<IMemMNode> entityMNode) {
    IMemMNode curNode = entityMNode.getAsMNode();
    if (!entityMNode.isUseTemplate()) {
      boolean hasMeasurement = false;
      boolean hasNonViewMeasurement = false;
      IMemMNode child;
      IMNodeIterator<IMemMNode> iterator = store.getChildrenIterator(curNode);
      while (iterator.hasNext()) {
        child = iterator.next();
        if (child.isMeasurement()) {
          hasMeasurement = true;
          if (!child.getAsMeasurementMNode().isLogicalView()) {
            hasNonViewMeasurement = true;
            break;
          }
        }
      }

      if (!hasMeasurement) {
        synchronized (this) {
          curNode = store.setToInternal(entityMNode);
        }
      } else if (!hasNonViewMeasurement) {
        // has some measurement but they are all logical view
        entityMNode.setAligned(null);
      }
    }

    // delete all empty ancestors except database and MeasurementMNode
    while (true) {
      // if current database has no time series, return the database name
      if (curNode.isDatabase()) {
        return;
      }

      synchronized (this) {
        if (!isEmptyInternalMNode(curNode)) {
          break;
        }
        store.deleteChild(curNode.getParent(), curNode.getName());
        curNode = curNode.getParent();
      }
    }
  }

  private boolean isEmptyInternalMNode(final IMemMNode node) {
    return !IoTDBConstant.PATH_ROOT.equals(node.getName())
        && !node.isMeasurement()
        && !(node.isDevice() && node.getAsDeviceMNode().isUseTemplate())
        && node.getChildren().isEmpty();
  }

  public List<PartialPath> constructSchemaBlackList(
      final PartialPath pathPattern, final AtomicBoolean isAllLogicalView)
      throws MetadataException {
    final List<PartialPath> result = new ArrayList<>();
    try (final MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected void updateMeasurement(final IMeasurementMNode<IMemMNode> node) {
            if (!node.isLogicalView()) {
              isAllLogicalView.set(false);
            }
            node.setPreDeleted(true);
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> rollbackSchemaBlackList(final PartialPath pathPattern)
      throws MetadataException {
    final List<PartialPath> result = new ArrayList<>();
    try (final MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected void updateMeasurement(final IMeasurementMNode<IMemMNode> node) {
            node.setPreDeleted(false);
            result.add(getPartialPathFromRootToNode(node.getAsMNode()));
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> getPreDeletedTimeSeries(final PartialPath pathPattern)
      throws MetadataException {
    final List<PartialPath> result = new LinkedList<>();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(final IMeasurementMNode<IMemMNode> node) {
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

  // TODO: seems useless
  public Set<PartialPath> getDevicesOfPreDeletedTimeSeries(final PartialPath pathPattern)
      throws MetadataException {
    final Set<PartialPath> result = new HashSet<>();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(final IMeasurementMNode<IMemMNode> node) {
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

  public void setAlias(final IMeasurementMNode<IMemMNode> measurementMNode, final String alias)
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
  public IMemMNode getDeviceNodeWithAutoCreating(final PartialPath deviceId)
      throws MetadataException {
    MetaFormatUtils.checkTimeseries(deviceId);
    final String[] nodeNames = deviceId.getNodes();
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

  /**
   * Check if the device node exists
   *
   * @param deviceId full path of device
   * @return true if the device node exists
   */
  public boolean checkDeviceNodeExists(final PartialPath deviceId) {
    final IMemMNode deviceMNode;
    try {
      deviceMNode = getNodeByPath(deviceId);
      return deviceMNode.isDevice();
    } catch (final MetadataException e) {
      return false;
    }
  }

  // endregion

  // region Interfaces and Implementation for metadata info Query
  public ClusterSchemaTree fetchSchema(
      final PartialPath pathPattern,
      final Map<Integer, Template> templateMap,
      final boolean withTags,
      final boolean withAttributes,
      final boolean withTemplate,
      final boolean withAliasForce)
      throws MetadataException {
    final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            final IDeviceMNode<IMemMNode> deviceMNode =
                getParentOfNextMatchedNode().getAsDeviceMNode();
            final int templateId = deviceMNode.getSchemaTemplateIdWithState();
            if (withTemplate && templateId >= 0) {
              schemaTree.appendTemplateDevice(
                  deviceMNode.getPartialPath(), deviceMNode.isAligned(), templateId, null);
              skipTemplateChildren(deviceMNode);
            } else {
              final MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
              schemaTree.appendSingleMeasurement(
                  path,
                  path.getMeasurementSchema(),
                  withTags ? tagGetter.apply(node) : null,
                  withAttributes ? attributeGetter.apply(node) : null,
                  withAliasForce || nodes[nodes.length - 1].equals(node.getAlias())
                      ? node.getAlias()
                      : null,
                  deviceMNode.isAligned());
            }
            return null;
          }
        }) {
      collector.setTemplateMap(templateMap, nodeFactory);
      collector.setSkipPreDeletedSchema(true);
      collector.traverse();
    }
    return schemaTree;
  }

  public ClusterSchemaTree fetchSchemaWithoutWildcard(
      final PathPatternTree patternTree,
      final Map<Integer, Template> templateMap,
      final boolean withTags,
      final boolean withAttributes,
      final boolean withTemplate)
      throws MetadataException {
    final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode, patternTree, store, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(IMeasurementMNode<IMemMNode> node) {
            final IDeviceMNode<IMemMNode> deviceMNode =
                getParentOfNextMatchedNode().getAsDeviceMNode();
            final int templateId = deviceMNode.getSchemaTemplateIdWithState();
            if (withTemplate && templateId >= 0) {
              schemaTree.appendTemplateDevice(
                  deviceMNode.getPartialPath(), deviceMNode.isAligned(), templateId, null);
              skipTemplateChildren(deviceMNode);
            } else {
              final MeasurementPath path = getCurrentMeasurementPathInTraverse(node);
              schemaTree.appendSingleMeasurement(
                  path,
                  path.getMeasurementSchema(),
                  withTags ? tagGetter.apply(node) : null,
                  withAttributes ? attributeGetter.apply(node) : null,
                  node.getAlias(),
                  deviceMNode.isAligned());
            }
            return null;
          }
        }) {
      collector.setTemplateMap(templateMap, nodeFactory);
      collector.setSkipPreDeletedSchema(true);
      collector.traverse();
    }
    return schemaTree;
  }

  public ClusterSchemaTree fetchDeviceSchema(
      final PathPatternTree patternTree, final PathPatternTree authorityScope)
      throws MetadataException {
    final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    for (final PartialPath pattern : patternTree.getAllPathPatterns()) {
      try (final EntityCollector<Void, IMemMNode> collector =
          new EntityCollector<Void, IMemMNode>(rootNode, pattern, store, false, authorityScope) {
            @Override
            protected Void collectEntity(final IDeviceMNode<IMemMNode> node) {
              if (node.isAlignedNullable() != null) {
                schemaTree.appendTemplateDevice(
                    node.getPartialPath(), node.isAligned(), node.getSchemaTemplateId(), null);
              }
              return null;
            }
          }) {
        collector.traverse();
      }
    }
    return schemaTree;
  }

  // endregion

  // region Interfaces and Implementation for MNode Query
  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  public IMemMNode getNodeByPath(final PartialPath path) throws PathNotExistException {
    final String[] nodes = path.getNodes();
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

  public IMeasurementMNode<IMemMNode> getMeasurementMNode(final PartialPath path)
      throws MetadataException {
    final IMemMNode node = getNodeByPath(path);
    if (node.isMeasurement()) {
      return node.getAsMeasurementMNode();
    } else {
      throw new MNodeTypeMismatchException(
          path.getFullPath(), SchemaConstant.MEASUREMENT_MNODE_TYPE);
    }
  }

  // endregion

  // region Interfaces and Implementation for Template check and query

  public void activateTemplate(final PartialPath activatePath, final Template template)
      throws MetadataException {
    final String[] nodes = activatePath.getNodes();
    IMemMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    final IDeviceMNode<IMemMNode> entityMNode;

    synchronized (this) {
      if (cur.isDevice()) {
        entityMNode = cur.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(cur);
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
      final Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    final Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (final Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (final EntityUpdater<?> updater =
          new EntityUpdater<IMemMNode>(
              rootNode, entry.getKey(), store, false, SchemaConstant.ALL_MATCH_SCOPE) {

            @Override
            protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.preDeactivateSelfOrTemplate();
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      final Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    final Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (final Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (final EntityUpdater<IMemMNode> updater =
          new EntityUpdater<IMemMNode>(
              rootNode, entry.getKey(), store, false, SchemaConstant.ALL_MATCH_SCOPE) {

            @Override
            protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateSelfOrTemplate()) {
                resultTemplateSetInfo.put(
                    node.getPartialPath(), Collections.singletonList(node.getSchemaTemplateId()));
                node.rollbackPreDeactivateSelfOrTemplate();
              }
            }
          }) {
        updater.update();
      }
    }
    return resultTemplateSetInfo;
  }

  public Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      final Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException {
    final Map<PartialPath, List<Integer>> resultTemplateSetInfo = new HashMap<>();
    for (final Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      try (final EntityUpdater<?> collector =
          new EntityUpdater<IMemMNode>(
              rootNode, entry.getKey(), store, false, SchemaConstant.ALL_MATCH_SCOPE) {

            @Override
            protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
              if (entry.getValue().contains(node.getSchemaTemplateId())
                  && node.isPreDeactivateSelfOrTemplate()) {
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
      final PartialPath activatePath, final int templateId, final boolean isAligned) {
    final String[] nodes = activatePath.getNodes();
    IMemMNode cur = storageGroupMNode;
    for (int i = levelOfSG + 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    final IDeviceMNode<IMemMNode> entityMNode;
    if (cur.isDevice()) {
      entityMNode = cur.getAsDeviceMNode();
    } else {
      entityMNode = store.setToEntity(cur);
    }

    if (!entityMNode.isAligned()) {
      entityMNode.setAligned(isAligned);
    }
    entityMNode.setUseTemplate(true);
    entityMNode.setSchemaTemplateId(templateId);
    regionStatistics.activateTemplate(templateId);
  }

  public long countPathsUsingTemplate(final PartialPath pathPattern, final int templateId)
      throws MetadataException {
    try (final EntityCounter<IMemMNode> counter =
        new EntityCounter<>(rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE)) {
      counter.setSchemaTemplateFilter(templateId);
      return counter.count();
    }
  }

  // endregion

  // region Interfaces for schema reader

  @SuppressWarnings("java:S2095")
  public ISchemaReader<IDeviceSchemaInfo> getDeviceReader(
      final IShowDevicesPlan showDevicesPlan,
      final BiFunction<Integer, String, Binary> attributeProvider)
      throws MetadataException {
    final EntityCollector<IDeviceSchemaInfo, IMemMNode> collector =
        new EntityCollector<IDeviceSchemaInfo, IMemMNode>(
            rootNode,
            showDevicesPlan.getPath(),
            store,
            showDevicesPlan.isPrefixMatch(),
            showDevicesPlan.getScope()) {

          @Override
          protected IDeviceSchemaInfo collectEntity(final IDeviceMNode<IMemMNode> node) {
            final PartialPath device = getPartialPathFromRootToNode(node.getAsMNode());
            final ShowDevicesResult result =
                new ShowDevicesResult(
                    device.getFullPath(), node.isAlignedNullable(), node.getSchemaTemplateId());
            result.setAttributeProvider(
                k ->
                    attributeProvider.apply(
                        ((TableDeviceInfo<IMemMNode>) node.getDeviceInfo()).getAttributePointer(),
                        k));
            return result;
          }
        };
    if (showDevicesPlan.usingSchemaTemplate()) {
      collector.setSchemaTemplateFilter(showDevicesPlan.getSchemaTemplateId());
    }
    final ISchemaReader<IDeviceSchemaInfo> reader =
        new ISchemaReader<IDeviceSchemaInfo>() {

          private final DeviceFilterVisitor filterVisitor = new DeviceFilterVisitor();
          private IDeviceSchemaInfo next;

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
          public ListenableFuture<?> isBlocked() {
            return NOT_BLOCKED;
          }

          @Override
          public boolean hasNext() {
            while (next == null && collector.hasNext()) {
              IDeviceSchemaInfo temp = collector.next();
              if (showDevicesPlan.getSchemaFilter() == null
                  || filterVisitor.process(showDevicesPlan.getSchemaFilter(), temp)) {
                next = temp;
              }
            }
            return next != null;
          }

          @Override
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

  // Used for device query/fetch with filters during show device or table query
  public ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(
      final PartialPath pattern, final BiFunction<Integer, String, Binary> attributeProvider)
      throws MetadataException {

    final EntityCollector<IDeviceSchemaInfo, IMemMNode> collector =
        new EntityCollector<IDeviceSchemaInfo, IMemMNode>(rootNode, pattern, store, false, null) {
          @Override
          protected boolean acceptFullMatchedNode(final IMemMNode node) {
            return node.isDevice() && !node.getAsDeviceMNode().isPreDeactivateSelfOrTemplate();
          }

          @Override
          protected IDeviceSchemaInfo collectEntity(final IDeviceMNode<IMemMNode> node) {
            final ShowDevicesResult result =
                new ShowDevicesResult(
                    null,
                    node.isAlignedNullable(),
                    node.getSchemaTemplateId(),
                    node.getPartialPath().getNodes());
            result.setAttributeProvider(
                k ->
                    attributeProvider.apply(
                        ((TableDeviceInfo<IMemMNode>) node.getDeviceInfo()).getAttributePointer(),
                        k));
            return result;
          }
        };
    return new ISchemaReader<IDeviceSchemaInfo>() {

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
      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      @Override
      public boolean hasNext() {
        return collector.hasNext();
      }

      @Override
      public IDeviceSchemaInfo next() {
        return collector.next();
      }
    };
  }

  // used for device fetch with explicit device id/path during table insertion
  public ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(
      final String table,
      final List<Object[]> devicePathList,
      final BiFunction<Integer, String, Binary> attributeProvider) {
    return new ISchemaReader<IDeviceSchemaInfo>() {

      final Iterator<Object[]> deviceIdIterator = devicePathList.listIterator();

      IDeviceSchemaInfo next = null;

      Throwable t = null;

      @Override
      public boolean isSuccess() {
        return t == null;
      }

      @Override
      public Throwable getFailure() {
        return t;
      }

      @Override
      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      @Override
      public boolean hasNext() {
        if (next == null) {
          tryGetNext();
        }
        return next != null;
      }

      @Override
      public IDeviceSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final IDeviceSchemaInfo result = next;
        next = null;
        return result;
      }

      private void tryGetNext() {
        while (deviceIdIterator.hasNext()) {
          try {
            final IMemMNode node = getTableDeviceNode(table, deviceIdIterator.next());

            if (!node.isDevice() || node.getAsDeviceMNode().isPreDeactivateSelfOrTemplate()) {
              continue;
            }
            final IDeviceMNode<IMemMNode> deviceNode = node.getAsDeviceMNode();
            final ShowDevicesResult result =
                new ShowDevicesResult(
                    null,
                    deviceNode.isAlignedNullable(),
                    deviceNode.getSchemaTemplateId(),
                    deviceNode.getPartialPath().getNodes());
            result.setAttributeProvider(
                k ->
                    attributeProvider.apply(
                        ((TableDeviceInfo<IMemMNode>) deviceNode.getDeviceInfo())
                            .getAttributePointer(),
                        k));
            next = result;
            break;
          } catch (final PathNotExistException e) {
            // Do nothing
          } catch (final Throwable e) {
            t = e;
            return;
          }
        }
      }

      @Override
      public void close() {}
    };
  }

  private IMemMNode getTableDeviceNode(final String table, final Object[] deviceId)
      throws PathNotExistException {
    IMemMNode cur = storageGroupMNode;
    IMemMNode next;

    next = cur.getChild(table);
    if (next == null) {
      throw new PathNotExistException(
          storageGroupMNode.getFullPath() + PATH_SEPARATOR + table + Arrays.toString(deviceId),
          true);
    } else if (next.isMeasurement()) {
      throw new PathNotExistException(
          storageGroupMNode.getFullPath() + PATH_SEPARATOR + table + Arrays.toString(deviceId),
          true);
    }
    cur = next;

    for (int i = 0; i < deviceId.length; i++) {
      next = cur.getChild(deviceId[i] == null ? null : String.valueOf(deviceId[i]));
      if (next == null) {
        throw new PathNotExistException(
            storageGroupMNode.getFullPath() + PATH_SEPARATOR + table + Arrays.toString(deviceId),
            true);
      } else if (next.isMeasurement()) {
        if (i == deviceId.length - 1) {
          return next;
        } else {
          throw new PathNotExistException(
              storageGroupMNode.getFullPath() + PATH_SEPARATOR + table + Arrays.toString(deviceId),
              true);
        }
      }
      cur = next;
    }
    return cur;
  }

  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(
      final IShowTimeSeriesPlan showTimeSeriesPlan,
      final Function<Long, Pair<Map<String, String>, Map<String, String>>> tagAndAttributeProvider)
      throws MetadataException {
    final MeasurementCollector<ITimeSeriesSchemaInfo, IMemMNode> collector =
        new MeasurementCollector<ITimeSeriesSchemaInfo, IMemMNode>(
            rootNode,
            showTimeSeriesPlan.getPath(),
            store,
            showTimeSeriesPlan.isPrefixMatch(),
            showTimeSeriesPlan.getScope()) {

          @Override
          protected ITimeSeriesSchemaInfo collectMeasurement(
              final IMeasurementMNode<IMemMNode> node) {
            return new ITimeSeriesSchemaInfo() {

              private Pair<Map<String, String>, Map<String, String>> tagAndAttribute = null;

              @Override
              public String getAlias() {
                return node.getAlias();
              }

              @Override
              public IMeasurementSchema getSchema() {
                return node.getSchema();
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
              public boolean isLogicalView() {
                return node.isLogicalView();
              }

              @Override
              public String getFullPath() {
                return getPartialPathFromRootToNode(node.getAsMNode()).getFullPath();
              }

              @Override
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
    final ISchemaReader<ITimeSeriesSchemaInfo> reader =
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
  public ISchemaReader<INodeSchemaInfo> getNodeReader(final IShowNodesPlan showNodesPlan)
      throws MetadataException {
    final MNodeCollector<INodeSchemaInfo, IMemMNode> collector =
        new MNodeCollector<INodeSchemaInfo, IMemMNode>(
            rootNode,
            showNodesPlan.getPath(),
            store,
            showNodesPlan.isPrefixMatch(),
            showNodesPlan.getScope()) {

          @Override
          protected INodeSchemaInfo collectMNode(final IMemMNode node) {
            return new ShowNodesResult(
                getPartialPathFromRootToNode(node).getFullPath(), node.getMNodeType());
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
      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      @Override
      public boolean hasNext() {
        return collector.hasNext();
      }

      @Override
      public INodeSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return collector.next();
      }
    };
  }

  // endregion

  // region interfaces for logical view
  public IMeasurementMNode<IMemMNode> createLogicalView(
      final PartialPath path, final ViewExpression viewExpression) throws MetadataException {
    // check path
    final String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2) {
      throw new IllegalPathException(path.getFullPath());
    }
    MetaFormatUtils.checkTimeseries(path);
    final PartialPath devicePath = path.getDevicePath();
    final IMemMNode deviceParent = checkAndAutoCreateInternalPath(devicePath);

    synchronized (this) {
      final String leafName = path.getMeasurement();
      final IMeasurementMNode<IMemMNode> measurementMNode =
          nodeFactory.createLogicalViewMNode(
              null, leafName, new LogicalViewSchema(leafName, viewExpression));
      final IMemMNode device = checkAndAutoCreateDeviceNode(devicePath.getTailNode(), deviceParent);

      // no need to check alias, because logical view has no alias

      if (device.hasChild(leafName)) {
        final IMemMNode node = device.getChild(leafName);
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

      final IDeviceMNode<IMemMNode> entityMNode;
      if (device.isDevice()) {
        entityMNode = device.getAsDeviceMNode();
      } else {
        entityMNode = store.setToEntity(device);
        // this parent has no measurement before. The leafName is his first child who is a logical
        // view.
        entityMNode.setAligned(null);
      }

      measurementMNode.setParent(entityMNode.getAsMNode());
      store.addChild(entityMNode.getAsMNode(), leafName, measurementMNode.getAsMNode());

      return measurementMNode;
    }
  }

  public List<PartialPath> constructLogicalViewBlackList(final PartialPath pathPattern)
      throws MetadataException {
    final List<PartialPath> result = new ArrayList<>();
    try (final MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected void updateMeasurement(final IMeasurementMNode<IMemMNode> node) {
            if (node.isLogicalView()) {
              node.setPreDeleted(true);
              result.add(getPartialPathFromRootToNode(node.getAsMNode()));
            }
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> rollbackLogicalViewBlackList(final PartialPath pathPattern)
      throws MetadataException {
    final List<PartialPath> result = new ArrayList<>();
    try (final MeasurementUpdater<IMemMNode> updater =
        new MeasurementUpdater<IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected void updateMeasurement(final IMeasurementMNode<IMemMNode> node) {
            if (node.isLogicalView()) {
              node.setPreDeleted(false);
              result.add(getPartialPathFromRootToNode(node.getAsMNode()));
            }
          }
        }) {
      updater.update();
    }
    return result;
  }

  public List<PartialPath> getPreDeletedLogicalView(final PartialPath pathPattern)
      throws MetadataException {
    final List<PartialPath> result = new LinkedList<>();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode, pathPattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {

          @Override
          protected Void collectMeasurement(final IMeasurementMNode<IMemMNode> node) {
            if (node.isLogicalView() && node.isPreDeleted()) {
              result.add(getPartialPathFromRootToNode(node.getAsMNode()));
            }
            return null;
          }
        }) {
      collector.traverse();
    }
    return result;
  }

  // endregion

  // region table device management

  public int getTableDeviceNotExistNum(final String tableName, final List<Object[]> deviceIdList) {
    final IMemMNode tableNode = storageGroupMNode.getChild(tableName);
    int notExistNum = deviceIdList.size();
    if (tableNode == null) {
      return notExistNum;
    }
    IMemMNode cur;
    for (final Object[] deviceId : deviceIdList) {
      cur = tableNode;
      for (final Object device : deviceId) {
        cur = cur.getChild(Objects.nonNull(device) ? device.toString() : null);
        if (cur == null) {
          break;
        }
      }
      if (Objects.nonNull(cur)) {
        notExistNum--;
      }
    }
    return notExistNum;
  }

  public void createOrUpdateTableDevice(
      final String tableName,
      final String[] devicePath,
      final IntSupplier attributePointerGetter,
      final IntConsumer attributeUpdater)
      throws MetadataException {
    // todo implement storage for device of diverse data types\
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Start to create table device {}.{}", tableName, Arrays.toString(devicePath));
    }
    IMemMNode cur = storageGroupMNode.getChild(tableName);
    if (cur == null) {
      cur =
          store.addChild(
              storageGroupMNode, tableName, nodeFactory.createInternalMNode(cur, tableName));
    }

    for (final String childName : devicePath) {
      IMemMNode child = cur.getChild(childName);
      if (child == null) {
        child = store.addChild(cur, childName, nodeFactory.createInternalMNode(cur, childName));
      }
      cur = child;
    }

    final IDeviceMNode<IMemMNode> entityMNode;

    synchronized (this) {
      if (cur.isDevice()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Table device {}.{} already exists", tableName, Arrays.toString(devicePath));
        }
        entityMNode = cur.getAsDeviceMNode();
        if (!(entityMNode.getDeviceInfo() instanceof TableDeviceInfo)) {
          throw new MetadataException("Table device shall not create under tree model");
        }
        final TableDeviceInfo<IMemMNode> deviceInfo =
            (TableDeviceInfo<IMemMNode>) entityMNode.getDeviceInfo();
        attributeUpdater.accept(deviceInfo.getAttributePointer());
      } else {
        entityMNode = store.setToEntity(cur);
        final TableDeviceInfo<IMemMNode> deviceInfo = new TableDeviceInfo<>();
        deviceInfo.setAttributePointer(attributePointerGetter.getAsInt());
        entityMNode.getAsInternalMNode().setDeviceInfo(deviceInfo);
        regionStatistics.addTableDevice(tableName);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Table device {}.{} created", tableName, Arrays.toString(devicePath));
        }
      }
    }
  }

  public void updateTableDevice(
      final PartialPath pattern, final DeviceAttributeUpdater batchUpdater)
      throws MetadataException {
    try (final EntityUpdater<IMemMNode> updater =
        new EntityUpdater<IMemMNode>(
            rootNode, pattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {

          @Override
          protected void updateEntity(final IDeviceMNode<IMemMNode> node) throws MetadataException {
            batchUpdater.handleDeviceNode(node);
          }
        }) {
      updater.update();
    }
  }

  public void constructTableDeviceBlackList(
      final PartialPath pattern, final DeviceBlackListConstructor blackListConstructor)
      throws MetadataException {
    try (final EntityUpdater<IMemMNode> updater =
        new EntityUpdater<IMemMNode>(
            rootNode, pattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {

          @Override
          protected void updateEntity(final IDeviceMNode<IMemMNode> node) throws MetadataException {
            blackListConstructor.handleDeviceNode(node);
          }
        }) {
      updater.update();
    }
  }

  public void rollbackTableDeviceBlackList(final PartialPath pattern) throws MetadataException {
    try (final EntityUpdater<IMemMNode> updater =
        new EntityUpdater<IMemMNode>(
            rootNode, pattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {

          @Override
          protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
            if (node.isPreDeactivateSelfOrTemplate()) {
              regionStatistics.addTableDevice(pattern.getNodes()[2]);
              node.rollbackPreDeactivateSelfOrTemplate();
            }
          }
        }) {
      updater.update();
    }
  }

  public void deleteTableDevicesInBlackList(
      final PartialPath pattern,
      final IntConsumer attributeDeleter,
      final Consumer<String[]> deviceAttributeCacheUpdateInvalidator)
      throws MetadataException {
    try (final EntityUpdater<IMemMNode> updater =
        new EntityUpdater<IMemMNode>(
            rootNode, pattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {

          @Override
          protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
            if (node.isPreDeactivateSelfOrTemplate()) {
              attributeDeleter.accept(
                  ((TableDeviceInfo<IMemMNode>) node.getAsDeviceMNode().getDeviceInfo())
                      .getAttributePointer());
              deviceAttributeCacheUpdateInvalidator.accept(node.getPartialPath().getNodes());
              deleteEmptyInternalMNode(node);
            }
          }
        }) {
      updater.update();
    }
  }

  public boolean deleteTableDevice(final String tableName, final IntConsumer attributeDeleter)
      throws MetadataException {
    if (!store.hasChild(storageGroupMNode, tableName)) {
      return false;
    }
    final AtomicInteger memoryReleased = new AtomicInteger(0);
    try (final MNodeCollector<Void, IMemMNode> collector =
        new MNodeCollector<Void, IMemMNode>(
            storageGroupMNode,
            new PartialPath(new String[] {storageGroupMNode.getName(), tableName}),
            this.store,
            true,
            SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected boolean acceptInternalMatchedNode(final IMemMNode node) {
            return true;
          }

          @Override
          protected Void collectMNode(final IMemMNode node) {
            if (node.isDevice()) {
              attributeDeleter.accept(
                  ((TableDeviceInfo<IMemMNode>) node.getAsDeviceMNode().getDeviceInfo())
                      .getAttributePointer());
            }
            memoryReleased.addAndGet(node.estimateSize());
            return null;
          }
        }) {
      collector.traverse();
    }
    storageGroupMNode.deleteChild(tableName);
    regionStatistics.resetTableDevice(tableName);
    store.releaseMemory(memoryReleased.get());
    return true;
  }

  public boolean dropTableAttribute(final String tableName, final IntConsumer attributeDropper)
      throws MetadataException {
    if (!store.hasChild(storageGroupMNode, tableName)) {
      return false;
    }
    final AtomicInteger memoryReleased = new AtomicInteger(0);
    try (final EntityUpdater<IMemMNode> updater =
        new EntityUpdater<IMemMNode>(
            storageGroupMNode,
            new PartialPath(new String[] {storageGroupMNode.getName(), tableName}),
            this.store,
            true,
            SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected void updateEntity(final IDeviceMNode<IMemMNode> node) {
            attributeDropper.accept(
                ((TableDeviceInfo<IMemMNode>) node.getAsDeviceMNode().getDeviceInfo())
                    .getAttributePointer());
          }
        }) {
      updater.update();
    }
    store.releaseMemory(memoryReleased.get());
    return true;
  }

  // endregion
}
