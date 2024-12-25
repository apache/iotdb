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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.MemMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.iotdb.commons.schema.SchemaConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.LOGICAL_VIEW_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.TABLE_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.isStorageGroupType;

public class MemMTreeSnapshotUtil {

  private static final Logger logger = LoggerFactory.getLogger(MemMTreeSnapshotUtil.class);
  private static final String SERIALIZE_ERROR_INFO = "Error occurred during serializing MemMTree.";
  private static final String DESERIALIZE_ERROR_INFO =
      "Error occurred during deserializing MemMTree.";

  private static final byte VERSION = 0;
  private static final IMNodeFactory<IMemMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();

  public static boolean createSnapshot(final File snapshotDir, final MemMTreeStore store) {
    final File snapshotTmp =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.MTREE_SNAPSHOT_TMP);
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.MTREE_SNAPSHOT);

    try {
      final FileOutputStream fileOutputStream = new FileOutputStream(snapshotTmp);
      final BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
      try {
        serializeTo(store, outputStream);
      } finally {
        outputStream.flush();
        fileOutputStream.getFD().sync();
        outputStream.close();
      }
      if (snapshot.exists() && !FileUtils.deleteFileIfExist(snapshot)) {
        logger.error(
            "Failed to delete old snapshot {} while creating mTree snapshot.", snapshot.getName());
        return false;
      }
      if (!snapshotTmp.renameTo(snapshot)) {
        logger.error(
            "Failed to rename {} to {} while creating mTree snapshot.",
            snapshotTmp.getName(),
            snapshot.getName());
        FileUtils.deleteFileIfExist(snapshot);
        return false;
      }

      return true;
    } catch (final IOException e) {
      logger.error("Failed to create mTree snapshot due to {}", e.getMessage(), e);
      FileUtils.deleteFileIfExist(snapshot);
      return false;
    } finally {
      FileUtils.deleteFileIfExist(snapshotTmp);
    }
  }

  public static IMemMNode loadSnapshot(
      final File snapshotDir,
      final Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      final Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      final BiConsumer<IDeviceMNode<IMemMNode>, String> tableDeviceProcess,
      final MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    final File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.MTREE_SNAPSHOT);
    try (final BufferedInputStream inputStream =
        new BufferedInputStream(Files.newInputStream(snapshot.toPath()))) {
      return deserializeFrom(
          inputStream, measurementProcess, deviceProcess, tableDeviceProcess, regionStatistics);
    } catch (final Throwable e) {
      // This method is only invoked during recovery. If failed, the memory usage should be cleared
      // since the loaded schema will not be used.
      regionStatistics.clear();
      throw e;
    }
  }

  private static void serializeTo(MemMTreeStore store, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(VERSION, outputStream);
    inorderSerialize(store.getRoot(), store, outputStream);
  }

  private static void inorderSerialize(
      IMemMNode root, MemMTreeStore store, OutputStream outputStream) throws IOException {
    MNodeSerializer serializer = new MNodeSerializer();
    if (!root.accept(serializer, outputStream)) {
      throw new IOException(SERIALIZE_ERROR_INFO);
    }

    Deque<IMNodeIterator<IMemMNode>> stack = new ArrayDeque<>();
    stack.push(store.getChildrenIterator(root));
    IMemMNode node;
    IMNodeIterator<IMemMNode> iterator;
    while (!stack.isEmpty()) {
      iterator = stack.peek();
      if (iterator.hasNext()) {
        node = iterator.next();
        if (!node.accept(serializer, outputStream)) {
          throw new IOException(SERIALIZE_ERROR_INFO);
        }
        if (!node.isMeasurement()) {
          stack.push(store.getChildrenIterator(node));
        }
      } else {
        stack.pop();
      }
    }
  }

  private static IMemMNode deserializeFrom(
      final InputStream inputStream,
      final Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      final Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      final BiConsumer<IDeviceMNode<IMemMNode>, String> tableDeviceProcess,
      final MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    byte version = ReadWriteIOUtils.readByte(inputStream);
    return inorderDeserialize(
        inputStream, measurementProcess, deviceProcess, tableDeviceProcess, regionStatistics);
  }

  private static IMemMNode inorderDeserialize(
      final InputStream inputStream,
      final Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      final Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      final BiConsumer<IDeviceMNode<IMemMNode>, String> tableDeviceProcess,
      final MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    final MNodeDeserializer deserializer = new MNodeDeserializer();
    final Deque<IMemMNode> ancestors = new ArrayDeque<>();
    final Deque<Integer> restChildrenNum = new ArrayDeque<>();
    final AtomicReference<String> tableRef = new AtomicReference<>();
    deserializeMNode(
        ancestors,
        restChildrenNum,
        deserializer,
        inputStream,
        measurementProcess,
        deviceProcess,
        tableDeviceProcess,
        regionStatistics,
        tableRef);
    int childrenNum;
    final IMemMNode root = ancestors.peek();
    while (!ancestors.isEmpty()) {
      childrenNum = restChildrenNum.pop();
      if (childrenNum == 0) {
        ancestors.pop();
      } else {
        restChildrenNum.push(childrenNum - 1);
        deserializeMNode(
            ancestors,
            restChildrenNum,
            deserializer,
            inputStream,
            measurementProcess,
            deviceProcess,
            tableDeviceProcess,
            regionStatistics,
            tableRef);
      }
    }
    return root;
  }

  private static void deserializeMNode(
      final Deque<IMemMNode> ancestors,
      final Deque<Integer> restChildrenNum,
      final MNodeDeserializer deserializer,
      final InputStream inputStream,
      final Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      final Consumer<IDeviceMNode<IMemMNode>> deviceProcess,
      final BiConsumer<IDeviceMNode<IMemMNode>, String> tableDeviceProcess,
      final MemSchemaRegionStatistics regionStatistics,
      final AtomicReference<String> currentTableName)
      throws IOException {
    final byte type = ReadWriteIOUtils.readByte(inputStream);
    final IMemMNode node;
    final int childrenNum;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeInternalMNode(inputStream);
        if (ancestors.size() == 1) {
          currentTableName.set(node.getName());
        }
        break;
      case STORAGE_GROUP_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupMNode(inputStream);
        break;
      case ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeEntityMNode(inputStream);
        deviceProcess.accept(node.getAsDeviceMNode());
        break;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupEntityMNode(inputStream);
        deviceProcess.accept(node.getAsDeviceMNode());
        break;
      case MEASUREMENT_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeMeasurementMNode(inputStream);
        measurementProcess.accept(node.getAsMeasurementMNode());
        break;
      case LOGICAL_VIEW_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeLogicalViewMNode(inputStream);
        measurementProcess.accept(node.getAsMeasurementMNode());
        break;
      case TABLE_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeTableDeviceMNode(inputStream);
        if (ancestors.size() == 1) {
          currentTableName.set(node.getName());
        }
        deviceProcess.accept(node.getAsDeviceMNode());
        tableDeviceProcess.accept(node.getAsDeviceMNode(), currentTableName.get());
        break;
      default:
        throw new IOException("Unrecognized MNode type " + type);
    }

    regionStatistics.requestMemory(node.estimateSize());

    if (!ancestors.isEmpty()) {
      node.setParent(ancestors.peek());
      ancestors.peek().addChild(node);
      if (node.isMeasurement() && node.getAsMeasurementMNode().getAlias() != null) {
        ancestors
            .peek()
            .getAsDeviceMNode()
            .addAlias(node.getAsMeasurementMNode().getAlias(), node.getAsMeasurementMNode());
      }
    }

    // Storage type means current node is root node, so it must be returned.
    if (childrenNum > 0 || isStorageGroupType(type)) {
      ancestors.push(node);
      restChildrenNum.push(childrenNum);
    }
  }

  private static class MNodeSerializer extends MNodeVisitor<Boolean, OutputStream> {

    @Override
    public Boolean visitBasicMNode(IMNode<?> node, OutputStream outputStream) {
      try {
        if (node.isDevice()) {
          if (node.getAsDeviceMNode().getDeviceInfo() instanceof TableDeviceInfo) {
            ReadWriteIOUtils.write(TABLE_MNODE_TYPE, outputStream);
            TableDeviceInfo<IMemMNode> tableDeviceInfo =
                (TableDeviceInfo<IMemMNode>) (node.getAsDeviceMNode().getDeviceInfo());
            serializeBasicMNode(node, outputStream);
            ReadWriteIOUtils.write(tableDeviceInfo.getAttributePointer(), outputStream);
          } else {
            ReadWriteIOUtils.write(ENTITY_MNODE_TYPE, outputStream);
            serializeBasicMNode(node, outputStream);
            IDeviceMNode<?> deviceMNode = node.getAsDeviceMNode();
            ReadWriteIOUtils.write(deviceMNode.getSchemaTemplateIdWithState(), outputStream);
            ReadWriteIOUtils.write(deviceMNode.isUseTemplate(), outputStream);
            ReadWriteIOUtils.write(deviceMNode.isAlignedNullable(), outputStream);
          }
        } else {
          ReadWriteIOUtils.write(INTERNAL_MNODE_TYPE, outputStream);
          serializeBasicMNode(node, outputStream);
          ReadWriteIOUtils.write(0, outputStream); // for compatibly
          ReadWriteIOUtils.write(false, outputStream); // for compatibly
        }
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitDatabaseMNode(
        AbstractDatabaseMNode<?, ? extends IMNode<?>> node, OutputStream outputStream) {
      try {
        if (node.isDevice()) {
          ReadWriteIOUtils.write(STORAGE_GROUP_ENTITY_MNODE_TYPE, outputStream);
          serializeBasicMNode(node.getBasicMNode(), outputStream);
          IDeviceMNode<?> deviceMNode = node.getAsDeviceMNode();
          ReadWriteIOUtils.write(deviceMNode.getSchemaTemplateIdWithState(), outputStream);
          ReadWriteIOUtils.write(deviceMNode.isUseTemplate(), outputStream);
          ReadWriteIOUtils.write(deviceMNode.isAlignedNullable(), outputStream);
          // database node in schemaRegion doesn't store any database schema
          return true;
        } else {
          ReadWriteIOUtils.write(STORAGE_GROUP_MNODE_TYPE, outputStream);
          serializeBasicMNode(node.getBasicMNode(), outputStream);
          ReadWriteIOUtils.write(0, outputStream); // for compatibly
          ReadWriteIOUtils.write(false, outputStream); // for compatibly
          // database node in schemaRegion doesn't store any database schema
          return true;
        }
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitMeasurementMNode(
        AbstractMeasurementMNode<?, ? extends IMNode<?>> node, OutputStream outputStream) {
      try {
        if (node.isLogicalView()) {
          ReadWriteIOUtils.write(LOGICAL_VIEW_MNODE_TYPE, outputStream);
          ReadWriteIOUtils.write(node.getName(), outputStream);
          node.getSchema().serializeTo(outputStream);
          ReadWriteIOUtils.write(node.getOffset(), outputStream);
          ReadWriteIOUtils.write(node.isPreDeleted(), outputStream);
        } else {
          ReadWriteIOUtils.write(MEASUREMENT_MNODE_TYPE, outputStream);
          ReadWriteIOUtils.write(node.getName(), outputStream);
          node.getSchema().serializeTo(outputStream);
          ReadWriteIOUtils.write(node.getAlias(), outputStream);
          ReadWriteIOUtils.write(node.getOffset(), outputStream);
          ReadWriteIOUtils.write(node.isPreDeleted(), outputStream);
        }
        return true;
      } catch (Exception e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    private void serializeBasicMNode(IMNode<?> node, OutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      ReadWriteIOUtils.write(node.getName(), outputStream);
    }
  }

  public static class MNodeDeserializer {

    public IMemMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IMemMNode node = nodeFactory.createInternalMNode(null, name);
      int templateId = ReadWriteIOUtils.readInt(inputStream);
      boolean useTemplate = ReadWriteIOUtils.readBool(inputStream);
      return node;
    }

    public IMemMNode deserializeStorageGroupMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IMemMNode node = nodeFactory.createDatabaseMNode(null, name).getAsMNode();
      int templateId = ReadWriteIOUtils.readInt(inputStream);
      boolean useTemplate = ReadWriteIOUtils.readBool(inputStream);
      return node;
    }

    public IMemMNode deserializeStorageGroupEntityMNode(InputStream inputStream)
        throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IMemMNode node = nodeFactory.createDatabaseDeviceMNode(null, name);
      node.getAsDeviceMNode().setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
      node.getAsDeviceMNode().setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
      node.getAsDeviceMNode().setAligned(ReadWriteIOUtils.readBoolObject(inputStream));
      return node;
    }

    public IMemMNode deserializeEntityMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IDeviceMNode<IMemMNode> node = nodeFactory.createDeviceMNode(null, name);
      node.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
      node.setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
      node.setAligned(ReadWriteIOUtils.readBoolObject(inputStream));
      return node.getAsMNode();
    }

    public IMemMNode deserializeTableDeviceMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IDeviceMNode<IMemMNode> node = nodeFactory.createDeviceMNode(null, name);
      TableDeviceInfo<IMemMNode> tableDeviceInfo = new TableDeviceInfo<>();
      tableDeviceInfo.setAttributePointer(ReadWriteIOUtils.readInt(inputStream));
      node.getAsInternalMNode().setDeviceInfo(tableDeviceInfo);
      return node.getAsMNode();
    }

    public IMemMNode deserializeMeasurementMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      MeasurementSchema schema = MeasurementSchema.deserializeFrom(inputStream);
      String alias = ReadWriteIOUtils.readString(inputStream);
      long tagOffset = ReadWriteIOUtils.readLong(inputStream);
      IMeasurementMNode<IMemMNode> node =
          nodeFactory.createMeasurementMNode(null, name, schema, alias);
      node.setOffset(tagOffset);
      node.setPreDeleted(ReadWriteIOUtils.readBool(inputStream));
      return node.getAsMNode();
    }

    public IMemMNode deserializeLogicalViewMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      LogicalViewSchema logicalViewSchema = LogicalViewSchema.deserializeFrom(inputStream);
      long tagOffset = ReadWriteIOUtils.readLong(inputStream);
      IMeasurementMNode<IMemMNode> node =
          nodeFactory.createLogicalViewMNode(null, name, logicalViewSchema);
      node.setOffset(tagOffset);
      node.setPreDeleted(ReadWriteIOUtils.readBool(inputStream));
      return node.getAsMNode();
    }
  }
}
