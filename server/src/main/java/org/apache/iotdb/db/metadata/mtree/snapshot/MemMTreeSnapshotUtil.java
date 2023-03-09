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

package org.apache.iotdb.db.metadata.mtree.snapshot;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseDeviceMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDeviceMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.mem.IMemMNode;
import org.apache.iotdb.db.metadata.mnode.mem.factory.MemMNodeFactory;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

import static org.apache.iotdb.db.metadata.MetadataConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.isStorageGroupType;

public class MemMTreeSnapshotUtil {

  private static final Logger logger = LoggerFactory.getLogger(MemMTreeSnapshotUtil.class);
  private static final String SERIALIZE_ERROR_INFO = "Error occurred during serializing MemMTree.";
  private static final String DESERIALIZE_ERROR_INFO =
      "Error occurred during deserializing MemMTree.";

  private static final byte VERSION = 0;
  private static final IMNodeFactory<IMemMNode> nodeFactory = MemMNodeFactory.getInstance();

  public static boolean createSnapshot(File snapshotDir, MemMTreeStore store) {
    File snapshotTmp =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, MetadataConstant.MTREE_SNAPSHOT_TMP);
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, MetadataConstant.MTREE_SNAPSHOT);

    try {
      try (BufferedOutputStream outputStream =
          new BufferedOutputStream(new FileOutputStream(snapshotTmp))) {
        serializeTo(store, outputStream);
      }
      if (snapshot.exists() && !snapshot.delete()) {
        logger.error(
            "Failed to delete old snapshot {} while creating mtree snapshot.", snapshot.getName());
        return false;
      }
      if (!snapshotTmp.renameTo(snapshot)) {
        logger.error(
            "Failed to rename {} to {} while creating mtree snapshot.",
            snapshotTmp.getName(),
            snapshot.getName());
        snapshot.delete();
        return false;
      }

      return true;
    } catch (IOException e) {
      logger.error("Failed to create mtree snapshot due to {}", e.getMessage(), e);
      snapshot.delete();
      return false;
    } finally {
      snapshotTmp.delete();
    }
  }

  public static IMemMNode loadSnapshot(
      File snapshotDir,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, MetadataConstant.MTREE_SNAPSHOT);
    try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(snapshot))) {
      return deserializeFrom(inputStream, measurementProcess, regionStatistics);
    } catch (Throwable e) {
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
      InputStream inputStream,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    byte version = ReadWriteIOUtils.readByte(inputStream);
    return inorderDeserialize(inputStream, measurementProcess, regionStatistics);
  }

  private static IMemMNode inorderDeserialize(
      InputStream inputStream,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    MNodeDeserializer deserializer = new MNodeDeserializer();
    Deque<IMemMNode> ancestors = new ArrayDeque<>();
    Deque<Integer> restChildrenNum = new ArrayDeque<>();
    deserializeMNode(
        ancestors,
        restChildrenNum,
        deserializer,
        inputStream,
        measurementProcess,
        regionStatistics);
    int childrenNum;
    IMemMNode root = ancestors.peek();
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
            regionStatistics);
      }
    }
    return root;
  }

  private static void deserializeMNode(
      Deque<IMemMNode> ancestors,
      Deque<Integer> restChildrenNum,
      MNodeDeserializer deserializer,
      InputStream inputStream,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
      MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    IMemMNode node;
    int childrenNum;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeInternalMNode(inputStream);
        break;
      case STORAGE_GROUP_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupMNode(inputStream);
        break;
      case ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeEntityMNode(inputStream);
        break;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupEntityMNode(inputStream);
        break;
      case MEASUREMENT_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeMeasurementMNode(inputStream);
        measurementProcess.accept(node.getAsMeasurementMNode());
        break;
      default:
        throw new IOException("Unrecognized MNode type " + type);
    }

    regionStatistics.requestMemory(node.estimateSize());

    if (!ancestors.isEmpty()) {
      node.setParent(ancestors.peek());
      ancestors.peek().addChild(node);
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
        ReadWriteIOUtils.write(INTERNAL_MNODE_TYPE, outputStream);
        serializeBasicMNode(node, outputStream);
        ReadWriteIOUtils.write(0, outputStream); // for compatibly
        ReadWriteIOUtils.write(false, outputStream); // for compatibly
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
        ReadWriteIOUtils.write(STORAGE_GROUP_MNODE_TYPE, outputStream);
        serializeBasicMNode(node.getBasicMNode(), outputStream);
        ReadWriteIOUtils.write(0, outputStream); // for compatibly
        ReadWriteIOUtils.write(false, outputStream); // for compatibly
        // database node in schemaRegion doesn't store any database schema
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitDatabaseDeviceMNode(
        AbstractDatabaseDeviceMNode<?, ? extends IMNode<?>> node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(STORAGE_GROUP_ENTITY_MNODE_TYPE, outputStream);
        serializeBasicMNode(node.getBasicMNode(), outputStream);
        ReadWriteIOUtils.write(node.getSchemaTemplateIdWithState(), outputStream);
        ReadWriteIOUtils.write(node.isUseTemplate(), outputStream);
        ReadWriteIOUtils.write(node.isAligned(), outputStream);
        // database node in schemaRegion doesn't store any database schema
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitDeviceMNode(
        AbstractDeviceMNode<?, ? extends IMNode<?>> node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(ENTITY_MNODE_TYPE, outputStream);
        serializeBasicMNode(node.getBasicMNode(), outputStream);
        ReadWriteIOUtils.write(node.getSchemaTemplateIdWithState(), outputStream);
        ReadWriteIOUtils.write(node.isUseTemplate(), outputStream);
        ReadWriteIOUtils.write(node.isAligned(), outputStream);
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitMeasurementMNode(
        AbstractMeasurementMNode<?, ? extends IMNode<?>> node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(MEASUREMENT_MNODE_TYPE, outputStream);
        ReadWriteIOUtils.write(node.getName(), outputStream);
        node.getSchema().serializeTo(outputStream);
        ReadWriteIOUtils.write(node.getAlias(), outputStream);
        ReadWriteIOUtils.write(node.getOffset(), outputStream);
        ReadWriteIOUtils.write(node.isPreDeleted(), outputStream);
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    private void serializeBasicMNode(IMNode<?> node, OutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      ReadWriteIOUtils.write(node.getName(), outputStream);
    }
  }

  private static class MNodeDeserializer {

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
      IMemMNode node = nodeFactory.createDatabaseDeviceMNode(null, name, 0);
      node.getAsDeviceMNode().setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
      node.getAsDeviceMNode().setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
      node.getAsDeviceMNode().setAligned(ReadWriteIOUtils.readBool(inputStream));
      return node;
    }

    public IMemMNode deserializeEntityMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      IDeviceMNode<IMemMNode> node = nodeFactory.createDeviceMNode(null, name);
      node.setSchemaTemplateId(ReadWriteIOUtils.readInt(inputStream));
      node.setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
      node.setAligned(ReadWriteIOUtils.readBool(inputStream));
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
  }
}
