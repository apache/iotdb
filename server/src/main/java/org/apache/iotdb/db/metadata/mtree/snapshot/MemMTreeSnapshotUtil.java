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
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.mtree.store.MemMTreeStore;
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

import static org.apache.iotdb.db.metadata.MetadataConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_MNODE_TYPE;

public class MemMTreeSnapshotUtil {

  private static final Logger logger = LoggerFactory.getLogger(MemMTreeSnapshotUtil.class);
  private static final String SERIALIZE_ERROR_INFO = "Error occurred during serializing MemMTree.";
  private static final String DESERIALIZE_ERROR_INFO =
      "Error occurred during deserializing MemMTree.";

  private static final byte VERSION = 0;

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

  public static IMNode loadSnapshot(File snapshotDir) throws IOException {
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, MetadataConstant.MTREE_SNAPSHOT);
    try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(snapshot))) {
      return deserializeFrom(inputStream);
    }
  }

  private static void serializeTo(MemMTreeStore store, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(VERSION, outputStream);
    dfsSerialize(store.getRoot(), store, outputStream);
  }

  private static void dfsSerialize(IMNode root, MemMTreeStore store, OutputStream outputStream)
      throws IOException {
    MNodeSerializer serializer = new MNodeSerializer();
    if (!root.accept(serializer, outputStream)) {
      throw new IOException(SERIALIZE_ERROR_INFO);
    }

    Deque<IMNodeIterator> stack = new ArrayDeque<>();
    stack.push(store.getChildrenIterator(root));
    IMNode node;
    IMNodeIterator iterator;
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

  private static IMNode deserializeFrom(InputStream inputStream) throws IOException {
    byte version = ReadWriteIOUtils.readByte(inputStream);
    return dfsDeserialize(inputStream);
  }

  private static IMNode dfsDeserialize(InputStream inputStream) throws IOException {
    MNodeDeserializer deserializer = new MNodeDeserializer();
    Deque<IMNode> ancestors = new ArrayDeque<>();
    Deque<Integer> restChildrenNum = new ArrayDeque<>();
    int childrenNum = deserializeMNode(ancestors, deserializer, inputStream);
    IMNode root = ancestors.peek();
    restChildrenNum.push(childrenNum);
    while (!ancestors.isEmpty()) {
      childrenNum = restChildrenNum.pop();
      if (childrenNum == 0) {
        ancestors.pop();
      } else {
        restChildrenNum.push(childrenNum - 1);
        childrenNum = deserializeMNode(ancestors, deserializer, inputStream);
        if (childrenNum > 0) {
          restChildrenNum.push(childrenNum);
        }
      }
    }
    return root;
  }

  private static int deserializeMNode(
      Deque<IMNode> ancestors, MNodeDeserializer deserializer, InputStream inputStream)
      throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    IMNode node;
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
        break;
      default:
        throw new IOException("Unrecognized MNode type " + type);
    }

    if (!ancestors.isEmpty()) {
      node.setParent(ancestors.peek());
      ancestors.peek().addChild(node);
    }
    ancestors.push(node);

    return childrenNum;
  }

  private static class MNodeSerializer extends MNodeVisitor<Boolean, OutputStream> {

    @Override
    public Boolean visitInternalMNode(InternalMNode node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(INTERNAL_MNODE_TYPE, outputStream);
        serializeInternalBasicInfo(node, outputStream);
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitStorageGroupMNode(StorageGroupMNode node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(STORAGE_GROUP_MNODE_TYPE, outputStream);
        serializeInternalBasicInfo(node, outputStream);
        // storage group node in schemaRegion doesn't store any storage group schema
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitStorageGroupEntityMNode(
        StorageGroupEntityMNode node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(STORAGE_GROUP_ENTITY_MNODE_TYPE, outputStream);
        serializeInternalBasicInfo(node, outputStream);
        ReadWriteIOUtils.write(node.isAligned(), outputStream);
        // storage group node in schemaRegion doesn't store any storage group schema
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitEntityMNode(EntityMNode node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(ENTITY_MNODE_TYPE, outputStream);
        serializeInternalBasicInfo(node, outputStream);
        ReadWriteIOUtils.write(node.isAligned(), outputStream);
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    @Override
    public Boolean visitMeasurementMNode(MeasurementMNode node, OutputStream outputStream) {
      try {
        ReadWriteIOUtils.write(MEASUREMENT_MNODE_TYPE, outputStream);
        ReadWriteIOUtils.write(node.getName(), outputStream);
        node.getSchema().serializeTo(outputStream);
        ReadWriteIOUtils.write(node.getAlias(), outputStream);
        ReadWriteIOUtils.write(node.getOffset(), outputStream);
        return true;
      } catch (IOException e) {
        logger.error(SERIALIZE_ERROR_INFO, e);
        return false;
      }
    }

    private void serializeInternalBasicInfo(InternalMNode node, OutputStream outputStream)
        throws IOException {
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      ReadWriteIOUtils.write(node.getName(), outputStream);
      ReadWriteIOUtils.write(node.getSchemaTemplate().hashCode(), outputStream);
      ReadWriteIOUtils.write(node.isUseTemplate(), outputStream);
    }
  }

  private static class MNodeDeserializer {

    public InternalMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      InternalMNode node = new InternalMNode(null, name);
      deserializeInternalBasicInfo(node, inputStream);
      return node;
    }

    public StorageGroupMNode deserializeStorageGroupMNode(InputStream inputStream)
        throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      StorageGroupMNode node = new StorageGroupMNode(null, name);
      deserializeInternalBasicInfo(node, inputStream);
      return node;
    }

    public StorageGroupEntityMNode deserializeStorageGroupEntityMNode(InputStream inputStream)
        throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      StorageGroupEntityMNode node = new StorageGroupEntityMNode(null, name, 0);
      deserializeInternalBasicInfo(node, inputStream);
      node.setAligned(ReadWriteIOUtils.readBool(inputStream));
      return node;
    }

    public EntityMNode deserializeEntityMNode(InputStream inputStream) throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      EntityMNode node = new EntityMNode(null, name);
      deserializeInternalBasicInfo(node, inputStream);
      node.setAligned(ReadWriteIOUtils.readBool(inputStream));
      return node;
    }

    public MeasurementMNode deserializeMeasurementMNode(InputStream inputStream)
        throws IOException {
      String name = ReadWriteIOUtils.readString(inputStream);
      MeasurementSchema schema = MeasurementSchema.deserializeFrom(inputStream);
      String alias = ReadWriteIOUtils.readString(inputStream);
      long tagOffset = ReadWriteIOUtils.readLong(inputStream);
      MeasurementMNode node = new MeasurementMNode(null, name, schema, alias);
      node.setOffset(tagOffset);
      return node;
    }

    private void deserializeInternalBasicInfo(InternalMNode node, InputStream inputStream)
        throws IOException {
      int templateHashCode = ReadWriteIOUtils.readInt(inputStream);
      node.setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
    }
  }
}
