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

import org.apache.iotdb.commons.exception.MetadataException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final String ERROR_INFO = "Error occurred during serializing MemMTree.";

  public static void serializeTo(MemMTreeStore store, OutputStream outputStream) throws MetadataException {
    dfsVisitMNode(store.getRoot(), store, outputStream);
  }

  private static void dfsVisitMNode(IMNode root, MemMTreeStore store, OutputStream outputStream)
      throws MetadataException {
    MNodeSerializer serializer = new MNodeSerializer();
    if (!root.accept(serializer, outputStream)) {
      throw new MetadataException(ERROR_INFO);
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
          throw new MetadataException(ERROR_INFO);
        }
        if (!node.isMeasurement()) {
          stack.push(store.getChildrenIterator(node));
        }
      } else {
        stack.pop();
      }
    }
  }

  public static IMNode deserializeFrom(InputStream inputStream) throws MetadataException {
    try {
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
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  private static int deserializeMNode(
      Deque<IMNode> ancestors, MNodeDeserializer deserializer, InputStream inputStream)
      throws IOException, MetadataException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    IMNode node;
    int childrenNum;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        InternalMNode internalMNode = new InternalMNode(null, null);
        childrenNum = internalMNode.accept(deserializer, inputStream);
        node = internalMNode;
        break;
      case STORAGE_GROUP_MNODE_TYPE:
        StorageGroupMNode storageGroupMNode = new StorageGroupMNode(null, null);
        childrenNum = storageGroupMNode.accept(deserializer, inputStream);
        node = storageGroupMNode;
        break;
      case MEASUREMENT_MNODE_TYPE:
        MeasurementMNode measurementMNode = new MeasurementMNode(null, null, null, null);
        childrenNum = measurementMNode.accept(deserializer, inputStream);
        node = measurementMNode;
        break;
      case ENTITY_MNODE_TYPE:
        EntityMNode entityMNode = new EntityMNode(null, null);
        childrenNum = entityMNode.accept(deserializer, inputStream);
        node = entityMNode;
        break;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        StorageGroupEntityMNode storageGroupEntityMNode =
            new StorageGroupEntityMNode(null, null, 0);
        childrenNum = storageGroupEntityMNode.accept(deserializer, inputStream);
        node = storageGroupEntityMNode;
        break;
      default:
        throw new MetadataException("Unrecognized MNode type " + type);
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
        logger.error(ERROR_INFO, e);
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
        logger.error(ERROR_INFO, e);
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
        logger.error(ERROR_INFO, e);
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
        logger.error(ERROR_INFO, e);
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
        logger.error(ERROR_INFO, e);
        return false;
      }
    }

    private void serializeInternalBasicInfo(InternalMNode node, OutputStream outputStream)
        throws IOException {
      ReadWriteIOUtils.write(node.getName(), outputStream);
      ReadWriteIOUtils.write(node.getChildren().size(), outputStream);
      ReadWriteIOUtils.write(node.getSchemaTemplate().hashCode(), outputStream);
      ReadWriteIOUtils.write(node.isUseTemplate(), outputStream);
    }
  }

  private static class MNodeDeserializer extends MNodeVisitor<Integer, InputStream> {

    @Override
    public Integer visitInternalMNode(InternalMNode node, InputStream inputStream) {

      return null;
    }

    @Override
    public Integer visitStorageGroupMNode(StorageGroupMNode node, InputStream inputStream) {
      return null;
    }

    @Override
    public Integer visitStorageGroupEntityMNode(
        StorageGroupEntityMNode node, InputStream inputStream) {
      return null;
    }

    @Override
    public Integer visitEntityMNode(EntityMNode node, InputStream inputStream) {
      return null;
    }

    @Override
    public Integer visitMeasurementMNode(MeasurementMNode node, InputStream inputStream) {
      return null;
    }

    private int deserializeInternalBasicInfo(InternalMNode node, InputStream inputStream)
        throws IOException {
      node.setName(ReadWriteIOUtils.readString(inputStream));
      int childrenNum = ReadWriteIOUtils.readInt(inputStream);
      int templateHashCode = ReadWriteIOUtils.readInt(inputStream);
      node.setUseTemplate(ReadWriteIOUtils.readBool(inputStream));
      return childrenNum;
    }
  }
}
