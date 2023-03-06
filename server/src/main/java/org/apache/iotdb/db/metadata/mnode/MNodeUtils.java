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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.newnode.database.AbstractDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.databasedevice.AbstractDatabaseDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.AbstractDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;

public class MNodeUtils {

  /**
   * When a measurement, represented by template or MeasurementMNode, is going to be added to a
   * node, the node should be set to entity. Before invoking this method, lock the related MTree
   * part first.
   *
   * @param node node to be transformed
   * @return generated entityMNode
   */
  public static <N extends IMNode<N>> IDeviceMNode<N> setToEntity(IMNode<N> node) {
    IDeviceMNode<N> entityMNode;
    if (node.isEntity()) {
      entityMNode = node.getAsEntityMNode();
    } else {
      if (node.isDatabase()) {
        entityMNode =
            new AbstractDatabaseDeviceMNode(
                node.getParent(), node.getName(), node.getAsDatabaseMNode().getDataTTL());
        node.moveDataToNewMNode(entityMNode);
      } else {
        // basic node
        entityMNode = new AbstractDeviceMNode(node.getParent(), node.getName());
        if (node.getParent() != null) {
          node.getParent().replaceChild(node.getName(), entityMNode);
        } else {
          node.moveDataToNewMNode(entityMNode);
        }
      }
    }
    return entityMNode;
  }

  /**
   * When there's no measurement, represented by template or MeasurementMNode, is under this
   * entityMNode, it should not act as entity anymore. Before invoking this method, lock related
   * MTree structure first.
   *
   * @param entityMNode node to be transformed
   * @return generated NoEntity node
   */
  public static <N extends IMNode<N>> IMNode<N> setToInternal(IDeviceMNode<N> entityMNode) {
    IMNode<N> node;
    IMNode<N> parent = entityMNode.getParent();
    if (entityMNode.isDatabase()) {
      node =
          new AbstractDatabaseMNode(
              parent, entityMNode.getName(), entityMNode.getAsDatabaseMNode().getDataTTL());
    } else {
      node = new BasicMNode(parent, entityMNode.getName());
    }

    if (parent != null) {
      parent.replaceChild(entityMNode.getName(), node);
    }
    return node;
  }
}
