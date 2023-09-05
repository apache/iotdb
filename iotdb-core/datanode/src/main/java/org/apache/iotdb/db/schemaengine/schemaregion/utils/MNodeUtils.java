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
package org.apache.iotdb.db.schemaengine.schemaregion.utils;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Iterator;

public class MNodeUtils {

  /**
   * When a measurement, represented by template or MeasurementMNode, is going to be added to a
   * node, the node should be set to entity. Before invoking this method, lock the related MTree
   * part first.
   *
   * @param node node to be transformed
   * @return generated entityMNode
   */
  public static <N extends IMNode<N>> IDeviceMNode<N> setToEntity(
      IMNode<N> node, IMNodeFactory<N> nodeFactory) {
    IDeviceMNode<N> entityMNode;
    if (node.isDevice()) {
      entityMNode = node.getAsDeviceMNode();
    } else {
      if (node.isDatabase()) {
        entityMNode =
            nodeFactory
                .createDatabaseDeviceMNode(
                    node.getParent(), node.getName(), node.getAsDatabaseMNode().getDataTTL())
                .getAsDeviceMNode();
        node.moveDataToNewMNode(entityMNode.getAsMNode());
      } else {
        // basic node
        entityMNode = nodeFactory.createDeviceMNode(node.getParent(), node.getName());
        if (node.getParent() != null) {
          node.getParent().replaceChild(node.getName(), entityMNode.getAsMNode());
        } else {
          node.moveDataToNewMNode(entityMNode.getAsMNode());
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
  public static <N extends IMNode<N>> N setToInternal(
      IDeviceMNode<N> entityMNode, IMNodeFactory<N> nodeFactor) {
    N node;
    N parent = entityMNode.getParent();
    if (entityMNode.isDatabase()) {
      IDatabaseMNode<N> databaseMNode =
          nodeFactor.createDatabaseMNode(parent, entityMNode.getName());
      databaseMNode.setDataTTL(entityMNode.getAsDatabaseMNode().getDataTTL());
      node = databaseMNode.getAsMNode();
    } else {
      node = nodeFactor.createInternalMNode(parent, entityMNode.getName());
    }

    if (parent != null) {
      parent.replaceChild(entityMNode.getName(), node);
    }
    return node;
  }

  public static <N extends IMNode<N>> N getChild(
      Template template, String name, IMNodeFactory<N> nodeFactor) {
    IMeasurementSchema schema = template.getSchema(name);
    return schema == null
        ? null
        : nodeFactor
            .createMeasurementMNode(null, name, template.getSchema(name), null)
            .getAsMNode();
  }

  public static <N extends IMNode<N>> Iterator<N> getChildren(
      Template template, IMNodeFactory<N> nodeFactor) {
    return new Iterator<N>() {
      private final Iterator<IMeasurementSchema> schemas =
          template.getSchemaMap().values().iterator();

      @Override
      public boolean hasNext() {
        return schemas.hasNext();
      }

      @Override
      public N next() {
        IMeasurementSchema schema = schemas.next();
        return nodeFactor
            .createMeasurementMNode(null, schema.getMeasurementId(), schema, null)
            .getAsMNode();
      }
    };
  }
}
