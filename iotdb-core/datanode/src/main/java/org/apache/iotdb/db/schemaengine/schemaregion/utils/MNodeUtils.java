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
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TreeDeviceInfo;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Iterator;

public class MNodeUtils {

  /**
   * When a measurement, represented by template or MeasurementMNode, is going to be added to a
   * node, the node should be set to entity. Before invoking this method, lock the related MTree
   * part first.
   *
   * @param node node to be transformed
   */
  public static <N extends IMNode<N>> boolean setToEntity(IMNode<N> node) {
    IInternalMNode<N> internalMNode = node.getAsInternalMNode();
    if (!internalMNode.isDevice()) {
      internalMNode.setDeviceInfo(new TreeDeviceInfo<>());
      return true;
    } else {
      return false;
    }
  }

  /**
   * When there's no measurement, represented by template or MeasurementMNode, is under this
   * entityMNode, it should not act as entity anymore. Before invoking this method, lock related
   * MTree structure first.
   *
   * @param entityMNode node to be transformed
   */
  public static <N extends IMNode<N>> boolean setToInternal(IDeviceMNode<N> entityMNode) {
    IInternalMNode<N> internalMNode = entityMNode.getAsInternalMNode();
    if (internalMNode.isDevice()) {
      internalMNode.setDeviceInfo(null);
      return true;
    } else {
      return false;
    }
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
            .createMeasurementMNode(null, schema.getMeasurementName(), schema, null)
            .getAsMNode();
      }
    };
  }
}
