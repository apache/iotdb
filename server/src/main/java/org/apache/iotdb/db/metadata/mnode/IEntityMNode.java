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

import java.util.Map;

public interface IEntityMNode extends IMNode {

  boolean addAlias(String alias, IMeasurementMNode child);

  void deleteAliasChild(String alias);

  Map<String, IMeasurementMNode> getAliasChildren();

  void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren);

  boolean isUseTemplate();

  void setUseTemplate(boolean useTemplate);

  static IEntityMNode setToEntity(IMNode node) {
    IEntityMNode entityMNode;
    if (node.isEntity()) {
      entityMNode = (IEntityMNode) node;
    } else {
      if (node.isStorageGroup()) {
        entityMNode =
            new StorageGroupEntityMNode(
                node.getParent(), node.getName(), ((StorageGroupMNode) node).getDataTTL());
      } else {
        entityMNode = new EntityMNode(node.getParent(), node.getName());
      }
      if (node.getParent() != null) {
        node.getParent().replaceChild(node.getName(), entityMNode);
      }
    }
    return entityMNode;
  }
}
