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
package org.apache.iotdb.db.metadata.mnode.config.info;

import org.apache.iotdb.db.metadata.mnode.mem.info.BasicMNodeInfo;

import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;

public class ConfigMNodeInfo extends BasicMNodeInfo {
  /**
   * This field is mainly used in cluster schema template features. In InternalMNode of ConfigMTree,
   * this field represents the template set on this node. The normal usage value range is [0,
   * Int.MaxValue], since this is implemented as auto inc id. The default value -1 means
   * NON_TEMPLATE. This value will be set negative to implement some pre-delete features.
   */
  protected int schemaTemplateId = NON_TEMPLATE;

  public ConfigMNodeInfo(String name) {
    super(name);
  }

  public void setSchemaTemplateId(int schemaTemplateId) {
    this.schemaTemplateId = schemaTemplateId;
  }

  /**
   * In InternalMNode, schemaTemplateId represents the template set on this node. The pre unset
   * mechanism is implemented by making this value negative. Since value 0 and -1 are all occupied,
   * the available negative value range is [Int.MIN_VALUE, -2]. The value of a pre unset case equals
   * the negative normal value minus 2. For example, if the id of set template is 0, then - 0 - 2 =
   * -2 represents the pre unset operation of this template on this node.
   */
  public int getSchemaTemplateId() {
    return schemaTemplateId >= -1 ? schemaTemplateId : -schemaTemplateId - 2;
  }

  public int getSchemaTemplateIdWithState() {
    return schemaTemplateId;
  }

  public void preUnsetSchemaTemplate() {
    if (this.schemaTemplateId > -1) {
      this.schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  public void rollbackUnsetSchemaTemplate() {
    if (schemaTemplateId < -1) {
      schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  public boolean isSchemaTemplatePreUnset() {
    return schemaTemplateId < -1;
  }

  public void unsetSchemaTemplate() {
    this.schemaTemplateId = -1;
  }

  @Override
  public int estimateSize() {
    // int schemaTemplateId, 4B
    return super.estimateSize() + 4;
  }
}
