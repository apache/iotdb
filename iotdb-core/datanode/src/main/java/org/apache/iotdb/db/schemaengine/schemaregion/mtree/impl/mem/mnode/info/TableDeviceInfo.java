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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.info.IDeviceInfo;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;

import java.util.Map;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

public class TableDeviceInfo<N extends IMNode<N>> implements IDeviceInfo<N> {

  private int attributePointer = -1;

  public int getAttributePointer() {
    return attributePointer >= -1 ? attributePointer : -attributePointer - 2;
  }

  public void setAttributePointer(final int attributePointer) {
    this.attributePointer = attributePointer;
  }

  @Override
  public void moveDataToNewMNode(final IDeviceMNode<N> newMNode) {}

  @Override
  public boolean addAlias(final String alias, final IMeasurementMNode<N> child) {
    return false;
  }

  @Override
  public void deleteAliasChild(final String alias) {}

  @Override
  public Map<String, IMeasurementMNode<N>> getAliasChildren() {
    return null;
  }

  @Override
  public void setAliasChildren(final Map<String, IMeasurementMNode<N>> aliasChildren) {}

  @Override
  public boolean hasAliasChild(final String name) {
    return false;
  }

  @Override
  public N getAliasChild(final String name) {
    return null;
  }

  @Override
  public boolean isUseTemplate() {
    return false;
  }

  @Override
  public void setUseTemplate(final boolean useTemplate) {}

  @Override
  public void setSchemaTemplateId(final int schemaTemplateId) {}

  @Override
  public int getSchemaTemplateId() {
    return NON_TEMPLATE;
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    return 0;
  }

  @Override
  public boolean isPreDeactivateSelfOrTemplate() {
    return attributePointer < -1;
  }

  @Override
  public void preDeactivateSelfOrTemplate() {
    if (attributePointer > -1) {
      attributePointer = -attributePointer - 2;
    }
  }

  @Override
  public void rollbackPreDeactivateSelfOrTemplate() {
    if (attributePointer < -1) {
      attributePointer = -attributePointer - 2;
    }
  }

  @Override
  public void deactivateTemplate() {}

  @Override
  public Boolean isAligned() {
    return true;
  }

  @Override
  public void setAligned(final Boolean isAligned) {}

  @Override
  public int estimateSize() {
    return 12;
  }
}
