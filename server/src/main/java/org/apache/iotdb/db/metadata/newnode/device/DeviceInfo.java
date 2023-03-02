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
package org.apache.iotdb.db.metadata.newnode.device;

import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;

public class DeviceInfo implements IDeviceInfo {

  /**
   * In EntityMNode of MTree in SchemaRegion, this field represents the template activated on this
   * node. The normal usage value range is [0, Int.MaxValue], since this is implemented as auto inc
   * id. The default value -1 means NON_TEMPLATE. This value will be set negative to implement some
   * pre-delete features.
   */
  protected int schemaTemplateId = NON_TEMPLATE;

  private volatile boolean useTemplate = false;

  /**
   * suppress warnings reason: volatile for double synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile Map<String, IMeasurementMNode> aliasChildren = null;

  private volatile boolean isAligned = false;

  /** add an alias */
  @Override
  public boolean addAlias(String alias, IMeasurementMNode child) {
    if (aliasChildren == null) {
      // double check, alias children volatile
      synchronized (this) {
        if (aliasChildren == null) {
          aliasChildren = new ConcurrentHashMap<>();
        }
      }
    }

    return aliasChildren.computeIfAbsent(alias, aliasName -> child) == child;
  }

  /** delete the alias of a child */
  @Override
  public void deleteAliasChild(String alias) {
    if (aliasChildren != null) {
      aliasChildren.remove(alias);
    }
  }

  @Override
  public Map<String, IMeasurementMNode> getAliasChildren() {
    if (aliasChildren == null) {
      return Collections.emptyMap();
    }
    return aliasChildren;
  }

  @Override
  public void setAliasChildren(Map<String, IMeasurementMNode> aliasChildren) {
    this.aliasChildren = aliasChildren;
  }

  /**
   * In EntityMNode(device node), schemaTemplateId represents the template activated on this node.
   * The pre deactivation mechanism is implemented by making this value negative. Since value 0 and
   * -1 are all occupied, the available negative value range is [Int.MIN_VALUE, -2]. The value of a
   * pre deactivated case equals the negative normal value minus 2. For example, if the id of
   * activated template is 0, then - 0 - 2 = -2 represents the pre deactivation of this template on
   * this node.
   */
  @Override
  public int getSchemaTemplateId() {
    return schemaTemplateId >= -1 ? schemaTemplateId : -schemaTemplateId - 2;
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    return schemaTemplateId;
  }

  @Override
  public boolean isPreDeactivateTemplate() {
    return schemaTemplateId < -1;
  }

  @Override
  public void preDeactivateTemplate() {
    if (schemaTemplateId > -1) {
      schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  @Override
  public void rollbackPreDeactivateTemplate() {
    if (schemaTemplateId < -1) {
      schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  @Override
  public void deactivateTemplate() {
    schemaTemplateId = -1;
    setUseTemplate(false);
  }

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }

  @Override
  public void setSchemaTemplateId(int schemaTemplateId) {
    this.schemaTemplateId = schemaTemplateId;
  }

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean isAligned) {
    this.isAligned = isAligned;
  }
}
