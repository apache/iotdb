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
package org.apache.iotdb.commons.schema.node.role;

import org.apache.iotdb.commons.schema.node.IMNode;

import java.util.Map;

public interface IDeviceMNode<N extends IMNode<N>> extends IInternalMNode<N> {
  boolean addAlias(String alias, IMeasurementMNode<N> child);

  void deleteAliasChild(String alias);

  Map<String, IMeasurementMNode<N>> getAliasChildren();

  void setAliasChildren(Map<String, IMeasurementMNode<N>> aliasChildren);

  boolean isUseTemplate();

  void setUseTemplate(boolean useTemplate);

  void setSchemaTemplateId(int schemaTemplateId);

  /**
   * @return the logic id of template set or activated on this node, id>=-1
   */
  int getSchemaTemplateId();

  /**
   * @return the template id with current state, may be negative since unset or deactivation
   */
  int getSchemaTemplateIdWithState();

  boolean isPreDeactivateSelfOrTemplate();

  void preDeactivateSelfOrTemplate();

  void rollbackPreDeactivateSelfOrTemplate();

  void deactivateTemplate();

  boolean isAligned();

  Boolean isAlignedNullable();

  void setAligned(Boolean isAligned);
}
