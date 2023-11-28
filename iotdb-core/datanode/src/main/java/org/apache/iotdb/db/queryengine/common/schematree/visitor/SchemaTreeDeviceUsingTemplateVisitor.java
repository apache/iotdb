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

package org.apache.iotdb.db.queryengine.common.schematree.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;

public class SchemaTreeDeviceUsingTemplateVisitor extends SchemaTreeVisitor<PartialPath> {

  private final int templateId;

  public SchemaTreeDeviceUsingTemplateVisitor(
      SchemaNode root, PartialPath pathPattern, int templateId) {
    super(root, pathPattern, false);
    this.templateId = templateId;
  }

  @Override
  protected boolean mayTargetNodeType(SchemaNode node) {
    return node.isEntity();
  }

  @Override
  protected boolean acceptInternalMatchedNode(SchemaNode node) {
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(SchemaNode node) {
    return node.isEntity() && node.getAsEntityNode().getTemplateId() == templateId;
  }

  @Override
  protected PartialPath generateResult(SchemaNode nextMatchedNode) {
    return getPartialPathFromRootToNode(nextMatchedNode);
  }
}
