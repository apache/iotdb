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

package org.apache.iotdb.db.metadata.mnode.mem.impl;

import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.mem.IMemMNode;
import org.apache.iotdb.db.metadata.mnode.mem.basic.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.mem.container.MemMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.mem.info.LogicalViewInfo;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

public class LogicalViewMNode extends AbstractMeasurementMNode<IMemMNode, BasicMNode>
    implements IMemMNode {

  public LogicalViewMNode(
      IDeviceMNode<IMemMNode> parent, String name, ViewExpression viewExpression) {
    super(
        new BasicMNode(parent == null ? null : parent.getAsMNode(), name),
        new LogicalViewInfo(new LogicalViewSchema(name, viewExpression)));
  }

  @Override
  public IMNodeContainer<IMemMNode> getChildren() {
    return MemMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }

  public void setExpression(ViewExpression expression) {
    IMeasurementInfo measurementInfo = this.getMeasurementInfo();
    if (measurementInfo instanceof LogicalViewInfo) {
      ((LogicalViewInfo) measurementInfo).setExpression(expression);
    }
  }

  @Override
  public final boolean isLogicalView() {
    return true;
  }
}
