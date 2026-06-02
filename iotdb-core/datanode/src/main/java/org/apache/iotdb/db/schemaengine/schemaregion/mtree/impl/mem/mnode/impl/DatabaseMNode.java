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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl;

import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.info.IDeviceInfo;
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.DatabaseInfo;

public class DatabaseMNode extends AbstractDatabaseMNode<IMemMNode, BasicInternalMNode>
    implements IMemMNode, IInternalMNode<IMemMNode> {

  public DatabaseMNode(IMemMNode parent, String name) {
    super(new BasicInternalMNode(parent, name), new DatabaseInfo<>());
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }

  @Override
  public IDeviceInfo<IMemMNode> getDeviceInfo() {
    return basicMNode.getDeviceInfo();
  }

  @Override
  public void setDeviceInfo(IDeviceInfo<IMemMNode> deviceInfo) {
    basicMNode.setDeviceInfo(deviceInfo);
  }
}
