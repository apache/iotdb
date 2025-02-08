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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.factory;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.MNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.AboveDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.DatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.MeasurementMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TreeDeviceInfo;

import org.apache.tsfile.write.schema.IMeasurementSchema;

@MNodeFactory
public class MemMNodeFactory implements IMNodeFactory<IMemMNode> {

  @Override
  public IMeasurementMNode<IMemMNode> createMeasurementMNode(
      IDeviceMNode<IMemMNode> parent, String name, IMeasurementSchema schema, String alias) {
    return new MeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public IDeviceMNode<IMemMNode> createDeviceMNode(IMemMNode parent, String name) {
    BasicInternalMNode internalMNode = new BasicInternalMNode(parent, name);
    internalMNode.setDeviceInfo(new TreeDeviceInfo<>());
    return internalMNode.getAsDeviceMNode();
  }

  @Override
  public IDatabaseMNode<IMemMNode> createDatabaseMNode(IMemMNode parent, String name) {
    return new DatabaseMNode(parent, name);
  }

  @Override
  public IMemMNode createDatabaseDeviceMNode(IMemMNode parent, String name) {
    DatabaseMNode databaseMNode = new DatabaseMNode(parent, name);
    databaseMNode.setDeviceInfo(new TreeDeviceInfo<>());
    return databaseMNode.getAsMNode();
  }

  @Override
  public IMemMNode createAboveDatabaseMNode(IMemMNode parent, String name) {
    return new AboveDatabaseMNode(parent, name);
  }

  @Override
  public IMemMNode createInternalMNode(IMemMNode parent, String name) {
    return new BasicInternalMNode(parent, name);
  }

  @Override
  public IMeasurementMNode<IMemMNode> createLogicalViewMNode(
      IDeviceMNode<IMemMNode> parent, String name, IMeasurementSchema measurementSchema) {
    throw new UnsupportedOperationException("View is not supported.");
  }
}
