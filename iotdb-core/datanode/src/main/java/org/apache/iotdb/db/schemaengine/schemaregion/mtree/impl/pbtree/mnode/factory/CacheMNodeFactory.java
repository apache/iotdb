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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.factory;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.MNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TreeDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.impl.CachedAboveDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.impl.CachedBasicInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.impl.CachedDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.impl.CachedMeasurementMNode;

import org.apache.tsfile.write.schema.IMeasurementSchema;

@MNodeFactory
public class CacheMNodeFactory implements IMNodeFactory<ICachedMNode> {

  @Override
  public IMeasurementMNode<ICachedMNode> createMeasurementMNode(
      IDeviceMNode<ICachedMNode> parent, String name, IMeasurementSchema schema, String alias) {
    return new CachedMeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public IDeviceMNode<ICachedMNode> createDeviceMNode(ICachedMNode parent, String name) {
    CachedBasicInternalMNode internalMNode = new CachedBasicInternalMNode(parent, name);
    internalMNode.setDeviceInfo(new TreeDeviceInfo<>());
    return internalMNode.getAsDeviceMNode();
  }

  @Override
  public IDatabaseMNode<ICachedMNode> createDatabaseMNode(ICachedMNode parent, String name) {
    return new CachedDatabaseMNode(parent, name);
  }

  @Override
  public ICachedMNode createDatabaseDeviceMNode(ICachedMNode parent, String name) {
    CachedDatabaseMNode databaseMNode = new CachedDatabaseMNode(parent, name);
    databaseMNode.setDeviceInfo(new TreeDeviceInfo<>());
    return databaseMNode;
  }

  @Override
  public ICachedMNode createAboveDatabaseMNode(ICachedMNode parent, String name) {
    return new CachedAboveDatabaseMNode(parent, name);
  }

  @Override
  public ICachedMNode createInternalMNode(ICachedMNode parent, String name) {
    return new CachedBasicInternalMNode(parent, name);
  }

  @Override
  public IMeasurementMNode<ICachedMNode> createLogicalViewMNode(
      IDeviceMNode<ICachedMNode> parent, String name, IMeasurementSchema measurementSchema) {
    throw new UnsupportedOperationException("View is not supported.");
  }
}
