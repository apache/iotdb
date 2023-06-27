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

import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.AboveDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.DatabaseDeviceMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.DatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.DeviceMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.LogicalViewMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.MeasurementMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.LogicalViewInfo;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class MemMNodeFactory implements IMNodeFactory<IMemMNode> {

  private MemMNodeFactory() {}

  private static class MemMNodeFactoryHolder {
    private static final MemMNodeFactory INSTANCE = new MemMNodeFactory();

    private MemMNodeFactoryHolder() {}
  }

  public static MemMNodeFactory getInstance() {
    return MemMNodeFactory.MemMNodeFactoryHolder.INSTANCE;
  }

  @Override
  public IMeasurementMNode<IMemMNode> createMeasurementMNode(
      IDeviceMNode<IMemMNode> parent, String name, IMeasurementSchema schema, String alias) {
    return new MeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public IDeviceMNode<IMemMNode> createDeviceMNode(IMemMNode parent, String name) {
    return new DeviceMNode(parent, name);
  }

  @Override
  public IDatabaseMNode<IMemMNode> createDatabaseMNode(IMemMNode parent, String name) {
    return new DatabaseMNode(parent, name);
  }

  @Override
  public IDatabaseMNode<IMemMNode> createDatabaseMNode(
      IMemMNode parent, String name, long dataTTL) {
    return new DatabaseMNode(parent, name, dataTTL);
  }

  @Override
  public IMemMNode createDatabaseDeviceMNode(IMemMNode parent, String name, long dataTTL) {
    return new DatabaseDeviceMNode(parent, name, dataTTL);
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
      IDeviceMNode<IMemMNode> parent, String name, IMeasurementInfo measurementInfo) {
    if (measurementInfo instanceof LogicalViewInfo) {
      return new LogicalViewMNode(
          parent, name, ((LogicalViewInfo) measurementInfo).getExpression());
    }
    throw new UnsupportedOperationException(
        "createLogicalViewMNode should accept LogicalViewInfo, but got an instance that is not of this type.");
  }
}
