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
package org.apache.iotdb.db.metadata.newnode.factory;

import org.apache.iotdb.db.metadata.newnode.IMemMNode;
import org.apache.iotdb.db.metadata.newnode.abovedatabase.AboveDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.basic.BasicInternalMNode;
import org.apache.iotdb.db.metadata.newnode.basic.BasicMNode;
import org.apache.iotdb.db.metadata.newnode.database.DatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.databasedevice.DatabaseDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.DeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.MeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class MemMNodeFactory implements IMNodeFactory<IMemMNode> {
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
}
