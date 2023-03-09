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

import org.apache.iotdb.db.metadata.newnode.ICacheMNode;
import org.apache.iotdb.db.metadata.newnode.abovedatabase.CacheAboveDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.basic.CacheBasicMNode;
import org.apache.iotdb.db.metadata.newnode.database.CacheDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.databasedevice.CacheDatabaseDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.CacheDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.CacheMeasurementMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class CacheMNodeFactory implements IMNodeFactory<ICacheMNode> {
  @Override
  public IMeasurementMNode<ICacheMNode> createMeasurementMNode(
      IDeviceMNode<ICacheMNode> parent, String name, IMeasurementSchema schema, String alias) {
    return new CacheMeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public IDeviceMNode<ICacheMNode> createDeviceMNode(ICacheMNode parent, String name) {
    return new CacheDeviceMNode(parent, name);
  }

  @Override
  public IDatabaseMNode<ICacheMNode> createDatabaseMNode(ICacheMNode parent, String name) {
    return new CacheDatabaseMNode(parent, name);
  }

  @Override
  public ICacheMNode createDatabaseDeviceMNode(ICacheMNode parent, String name, long dataTTL) {
    return new CacheDatabaseDeviceMNode(parent, name, dataTTL);
  }

  @Override
  public ICacheMNode createAboveDatabaseMNode(ICacheMNode parent, String name) {
    return new CacheAboveDatabaseMNode(parent, name);
  }

  @Override
  public ICacheMNode createInternalMNode(ICacheMNode parent, String name) {
    return new CacheBasicMNode(parent, name);
  }
}
