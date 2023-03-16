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
package org.apache.iotdb.db.metadata.mnode.config.factory;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.metadata.mnode.config.IConfigMNode;
import org.apache.iotdb.db.metadata.mnode.config.impl.ConfigBasicInternalMNode;
import org.apache.iotdb.db.metadata.mnode.config.impl.ConfigDatabaseMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class ConfigMNodeFactory implements IMNodeFactory<IConfigMNode> {
  private ConfigMNodeFactory() {}

  private static class ConfigMNodeFactoryHolder {
    private static final ConfigMNodeFactory INSTANCE = new ConfigMNodeFactory();

    private ConfigMNodeFactoryHolder() {}
  }

  public static ConfigMNodeFactory getInstance() {
    return ConfigMNodeFactory.ConfigMNodeFactoryHolder.INSTANCE;
  }

  @Override
  public IMeasurementMNode<IConfigMNode> createMeasurementMNode(
      IDeviceMNode<IConfigMNode> parent, String name, IMeasurementSchema schema, String alias) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IDeviceMNode<IConfigMNode> createDeviceMNode(IConfigMNode parent, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IDatabaseMNode<IConfigMNode> createDatabaseMNode(IConfigMNode parent, String name) {
    return new ConfigDatabaseMNode(parent, name);
  }

  @Override
  public IDatabaseMNode<IConfigMNode> createDatabaseMNode(
      IConfigMNode parent, String name, long dataTTL) {
    IDatabaseMNode<IConfigMNode> res = new ConfigDatabaseMNode(parent, name);
    res.setDataTTL(dataTTL);
    return res;
  }

  @Override
  public IConfigMNode createDatabaseDeviceMNode(IConfigMNode parent, String name, long dataTTL) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IConfigMNode createAboveDatabaseMNode(IConfigMNode parent, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IConfigMNode createInternalMNode(IConfigMNode parent, String name) {
    return new ConfigBasicInternalMNode(parent, name);
  }
}
