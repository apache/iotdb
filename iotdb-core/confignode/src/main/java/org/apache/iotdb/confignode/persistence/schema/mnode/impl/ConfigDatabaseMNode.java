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

package org.apache.iotdb.confignode.persistence.schema.mnode.impl;

import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.basic.ConfigBasicMNode;
import org.apache.iotdb.confignode.persistence.schema.mnode.info.ConfigDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

public class ConfigDatabaseMNode extends AbstractDatabaseMNode<IConfigMNode, ConfigBasicMNode>
    implements IConfigMNode {
  private final ConfigDatabaseInfo configDatabaseInfo;

  public ConfigDatabaseMNode(final IConfigMNode parent, final String name) {
    super(new ConfigBasicInternalMNode(parent, name), new ConfigDatabaseInfo(name));
    this.configDatabaseInfo = (ConfigDatabaseInfo) getDatabaseInfo();
  }

  @Override
  public void setSchemaTemplateId(final int id) {
    basicMNode.setSchemaTemplateId(id);
  }

  @Override
  public int getSchemaTemplateId() {
    return basicMNode.getSchemaTemplateId();
  }

  @Override
  public void preUnsetSchemaTemplate() {
    basicMNode.preUnsetSchemaTemplate();
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    basicMNode.rollbackUnsetSchemaTemplate();
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    return basicMNode.isSchemaTemplatePreUnset();
  }

  @Override
  public void unsetSchemaTemplate() {
    basicMNode.unsetSchemaTemplate();
  }

  @Override
  public void setDatabaseSchema(TDatabaseSchema schema) {
    configDatabaseInfo.setSchema(schema);
  }

  @Override
  public TDatabaseSchema getDatabaseSchema() {
    return configDatabaseInfo.getSchema();
  }

  @Override
  public IConfigMNode getAsMNode() {
    return this;
  }
}
