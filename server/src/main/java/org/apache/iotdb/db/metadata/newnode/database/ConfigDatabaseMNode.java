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
package org.apache.iotdb.db.metadata.newnode.database;

import org.apache.iotdb.db.metadata.newnode.IConfigMNode;
import org.apache.iotdb.db.metadata.newnode.basic.ConfigBasicInternalMNode;
import org.apache.iotdb.db.metadata.newnode.basic.ConfigBasicMNode;

public class ConfigDatabaseMNode extends AbstractDatabaseMNode<IConfigMNode, ConfigBasicMNode>
    implements IConfigMNode {

  public ConfigDatabaseMNode(IConfigMNode parent, String name) {
    super(new ConfigBasicInternalMNode(parent, name), new DatabaseInfo(name));
  }

  @Override
  public void setSchemaTemplateId(int id) {
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
  public IConfigMNode getAsMNode() {
    return this;
  }
}
