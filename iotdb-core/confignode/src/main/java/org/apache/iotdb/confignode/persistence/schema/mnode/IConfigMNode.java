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

package org.apache.iotdb.confignode.persistence.schema.mnode;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

public interface IConfigMNode extends IMNode<IConfigMNode> {

  void setSchemaTemplateId(int id);

  int getSchemaTemplateId();

  void preUnsetSchemaTemplate();

  void rollbackUnsetSchemaTemplate();

  boolean isSchemaTemplatePreUnset();

  void unsetSchemaTemplate();

  default void setDatabaseSchema(TDatabaseSchema schema) {
    throw new UnsupportedOperationException();
  }

  default TDatabaseSchema getDatabaseSchema() {
    throw new UnsupportedOperationException();
  }

  @Override
  default boolean isDevice() {
    return false;
  }

  @Override
  default boolean isMeasurement() {
    return false;
  }

  @Override
  default IInternalMNode<IConfigMNode> getAsInternalMNode() {
    throw new UnsupportedOperationException("Wrong node type");
  }

  @Override
  default IDeviceMNode<IConfigMNode> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong node type");
  }

  @Override
  default IMeasurementMNode<IConfigMNode> getAsMeasurementMNode() {
    throw new UnsupportedOperationException("Wrong node type");
  }
}
