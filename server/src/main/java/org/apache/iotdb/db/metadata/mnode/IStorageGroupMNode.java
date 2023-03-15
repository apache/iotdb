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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

/** This interface defines a StorageGroupMNode's operation interfaces. */
public interface IStorageGroupMNode extends IMNode {

  long getDataTTL();

  void setDataTTL(long dataTTL);

  void setSchemaReplicationFactor(int schemaReplicationFactor);

  void setDataReplicationFactor(int dataReplicationFactor);

  void setTimePartitionInterval(long timePartitionInterval);

  void setStorageGroupSchema(TDatabaseSchema schema);

  TDatabaseSchema getStorageGroupSchema();
}
