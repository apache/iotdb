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

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public interface IMNodeFactory<N extends IMNode<N>> {
  IMeasurementMNode<N> createMeasurementMNode(
      N parent, String name, IMeasurementSchema schema, String alias);

  IDeviceMNode<N> createDeviceMNode(N parent, String name);

  IDatabaseMNode<N> createDatabaseMNode(N parent, String name);

  N createDatabaseDeviceMNode(N parent, String name, long dataTTL);

  N createAboveDatabaseMNode(N parent, String name);

  N createBasicMNode(N parent, String name);
}
