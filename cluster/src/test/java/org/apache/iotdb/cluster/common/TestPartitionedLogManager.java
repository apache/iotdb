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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.Constants;

public class TestPartitionedLogManager extends PartitionedSnapshotLogManager {

  public TestPartitionedLogManager() {
    super(
        new TestLogApplier(),
        null,
        new Node("localhost", 30001, 1, Constants.RPC_PORT, 6667, "localhost"),
        null,
        null,
        null);
  }

  public TestPartitionedLogManager(
      LogApplier logApplier, PartitionTable partitionTable, Node header, SnapshotFactory factory) {
    super(
        logApplier,
        partitionTable,
        header,
        new Node("localhost", 30001, 1, 40001, Constants.RPC_PORT, "localhost"),
        factory,
        null);
  }

  @Override
  public void takeSnapshot() {}
}
