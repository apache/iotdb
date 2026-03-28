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

package org.apache.iotdb.confignode.procedure.impl.partition;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.DatabaseScopedDataPartitionTable;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataPartitionTableIntegrityCheckProcedureTest {
  @Test
  public void serDeTest() throws IOException {
    DataPartitionTableIntegrityCheckProcedure original = createTestProcedureWithData();

    try (PublicBAOS baos = new PublicBAOS();
        DataOutputStream dos = new DataOutputStream(baos)) {

      original.serialize(dos);

      System.out.println("Serialized bytes length: " + baos.size());

      ByteBuffer buffer = ByteBuffer.wrap(baos.getBuf(), 0, baos.size());

      Procedure<?> recreated = ProcedureFactory.getInstance().create(buffer);

      if (recreated instanceof DataPartitionTableIntegrityCheckProcedure) {
        DataPartitionTableIntegrityCheckProcedure actual =
            (DataPartitionTableIntegrityCheckProcedure) recreated;
        assertProcedureEquals(original, actual);
        System.out.println("All checked fields match!");
      } else {
        Assert.fail("Recreated is not DataPartitionTableIntegrityCheckProcedure");
      }
    }
  }

  private DataPartitionTableIntegrityCheckProcedure createTestProcedureWithData() {
    DataPartitionTableIntegrityCheckProcedure proc =
        new DataPartitionTableIntegrityCheckProcedure();
    String database = "root.test";

    Map<String, Long> earliestTimeslots = new HashMap<>();
    earliestTimeslots.put(database, 0L);
    proc.setEarliestTimeslots(earliestTimeslots);

    Map<Integer, List<DatabaseScopedDataPartitionTable>> dataPartitionTables = new HashMap<>();
    DataPartitionTable dataPartitionTable = new DataPartitionTable();
    dataPartitionTables.put(
        1,
        Collections.singletonList(
            new DatabaseScopedDataPartitionTable(database, dataPartitionTable)));
    proc.setDataPartitionTables(dataPartitionTables);

    Set<String> databasesWithLostDataPartition = new HashSet<>();
    databasesWithLostDataPartition.add(database);
    proc.setDatabasesWithLostDataPartition(databasesWithLostDataPartition);

    Map<String, DataPartitionTable> finalDataPartitionTables = new HashMap<>();
    finalDataPartitionTables.put(database, dataPartitionTable);
    proc.setFinalDataPartitionTables(finalDataPartitionTables);

    Set<TDataNodeConfiguration> skipNodes = getTDataNodeConfigurations(1);
    proc.setSkipDataNodes(skipNodes);

    Set<TDataNodeConfiguration> failedNodes = getTDataNodeConfigurations(2);
    proc.setFailedDataNodes(failedNodes);

    return proc;
  }

  private static Set<TDataNodeConfiguration> getTDataNodeConfigurations(int dataNodeId) {
    Set<TDataNodeConfiguration> nodes = new HashSet<>();
    TDataNodeLocation tDataNodeConfiguration =
        new TDataNodeLocation(
            dataNodeId,
            new TEndPoint("127.0.0.1", 5),
            new TEndPoint("127.0.0.1", 6),
            new TEndPoint("127.0.0.1", 7),
            new TEndPoint("127.0.0.1", 8),
            new TEndPoint("127.0.0.1", 9));
    TNodeResource resource = new TNodeResource(16, 34359738368L);
    TDataNodeConfiguration skipDataNodeConfiguration =
        new TDataNodeConfiguration(tDataNodeConfiguration, resource);
    nodes.add(skipDataNodeConfiguration);
    return nodes;
  }

  private void assertProcedureEquals(
      DataPartitionTableIntegrityCheckProcedure expected,
      DataPartitionTableIntegrityCheckProcedure actual) {
    Assert.assertEquals("procId mismatch", expected.getProcId(), actual.getProcId());
    Assert.assertEquals("state mismatch", expected.getState(), actual.getState());
    Assert.assertEquals(
        "earliestTimeslots mismatch",
        expected.getEarliestTimeslots(),
        actual.getEarliestTimeslots());
    Assert.assertEquals(
        "dataPartitionTables mismatch",
        expected.getDataPartitionTables(),
        actual.getDataPartitionTables());
    Assert.assertEquals(
        "databasesWithLostDataPartition mismatch",
        expected.getDatabasesWithLostDataPartition(),
        actual.getDatabasesWithLostDataPartition());
    Assert.assertEquals(
        "finalDataPartitionTables mismatch",
        expected.getFinalDataPartitionTables(),
        actual.getFinalDataPartitionTables());
    Assert.assertEquals(
        "skipDataNodes mismatch", expected.getSkipDataNodes(), actual.getSkipDataNodes());
    Assert.assertEquals(
        "failedDataNodes mismatch", expected.getFailedDataNodes(), actual.getFailedDataNodes());
  }
}
