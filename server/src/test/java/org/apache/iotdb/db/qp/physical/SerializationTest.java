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

package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

  private Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  @After
  public void clean() throws IOException {
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testInsert() throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr = "INSERT INTO root.vehicle.d1(timestamp, s1) VALUES (1, 5.0)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);
      ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
      assertEquals(plan, planB);
    }

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    plan.serialize(buffer);
    buffer.flip();
    PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
    assertEquals(plan, planB);
  }

  @Test
  public void testFlush() throws IOException, IllegalPathException {
    Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds = new HashMap<>();

    Boolean[] isSeqArray = new Boolean[] {null, true};
    boolean[] isSyncArray = new boolean[] {true, false};
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      List<Pair<Long, Boolean>> partitionIdPairs = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        partitionIdPairs.add(new Pair<Long, Boolean>((long) i + j, isSyncArray[random.nextInt(1)]));
      }

      storageGroupPartitionIds.put(new PartialPath(new String[] {"path_" + i}), partitionIdPairs);
    }
    for (Boolean isSeq : isSeqArray) {
      for (boolean isSync : isSyncArray) {
        FlushPlan plan = new FlushPlan(isSeq, isSync, storageGroupPartitionIds);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
          plan.serialize(dataOutputStream);
          ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
          FlushPlan planB = (FlushPlan) PhysicalPlan.Factory.create(buffer);
          assertEquals(plan.getPaths(), planB.getPaths());
          assertEquals(plan.getStorageGroupPartitionIds(), planB.getStorageGroupPartitionIds());
          assertEquals(plan.isSeq(), planB.isSeq());
          assertEquals(plan.isSync(), planB.isSync());
        }

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        plan.serialize(buffer);
        buffer.flip();
        FlushPlan planB = (FlushPlan) PhysicalPlan.Factory.create(buffer);
        assertEquals(plan.getPaths(), planB.getPaths());
        assertEquals(plan.getStorageGroupPartitionIds(), planB.getStorageGroupPartitionIds());
        assertEquals(plan.isSeq(), planB.isSeq());
        assertEquals(plan.isSync(), planB.isSync());
      }
    }
  }
}
