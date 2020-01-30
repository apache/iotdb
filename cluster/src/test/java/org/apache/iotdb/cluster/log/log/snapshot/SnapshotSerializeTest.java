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

package org.apache.iotdb.cluster.log.log.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.RemoteFileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

public class SnapshotSerializeTest {

  @Test
  public void testFileSnapshot() {
    Set<MeasurementSchema> measurementSchemaList = new HashSet<>();
    FileSnapshot snapshot = new FileSnapshot();
    for (int i = 0; i < 10; i++) {
      measurementSchemaList.add(TestUtils.getTestSchema(i, i));
      TsFileResource tsFileResource = new TsFileResource(new File("TsFile" + i));
      tsFileResource.setHistoricalVersions(Collections.singleton((long) i));
      snapshot.addFile(tsFileResource, TestUtils.getNode(i));
    }
    snapshot.setTimeseriesSchemas(measurementSchemaList);

    ByteBuffer byteBuffer = snapshot.serialize();
    FileSnapshot deserializedSnapshot = new FileSnapshot();
    deserializedSnapshot.deserialize(byteBuffer);
    assertEquals(snapshot, deserializedSnapshot);
  }

  @Test
  public void testMetaSimpleSnapshot() {
    List<Log> logs = new ArrayList<>();
    List<String> sgs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan =
          new CreateTimeSeriesPlan(new Path(schema.getMeasurementId()), schema.getType(),
              schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      PhysicalPlanLog log = new PhysicalPlanLog();
      log.setPlan(createTimeSeriesPlan);
      logs.add(log);
      log.setPreviousLogTerm(i - 1);
      log.setPreviousLogIndex(i - 1);
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      sgs.add(TestUtils.getTestSg(i));
    }
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot(logs, sgs);
    snapshot.setLastLogId(10);
    snapshot.setLastLogTerm(10);

    ByteBuffer byteBuffer = snapshot.serialize();
    MetaSimpleSnapshot deserializedSnapshot = new MetaSimpleSnapshot();
    deserializedSnapshot.deserialize(byteBuffer);
    assertEquals(snapshot, deserializedSnapshot);
  }

  @Test
  public void testPartitionedSnapshot() {
    PartitionedSnapshot<TestSnapshot> snapshot = new PartitionedSnapshot<>(TestSnapshot::new);
    snapshot.setLastLogTerm(10);
    snapshot.setLastLogId(10);

    Map<Integer, Snapshot> slotSnapshots = new HashMap<>();
    List<Integer> slots = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      TestSnapshot s = new TestSnapshot();
      slotSnapshots.put(i, s);
      snapshot.putSnapshot(i, s);
      slots.add(i);
    }

    for (Integer slot : slots) {
      TestSnapshot s = (TestSnapshot) snapshot.getSnapshot(slot);
      assertTrue(slotSnapshots.values().contains(s));
    }

    PartitionedSnapshot subSnapshots = snapshot.getSubSnapshots(slots);
    for (Integer slot : slots) {
      TestSnapshot s = (TestSnapshot) subSnapshots.getSnapshot(slot);
      assertTrue(slotSnapshots.values().contains(s));
    }

    ByteBuffer byteBuffer = snapshot.serialize();
    PartitionedSnapshot deserializedSnapshot = new PartitionedSnapshot(TestSnapshot::new);
    deserializedSnapshot.deserialize(byteBuffer);
    assertEquals(snapshot, deserializedSnapshot);
  }


  @Test
  public void testRemoteFileSnapshot() {
    AtomicBoolean remoteSnapshotGet = new AtomicBoolean(false);
    RemoteFileSnapshot snapshot = new RemoteFileSnapshot(
        new Future<Void>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
          }

          @Override
          public boolean isCancelled() {
            return false;
          }

          @Override
          public boolean isDone() {
            return false;
          }

          @Override
          public Void get() {
            remoteSnapshotGet.set(true);
            return null;
          }

          @Override
          public Void get(long timeout, TimeUnit unit) {
            remoteSnapshotGet.set(true);
            return null;
          }
        });

    assertNull(snapshot.serialize());
    snapshot.getRemoteSnapshot();
    assertTrue(remoteSnapshotGet.get());
  }

  @Test
  public void testSimpleSnapshot() {
    List<Log> logs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan =
          new CreateTimeSeriesPlan(new Path(schema.getMeasurementId()), schema.getType(),
              schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      PhysicalPlanLog log = new PhysicalPlanLog();
      log.setPlan(createTimeSeriesPlan);
      logs.add(log);
      log.setPreviousLogTerm(i - 1);
      log.setPreviousLogIndex(i - 1);
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
    }
    SimpleSnapshot snapshot = new SimpleSnapshot(new ArrayList<>(logs.subList(0, 5)));
    snapshot.setLastLogId(10);
    snapshot.setLastLogTerm(10);
    for (Log log : logs.subList(5, logs.size())) {
      snapshot.add(log);
    }

    ByteBuffer byteBuffer = snapshot.serialize();
    SimpleSnapshot deserializedSnapshot = new SimpleSnapshot();
    deserializedSnapshot.deserialize(byteBuffer);
    assertEquals(snapshot, deserializedSnapshot);
  }
}