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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataPartitionRouterTest {

  private static final IDeviceID DEVICE_1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
  private static final IDeviceID DEVICE_2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2");
  private static final TTimePartitionSlot SLOT_1 = new TTimePartitionSlot(0);
  private static final TTimePartitionSlot SLOT_2 = new TTimePartitionSlot(100);

  @Test
  public void testReusesThePartitionsTheJudgementResolved() {
    final RecordingPartitionFetcher fetcher = new RecordingPartitionFetcher();
    final DataPartitionResolver resolver = new DataPartitionResolver(fetcher, "root");
    // The judgement resolves the partitions of the devices of the file.
    assertEquals(
        Arrays.asList(route(10), route(11)),
        resolver.resolve(Arrays.asList(pair(DEVICE_1, SLOT_1), pair(DEVICE_2, SLOT_1))));

    final DataPartitionRouter router = new DataPartitionRouter(resolver);
    final List<TRegionReplicaSet> routed =
        router.route(Arrays.asList(chunk(DEVICE_1, SLOT_1), chunk(DEVICE_2, SLOT_1)));

    // The routing reuses them, so the fetcher is asked only once, by the judgement.
    assertEquals(1, fetcher.queries.size());
    assertEquals(
        Arrays.asList(pair(DEVICE_1, SLOT_1), pair(DEVICE_2, SLOT_1)), fetcher.queries.get(0));
    assertEquals(Arrays.asList(route(10), route(11)), routed);
  }

  @Test
  public void testFetchesOnlyTheSlotsTheJudgementDidNotResolve() {
    final RecordingPartitionFetcher fetcher = new RecordingPartitionFetcher();
    final DataPartitionResolver resolver = new DataPartitionResolver(fetcher, "root");
    resolver.resolve(Collections.singletonList(pair(DEVICE_1, SLOT_1)));

    final DataPartitionRouter router = new DataPartitionRouter(resolver);
    final List<TRegionReplicaSet> routed =
        router.route(
            Arrays.asList(
                chunk(DEVICE_2, SLOT_1),
                chunk(DEVICE_1, SLOT_1),
                chunk(DEVICE_2, SLOT_2),
                chunk(DEVICE_1, SLOT_1)));

    // The judged slot stays out of the batch, and the routing keeps the order of the chunks.
    assertEquals(2, fetcher.queries.size());
    assertEquals(
        Arrays.asList(pair(DEVICE_2, SLOT_1), pair(DEVICE_2, SLOT_2)), fetcher.queries.get(1));
    assertEquals(Arrays.asList(route(11), route(10), route(12), route(10)), routed);
  }

  @Test
  public void testFetchesEverythingWithoutAJudgement() {
    final RecordingPartitionFetcher fetcher = new RecordingPartitionFetcher();
    final DataPartitionRouter router =
        new DataPartitionRouter(new DataPartitionResolver(fetcher, "root"));

    final List<TRegionReplicaSet> routed =
        router.route(
            Arrays.asList(
                chunk(DEVICE_1, SLOT_1), chunk(DEVICE_1, SLOT_1), chunk(DEVICE_2, SLOT_2)));

    // The routing deduplicates the slots and asks for them in one batch, as it always did.
    assertEquals(1, fetcher.queries.size());
    assertEquals(
        Arrays.asList(pair(DEVICE_1, SLOT_1), pair(DEVICE_2, SLOT_2)), fetcher.queries.get(0));
    assertEquals(Arrays.asList(route(10), route(10), route(11)), routed);
  }

  @Test
  public void testRejectsAnAnswerThatDoesNotCoverTheSlots() {
    final RecordingPartitionFetcher fetcher = new RecordingPartitionFetcher();
    fetcher.dropAnswers = true;
    final DataPartitionRouter router =
        new DataPartitionRouter(new DataPartitionResolver(fetcher, "root"));

    try {
      router.route(Collections.singletonList(chunk(DEVICE_1, SLOT_1)));
      fail("A fetcher that does not answer every slot has to be rejected");
    } catch (final IllegalStateException e) {
      assertTrue(e.getMessage().contains("expected 1"));
    }
  }

  private static ChunkData chunk(final IDeviceID device, final TTimePartitionSlot slot) {
    final ChunkData chunkData = mock(ChunkData.class);
    when(chunkData.getDevice()).thenReturn(device);
    when(chunkData.getTimePartitionSlot()).thenReturn(slot);
    return chunkData;
  }

  private static Pair<IDeviceID, TTimePartitionSlot> pair(
      final IDeviceID device, final TTimePartitionSlot slot) {
    return new Pair<>(device, slot);
  }

  private static TRegionReplicaSet route(final int regionId) {
    return new TRegionReplicaSet(
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId), new ArrayList<>());
  }

  /** A partition fetcher that answers with a route of its own and keeps every slot list it saw. */
  private static class RecordingPartitionFetcher extends DataPartitionBatchFetcher {

    private final List<List<Pair<IDeviceID, TTimePartitionSlot>>> queries = new ArrayList<>();

    private int nextRouteId = 10;

    /** Answers with a shorter list, to check that the routing rejects it. */
    private boolean dropAnswers;

    private RecordingPartitionFetcher() {
      super(mock(IPartitionFetcher.class));
    }

    @Override
    List<TRegionReplicaSet> queryDataPartition(
        final List<Pair<IDeviceID, TTimePartitionSlot>> slotList, final String userName) {
      queries.add(new ArrayList<>(slotList));
      if (dropAnswers) {
        return new ArrayList<>();
      }
      final List<TRegionReplicaSet> replicaSets = new ArrayList<>(slotList.size());
      for (int i = 0; i < slotList.size(); i++) {
        replicaSets.add(route(nextRouteId++));
      }
      return replicaSets;
    }
  }
}
