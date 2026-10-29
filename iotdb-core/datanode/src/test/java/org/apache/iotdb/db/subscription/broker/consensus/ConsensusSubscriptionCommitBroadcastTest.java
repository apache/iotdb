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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ConsensusSubscriptionCommitBroadcastTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final DataRegionId REGION = new DataRegionId(31);
  private static final WriterId WRITER = new WriterId(REGION.toString(), 7);

  private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
  private final AtomicLong now = new AtomicLong(10_000L);
  private ConsensusSubscriptionCommitManager manager;
  private String originalSystemDir;

  @Before
  public void setUp() throws Exception {
    originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSystemDir(temporaryFolder.newFolder().getAbsolutePath());
    manager =
        spy(
            new ConsensusSubscriptionCommitManager(
                (consumerGroupId, topicName, regionId) ->
                    ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
                        new RegionProgress(Collections.emptyMap())),
                executor,
                now::get));
    doNothing().when(manager).doBroadcast(eq("group"), eq("topic"), eq(REGION), any(), any());
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
  }

  @Test
  public void testThrottledFinalAckIsBroadcastWithoutAnotherCommit() {
    commit(WRITER, 1L);
    final ArgumentCaptor<Runnable> tasks = ArgumentCaptor.forClass(Runnable.class);
    verify(executor).schedule(tasks.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    tasks.getValue().run();
    verify(manager).doBroadcast("group", "topic", REGION, progress(1L), WRITER);

    now.addAndGet(100L);
    commit(WRITER, 2L);
    commit(WRITER, 3L);
    verify(executor).schedule(tasks.capture(), eq(4900L), eq(TimeUnit.MILLISECONDS));
    verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));

    now.addAndGet(4900L);
    tasks.getValue().run();
    verify(manager).doBroadcast("group", "topic", REGION, progress(3L), WRITER);
    verify(manager, never()).doBroadcast("group", "topic", REGION, progress(2L), WRITER);
  }

  @Test
  public void testWritersHaveIndependentBroadcasts() {
    final WriterId otherWriter = new WriterId(REGION.toString(), 8);
    commit(WRITER, 1L);
    commit(otherWriter, 1L);
    final ArgumentCaptor<Runnable> tasks = ArgumentCaptor.forClass(Runnable.class);
    verify(executor, times(2)).schedule(tasks.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    tasks.getAllValues().forEach(Runnable::run);

    verify(manager).doBroadcast("group", "topic", REGION, progress(1L), WRITER);
    verify(manager).doBroadcast("group", "topic", REGION, progress(1L), otherWriter);
  }

  @Test
  public void testRemovedStateCannotBroadcastIntoResubscribedState() {
    commit(WRITER, 1L);
    final ArgumentCaptor<Runnable> tasks = ArgumentCaptor.forClass(Runnable.class);
    verify(executor).schedule(tasks.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    final Runnable oldTask = tasks.getValue();

    manager.removeState("group", "topic", REGION);
    commit(WRITER, 2L);
    verify(executor, times(2)).schedule(tasks.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    oldTask.run();
    verify(manager, never()).doBroadcast("group", "topic", REGION, progress(1L), WRITER);
    tasks.getValue().run();
    verify(manager).doBroadcast("group", "topic", REGION, progress(2L), WRITER);
  }

  private void commit(final WriterId writerId, final long sequence) {
    manager.recordMapping("group", "topic", REGION, writerId, progress(sequence));
    assertTrue(manager.commit("group", "topic", REGION, writerId, progress(sequence)));
  }

  private static WriterProgress progress(final long sequence) {
    return new WriterProgress(100L + sequence, sequence);
  }
}
