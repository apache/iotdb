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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.source.IoTDBSource;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.historical.PipeHistoricalDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHeartbeatSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBDataRegionSourceTest {

  @Test
  public void testHistoryOnlyWaitsForReplicationBeforeLockingAndScanning() throws Exception {
    assertReplicationBeforeHistoricalStart(false, false);
  }

  @Test
  public void testHistoryOnlyCanRetryAfterReplicationBarrierFailure() throws Exception {
    assertReplicationBeforeHistoricalStart(false, true);
  }

  @Test
  public void testRealtimeSourceDoesNotWaitForReplicationBarrier() throws Exception {
    for (final boolean historyEnabled : new boolean[] {false, true}) {
      for (final String mode : new String[] {"hybrid", "log", "file", "forced-log"}) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(
            PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, String.valueOf(historyEnabled));
        attributes.put(PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY, "true");
        attributes.put(PipeSourceConstant.SOURCE_REALTIME_MODE_KEY, mode);
        // This option belongs to history-only sources and must not affect realtime sources.
        attributes.put(
            PipeSourceConstant.SOURCE_HISTORY_WAIT_FOR_REPLICATION_TIMEOUT_MS_KEY, "unused");
        assertReplicationBeforeHistoricalStart(attributes, false, 30_000);
      }
    }
  }

  @Test
  public void testSnapshotSourceWaitsForReplicationBarrier() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_MODE_KEY, "snapshot");
    assertReplicationBeforeHistoricalStart(attributes, false, 30_000);
  }

  @Test
  public void testHistoryOnlyReplicationTimeoutIsConfigurable() throws Exception {
    for (final String key :
        new String[] {
          PipeSourceConstant.SOURCE_HISTORY_WAIT_FOR_REPLICATION_TIMEOUT_MS_KEY,
          PipeSourceConstant.EXTRACTOR_HISTORY_WAIT_FOR_REPLICATION_TIMEOUT_MS_KEY,
          "history.wait-for-replication.timeout-ms"
        }) {
      final Map<String, String> attributes = new HashMap<>();
      attributes.put(PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY, "false");
      attributes.put(key, "90000");
      assertReplicationBeforeHistoricalStart(attributes, false, 90_000);
    }
  }

  @Test
  public void testHistoryOnlyRejectsNonPositiveReplicationTimeout() throws Exception {
    for (final String value : new String[] {"0", "-1"}) {
      final Map<String, String> attributes = new HashMap<>();
      attributes.put(PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY, "false");
      attributes.put(PipeSourceConstant.SOURCE_HISTORY_WAIT_FOR_REPLICATION_TIMEOUT_MS_KEY, value);
      try (final IoTDBDataRegionSource source = new IoTDBDataRegionSource()) {
        Assert.assertThrows(
            PipeParameterNotValidException.class,
            () -> source.validate(new PipeParameterValidator(new PipeParameters(attributes))));
      }
    }
  }

  private void assertReplicationBeforeHistoricalStart(
      final boolean realtimeEnabled, final boolean failFirst) throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY, String.valueOf(realtimeEnabled));
    assertReplicationBeforeHistoricalStart(attributes, failFirst, 30_000);
  }

  @SuppressWarnings("unchecked")
  private void assertReplicationBeforeHistoricalStart(
      final Map<String, String> attributes, final boolean failFirst, final long expectedTimeoutMs)
      throws Exception {
    final IoTDBDataRegionSource source = new IoTDBDataRegionSource();
    source.validate(new PipeParameterValidator(new PipeParameters(attributes)));
    final Field realtimeSourceField =
        IoTDBDataRegionSource.class.getDeclaredField("realtimeSource");
    realtimeSourceField.setAccessible(true);
    final PipeRealtimeDataRegionSource actualRealtimeSource =
        (PipeRealtimeDataRegionSource) realtimeSourceField.get(source);
    final boolean realtimeEnabled =
        !(actualRealtimeSource instanceof PipeRealtimeDataRegionHeartbeatSource);
    final IConsensus originalConsensus = DataRegionConsensusImpl.getInstance();
    final IoTConsensus consensus = Mockito.mock(IoTConsensus.class);
    final IoTConsensusServerImpl impl = Mockito.mock(IoTConsensusServerImpl.class);
    final DataRegionId regionId = new DataRegionId(2147483644);
    final Peer localPeer = new Peer(regionId, 1, new TEndPoint("127.0.0.1", 6667));
    final Peer remotePeer = new Peer(regionId, 2, new TEndPoint("127.0.0.1", 6668));
    Mockito.when(consensus.getImpl(regionId)).thenReturn(impl);
    Mockito.when(impl.getConfiguration()).thenReturn(Arrays.asList(localPeer, remotePeer));
    Mockito.when(impl.getThisNode()).thenReturn(localPeer);
    final DataRegion region = Mockito.mock(DataRegion.class);
    final PipeHistoricalDataRegionSource historical =
        Mockito.mock(PipeHistoricalDataRegionSource.class);
    final PipeRealtimeDataRegionSource realtime = Mockito.mock(actualRealtimeSource.getClass());
    setField(IoTDBDataRegionSource.class, source, "historicalSource", historical);
    setField(IoTDBDataRegionSource.class, source, "realtimeSource", realtime);
    setField(IoTDBSource.class, source, "regionId", regionId.getId());
    setField(IoTDBSource.class, source, "pipeName", "historical_replication_barrier");
    final AtomicBoolean replicated = new AtomicBoolean();
    final AtomicInteger attempts = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(region, Mockito.never()).writeLock(Mockito.anyString());
              final long timeoutMs = invocation.getArgument(1);
              Assert.assertTrue(
                  timeoutMs >= expectedTimeoutMs / 2 && timeoutMs <= expectedTimeoutMs);
              if (failFirst && attempts.getAndIncrement() == 0) {
                throw new ConsensusGroupModifyPeerException("mock replication failure");
              }
              replicated.set(true);
              return null;
            })
        .when(impl)
        .waitTargetPeerUntilSyncLogCompleted(Mockito.eq(remotePeer), Mockito.anyLong());
    Mockito.doAnswer(
            invocation -> {
              Assert.assertTrue(realtimeEnabled || replicated.get());
              return null;
            })
        .when(historical)
        .start();

    final Field regionsField = StorageEngine.class.getDeclaredField("dataRegionMap");
    regionsField.setAccessible(true);
    final Map<DataRegionId, DataRegion> regions =
        (Map<DataRegionId, DataRegion>) regionsField.get(StorageEngine.getInstance());
    Assert.assertFalse(regions.containsKey(regionId));
    regions.put(regionId, region);
    DataRegionConsensusImpl.setInstance(consensus);
    try {
      if (failFirst) {
        Assert.assertThrows(ConsensusGroupModifyPeerException.class, source::start);
        Mockito.verify(historical, Mockito.never()).start();
        Mockito.verify(region, Mockito.never()).writeLock(Mockito.anyString());
      }
      source.start();
      final InOrder order = Mockito.inOrder(impl, region, realtime, historical);
      if (realtimeEnabled) {
        Mockito.verify(consensus, Mockito.never()).getImpl(Mockito.any(DataRegionId.class));
        Mockito.verify(impl, Mockito.never())
            .waitTargetPeerUntilSyncLogCompleted(Mockito.any(Peer.class), Mockito.anyLong());
      } else {
        order
            .verify(impl, Mockito.times(failFirst ? 2 : 1))
            .waitTargetPeerUntilSyncLogCompleted(Mockito.eq(remotePeer), Mockito.anyLong());
        Mockito.verify(impl, Mockito.never())
            .waitTargetPeerUntilSyncLogCompleted(Mockito.eq(localPeer), Mockito.anyLong());
      }
      order.verify(region).writeLock(Mockito.anyString());
      order.verify(realtime).start();
      order.verify(historical).start();
      order.verify(region).writeUnlock();
    } finally {
      regions.remove(regionId);
      DataRegionConsensusImpl.setInstance(originalConsensus);
    }
  }

  private static void setField(
      final Class<?> declaringClass, final Object object, final String name, final Object value)
      throws ReflectiveOperationException {
    final Field field = declaringClass.getDeclaredField(name);
    field.setAccessible(true);
    field.set(object, value);
  }

  @Test
  public void testIoTDBDataRegionExtractor() {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY, Boolean.TRUE.toString());
                      put(
                          PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY,
                          Boolean.TRUE.toString());
                      put(
                          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY,
                          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE);
                    }
                  })));
    } catch (final Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testTsFileParserParameter() throws Exception {
    for (final String parser : new String[] {"query", "scan"}) {
      try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
        extractor.validate(
            new PipeParameterValidator(
                new PipeParameters(
                    new HashMap<String, String>() {
                      {
                        put(PipeSourceConstant.SOURCE_TSFILE_PARSER_KEY, parser);
                      }
                    })));
      }
    }

    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      Assert.assertThrows(
          PipeParameterNotValidException.class,
          () ->
              extractor.validate(
                  new PipeParameterValidator(
                      new PipeParameters(
                          new HashMap<String, String>() {
                            {
                              put(PipeSourceConstant.SOURCE_TSFILE_PARSER_KEY, "invalid");
                            }
                          }))));
    }
  }

  @Test
  public void testIoTDBDataRegionExtractorWithPattern() {
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a-b").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.1.a").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("r").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("123").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a b").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a+b").getClass());
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.ab."));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a#b"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.一二三"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.一二。三"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root."));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.ab"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a.b.c.1e2"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.`a-b`"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.1"));
  }

  public Exception testIoTDBDataRegionExtractorWithPattern(final String pattern) {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern);
                    }
                  })));
    } catch (final Exception e) {
      return e;
    }
    return null;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIoTDBDataRegionExtractorWithDeletionAndPattern() throws Exception {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root.db");
                      put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "data.delete");
                    }
                  })));
    }
  }
}
