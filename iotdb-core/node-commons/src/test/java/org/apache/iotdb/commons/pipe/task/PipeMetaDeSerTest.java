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

package org.apache.iotdb.commons.pipe.task;

import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipeMetaDeSerTest {

  @Test
  public void testCompletionSupportSurvivesBothDeserializationPaths() throws IOException {
    for (final BuiltinPipePlugin sink :
        new BuiltinPipePlugin[] {
          BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR,
          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK,
          BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR,
          BuiltinPipePlugin.IOTDB_AIR_GAP_SINK
        }) {
      final PipeStaticMeta staticMeta =
          new PipeStaticMeta(
              "pipe",
              1L,
              Map.of(),
              Map.of(),
              Map.of(PipeSinkConstant.SINK_KEY, sink.getPipePluginName().toUpperCase(Locale.ROOT)));
      Assert.assertTrue(staticMeta.supportsCompletionMark());
      Assert.assertTrue(
          PipeStaticMeta.deserialize(staticMeta.serialize()).supportsCompletionMark());
      final ByteBuffer serialized = staticMeta.serialize();
      final byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      Assert.assertTrue(
          PipeStaticMeta.deserialize(new ByteArrayInputStream(bytes)).supportsCompletionMark());
    }
  }

  @Test
  public void testCompletionSupportRejectsUnsupportedConfigurations() {
    Assert.assertFalse(
        new PipeStaticMeta("__subscription.pipe", 1L, Map.of(), Map.of(), Map.of())
            .supportsCompletionMark());
    Assert.assertFalse(
        new PipeStaticMeta(
                "pipe",
                1L,
                Map.of(PipeSourceConstant.SOURCE_KEY, "custom-source"),
                Map.of(),
                Map.of())
            .supportsCompletionMark());
    Assert.assertFalse(
        new PipeStaticMeta(
                "pipe",
                1L,
                Map.of(),
                Map.of(PipeProcessorConstant.PROCESSOR_KEY, "aggregate-processor"),
                Map.of())
            .supportsCompletionMark());
    Assert.assertFalse(
        new PipeStaticMeta(
                "pipe",
                1L,
                Map.of(),
                Map.of(),
                Map.of(PipeSinkConstant.SINK_KEY, "write-back-sink"))
            .supportsCompletionMark());
    Assert.assertFalse(
        new PipeStaticMeta(
                "pipe",
                1L,
                Map.of(),
                Map.of(),
                Map.of(
                    PipeSinkConstant.SINK_KEY, "iotdb-air-gap-sink",
                    PipeSinkConstant.SINK_LOAD_TSFILE_STRATEGY_KEY, "async"))
            .supportsCompletionMark());
  }

  @Test
  public void testPipeStatusTypeCompatibility() {
    Assert.assertEquals((byte) 0, PipeStatus.RUNNING.getType());
    Assert.assertEquals((byte) 1, PipeStatus.STOPPED.getType());
    Assert.assertEquals((byte) 2, PipeStatus.DROPPED.getType());
    Assert.assertEquals((byte) 3, PipeStatus.PRE_DELETE.getType());
  }

  @Test
  public void test() throws IOException {
    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "pipeName",
            123L,
            new HashMap<String, String>() {
              {
                put("extractor-key", "extractor-value");
              }
            },
            new HashMap<String, String>() {
              {
                put("processor-key-1", "processor-value-1");
                put("processor-key-2", "processor-value-2");
              }
            },
            new HashMap<String, String>() {});
    final ByteBuffer staticByteBuffer = pipeStaticMeta.serialize();
    final PipeStaticMeta pipeStaticMeta1 = PipeStaticMeta.deserialize(staticByteBuffer);
    Assert.assertEquals(pipeStaticMeta, pipeStaticMeta1);

    HybridProgressIndex hybridProgressIndex =
        new HybridProgressIndex(new SimpleProgressIndex(1, 2));
    hybridProgressIndex =
        (HybridProgressIndex)
            hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                new SimpleProgressIndex(2, 4));
    hybridProgressIndex =
        (HybridProgressIndex)
            hybridProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                new IoTProgressIndex(3, 6L));

    final Map<String, Pair<Long, ByteBuffer>> timeSeries2TimestampWindowBufferPairMap =
        new HashMap<>();
    final ByteBuffer buffer;
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write("123", outputStream);
      buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    timeSeries2TimestampWindowBufferPairMap.put("root.test.a1", new Pair<>(123L, buffer));

    final HybridProgressIndex finalHybridProgressIndex = hybridProgressIndex;
    final PipeRuntimeMeta pipeRuntimeMeta =
        new PipeRuntimeMeta(
            new ConcurrentHashMap<Integer, PipeTaskMeta>() {
              {
                put(123, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 987));
                put(234, new PipeTaskMeta(new IoTProgressIndex(1, 2L), 789));
                put(345, new PipeTaskMeta(new SimpleProgressIndex(3, 4), 789));
                put(456, new PipeTaskMeta(finalHybridProgressIndex, 789));
                put(
                    567,
                    new PipeTaskMeta(
                        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 9)), 123));
                put(
                    678,
                    new PipeTaskMeta(
                        new TimeWindowStateProgressIndex(timeSeries2TimestampWindowBufferPairMap),
                        789));
                put(
                    789,
                    new PipeTaskMeta(
                        new TimePartitionProgressIndex(
                            Map.of(
                                0L,
                                new SimpleProgressIndex(0, 1),
                                1L,
                                new SimpleProgressIndex(0, 2))),
                        789));
                put(Integer.MIN_VALUE, new PipeTaskMeta(new MetaProgressIndex(987), 0));
              }
            });
    ByteBuffer runtimeByteBuffer = pipeRuntimeMeta.serialize();
    PipeRuntimeMeta pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(false);
    pipeRuntimeMeta.setExceptionsClearTime(123456789L);
    pipeRuntimeMeta
        .getNodeId2PipeRuntimeExceptionMap()
        .put(123, new PipeRuntimeCriticalException("test"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.DROPPED);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(true);
    pipeRuntimeMeta.setExceptionsClearTime(0);
    pipeRuntimeMeta
        .getNodeId2PipeRuntimeExceptionMap()
        .put(123, new PipeRuntimeCriticalException("test123"));
    pipeRuntimeMeta
        .getNodeId2PipeRuntimeExceptionMap()
        .put(345, new PipeRuntimeCriticalException("test345"));
    pipeRuntimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .get(456)
        .trackExceptionMessage(new PipeRuntimeSinkCriticalException("test456"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.PRE_DELETE);
    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    final PipeMeta pipeMeta = new PipeMeta(pipeStaticMeta, pipeRuntimeMeta);
    final ByteBuffer byteBuffer = pipeMeta.serialize();
    final PipeMeta pipeMeta1 = PipeMeta.deserialize4Coordinator(byteBuffer);
    Assert.assertEquals(pipeMeta, pipeMeta1);
  }

  @Test
  public void testClearExceptionMessagesBeforeClearTime() {
    final PipeTaskMeta staleTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    staleTaskMeta.trackExceptionMessage(new PipeRuntimeCriticalException("stale", 100L));
    final PipeTaskMeta freshTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
    freshTaskMeta.trackExceptionMessage(new PipeRuntimeCriticalException("fresh", 300L));

    final ConcurrentHashMap<Integer, PipeTaskMeta> taskMetaMap = new ConcurrentHashMap<>();
    taskMetaMap.put(1, staleTaskMeta);
    taskMetaMap.put(2, freshTaskMeta);
    final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta(taskMetaMap);
    runtimeMeta
        .getNodeId2PipeRuntimeExceptionMap()
        .put(1, new PipeRuntimeCriticalException("stale node", 100L));
    runtimeMeta
        .getNodeId2PipeRuntimeExceptionMap()
        .put(2, new PipeRuntimeCriticalException("fresh node", 300L));

    runtimeMeta.clearExceptionMessagesBefore(200L);

    Assert.assertFalse(staleTaskMeta.hasExceptionMessages());
    Assert.assertTrue(freshTaskMeta.hasExceptionMessages());
    Assert.assertFalse(runtimeMeta.getNodeId2PipeRuntimeExceptionMap().containsKey(1));
    Assert.assertTrue(runtimeMeta.getNodeId2PipeRuntimeExceptionMap().containsKey(2));
  }
}
