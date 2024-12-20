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
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipeMetaDeSerTest {

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
        .trackExceptionMessage(new PipeRuntimeConnectorCriticalException("test456"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    final PipeMeta pipeMeta = new PipeMeta(pipeStaticMeta, pipeRuntimeMeta);
    final ByteBuffer byteBuffer = pipeMeta.serialize();
    final PipeMeta pipeMeta1 = PipeMeta.deserialize4Coordinator(byteBuffer);
    Assert.assertEquals(pipeMeta, pipeMeta1);
  }
}
