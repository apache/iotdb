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

package org.apache.iotdb.commons.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SchemaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta.PipeRuntimeMetaV1;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta.PipeRuntimeMetaV2;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta.PipeRuntimeMetaVersion;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class PipeMetaDeSerTest {

  @Test
  public void test() throws IOException {
    PipeStaticMeta pipeStaticMeta =
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
    ByteBuffer staticByteBuffer = pipeStaticMeta.serialize();
    PipeStaticMeta pipeStaticMeta1 = PipeStaticMeta.deserialize(staticByteBuffer);
    Assert.assertEquals(pipeStaticMeta, pipeStaticMeta1);

    HybridProgressIndex hybridProgressIndex =
        new HybridProgressIndex(new SimpleProgressIndex(1, 2));
    hybridProgressIndex.updateToMinimumIsAfterProgressIndex(new SimpleProgressIndex(2, 4));
    hybridProgressIndex.updateToMinimumIsAfterProgressIndex(new IoTProgressIndex(3, 6L));

    PipeRuntimeMeta pipeRuntimeMeta =
        new PipeRuntimeMeta(
            new HashMap<TConsensusGroupId, PipeTaskMeta>() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 987));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 234),
                    new PipeTaskMeta(new IoTProgressIndex(1, 2L), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 345),
                    new PipeTaskMeta(new SimpleProgressIndex(3, 4), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(hybridProgressIndex, 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 567),
                    new PipeTaskMeta(
                        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 9)), 123));
              }
            },
            new HashMap<TConsensusGroupId, PipeTaskMeta>() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 111),
                    new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 222));
                put(
                    new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 333),
                    new PipeTaskMeta(new SchemaProgressIndex(444), 555));
              }
            });
    ByteBuffer runtimeByteBuffer = pipeRuntimeMeta.serialize();
    PipeRuntimeMeta pipeRuntimeMeta1 =
        PipeRuntimeMetaVersion.deserializeRuntimeMeta(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(false);
    pipeRuntimeMeta.setExceptionsClearTime(123456789L);
    pipeRuntimeMeta
        .getDataNodeId2PipeRuntimeExceptionMap()
        .put(123, new PipeRuntimeCriticalException("test"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMetaVersion.deserializeRuntimeMeta(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.DROPPED);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(true);
    pipeRuntimeMeta.setExceptionsClearTime(0);
    pipeRuntimeMeta
        .getDataNodeId2PipeRuntimeExceptionMap()
        .put(123, new PipeRuntimeCriticalException("test123"));
    pipeRuntimeMeta
        .getDataNodeId2PipeRuntimeExceptionMap()
        .put(345, new PipeRuntimeCriticalException("test345"));
    pipeRuntimeMeta
        .getDataRegionId2TaskMetaMap()
        .get(new TConsensusGroupId(TConsensusGroupType.DataRegion, 456))
        .trackExceptionMessage(new PipeRuntimeConnectorCriticalException("test456"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMetaVersion.deserializeRuntimeMeta(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    PipeMeta pipeMeta = new PipeMeta(pipeStaticMeta, pipeRuntimeMeta);
    ByteBuffer byteBuffer = pipeMeta.serialize();
    PipeMeta pipeMeta1 = PipeMeta.deserialize(byteBuffer);
    Assert.assertEquals(pipeMeta, pipeMeta1);
  }

  @Test
  public void testRuntimeMetaV1ToCurrent() throws IOException {
    PipeRuntimeMetaV1 pipeRuntimeMetaV1 =
        new PipeRuntimeMetaV1(
            new HashMap<TConsensusGroupId, PipeTaskMeta>() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 987));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 234),
                    new PipeTaskMeta(new IoTProgressIndex(1, 2L), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 345),
                    new PipeTaskMeta(new SimpleProgressIndex(3, 4), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(new HybridProgressIndex(new SimpleProgressIndex(1, 2)), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 567),
                    new PipeTaskMeta(
                        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 9)), 123));
              }
            });

    // Do not set the exceptions here since we do not reserve the old exceptions
    pipeRuntimeMetaV1.getStatus().set(PipeStatus.RUNNING);

    // Test byteBuffer
    PipeRuntimeMeta pipeRuntimeMeta =
        PipeRuntimeMetaVersion.deserializeRuntimeMeta(pipeRuntimeMetaV1.serialize());

    Assert.assertEquals(pipeRuntimeMetaV1.getStatus().get(), pipeRuntimeMeta.getStatus().get());
    Assert.assertEquals(
        pipeRuntimeMetaV1.getConsensusGroupIdToTaskMetaMap(),
        pipeRuntimeMeta.getDataRegionId2TaskMetaMap());

    Assert.assertEquals(MinimumProgressIndex.INSTANCE, pipeRuntimeMeta.getConfigProgressIndex());
    Assert.assertEquals(
        new ConcurrentHashMap<>(), pipeRuntimeMeta.getDataNodeId2PipeRuntimeExceptionMap());
    Assert.assertEquals(new ConcurrentHashMap<>(), pipeRuntimeMeta.getSchemaRegionId2TaskMetaMap());
    Assert.assertFalse(pipeRuntimeMeta.getIsStoppedByRuntimeException());
    Assert.assertEquals(Long.MIN_VALUE, pipeRuntimeMeta.getExceptionsClearTime());

    // Test inputStream
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      pipeRuntimeMetaV1.serialize(outputStream);
      pipeRuntimeMeta =
          PipeRuntimeMetaVersion.deserializeRuntimeMeta(
              new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

      Assert.assertEquals(pipeRuntimeMetaV1.getStatus().get(), pipeRuntimeMeta.getStatus().get());
      Assert.assertEquals(
          pipeRuntimeMetaV1.getConsensusGroupIdToTaskMetaMap(),
          pipeRuntimeMeta.getDataRegionId2TaskMetaMap());

      Assert.assertEquals(MinimumProgressIndex.INSTANCE, pipeRuntimeMeta.getConfigProgressIndex());
      Assert.assertEquals(
          new ConcurrentHashMap<>(), pipeRuntimeMeta.getDataNodeId2PipeRuntimeExceptionMap());
      Assert.assertEquals(
          new ConcurrentHashMap<>(), pipeRuntimeMeta.getSchemaRegionId2TaskMetaMap());
      Assert.assertFalse(pipeRuntimeMeta.getIsStoppedByRuntimeException());
      Assert.assertEquals(Long.MIN_VALUE, pipeRuntimeMeta.getExceptionsClearTime());
    } catch (IOException ignore) {
      // Do not fail due to serialization failure
    }
  }

  @Test
  public void testRuntimeMetaV2ToCurrent() throws IOException {
    TConsensusGroupId exceptionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 567);

    PipeRuntimeMetaV2 pipeRuntimeMetaV2 =
        new PipeRuntimeMetaV2(
            new HashMap<TConsensusGroupId, PipeTaskMeta>() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 987));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 234),
                    new PipeTaskMeta(new IoTProgressIndex(1, 2L), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 345),
                    new PipeTaskMeta(new SimpleProgressIndex(3, 4), 789));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(new HybridProgressIndex(new SimpleProgressIndex(1, 2)), 789));
                put(
                    exceptionGroupId,
                    new PipeTaskMeta(
                        new RecoverProgressIndex(1, new SimpleProgressIndex(1, 9)), 123));
              }
            });

    PipeRuntimeException testException = new PipeRuntimeCriticalException("No available receivers");

    pipeRuntimeMetaV2.getStatus().set(PipeStatus.DROPPED);
    pipeRuntimeMetaV2.setIsStoppedByRuntimeException(true);
    pipeRuntimeMetaV2.setExceptionsClearTime(System.currentTimeMillis());
    pipeRuntimeMetaV2.getDataNodeId2PipeRuntimeExceptionMap().put(1, testException);
    pipeRuntimeMetaV2
        .getConsensusGroupId2TaskMetaMap()
        .get(exceptionGroupId)
        .trackExceptionMessage(testException);

    // Test byteBuffer
    PipeRuntimeMeta pipeRuntimeMeta =
        PipeRuntimeMetaVersion.deserializeRuntimeMeta(pipeRuntimeMetaV2.serialize());

    Assert.assertEquals(pipeRuntimeMetaV2.getStatus().get(), pipeRuntimeMeta.getStatus().get());
    Assert.assertEquals(
        pipeRuntimeMetaV2.getConsensusGroupId2TaskMetaMap(),
        pipeRuntimeMeta.getDataRegionId2TaskMetaMap());

    Assert.assertEquals(
        pipeRuntimeMetaV2.getExceptionsClearTime(), pipeRuntimeMeta.getExceptionsClearTime());
    Assert.assertEquals(
        pipeRuntimeMetaV2.getDataNodeId2PipeRuntimeExceptionMap(),
        pipeRuntimeMeta.getDataNodeId2PipeRuntimeExceptionMap());
    Assert.assertEquals(
        pipeRuntimeMetaV2.getIsStoppedByRuntimeException(),
        pipeRuntimeMeta.getIsStoppedByRuntimeException());

    Assert.assertEquals(MinimumProgressIndex.INSTANCE, pipeRuntimeMeta.getConfigProgressIndex());
    Assert.assertEquals(new ConcurrentHashMap<>(), pipeRuntimeMeta.getSchemaRegionId2TaskMetaMap());

    // Test inputStream
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      pipeRuntimeMetaV2.serialize(outputStream);
      pipeRuntimeMeta =
          PipeRuntimeMetaVersion.deserializeRuntimeMeta(
              new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

      Assert.assertEquals(pipeRuntimeMetaV2.getStatus().get(), pipeRuntimeMeta.getStatus().get());
      Assert.assertEquals(
          pipeRuntimeMetaV2.getConsensusGroupId2TaskMetaMap(),
          pipeRuntimeMeta.getDataRegionId2TaskMetaMap());

      Assert.assertEquals(
          pipeRuntimeMetaV2.getExceptionsClearTime(), pipeRuntimeMeta.getExceptionsClearTime());
      Assert.assertEquals(
          pipeRuntimeMetaV2.getDataNodeId2PipeRuntimeExceptionMap(),
          pipeRuntimeMeta.getDataNodeId2PipeRuntimeExceptionMap());
      Assert.assertEquals(
          pipeRuntimeMetaV2.getIsStoppedByRuntimeException(),
          pipeRuntimeMeta.getIsStoppedByRuntimeException());

      Assert.assertEquals(MinimumProgressIndex.INSTANCE, pipeRuntimeMeta.getConfigProgressIndex());
      Assert.assertEquals(
          new ConcurrentHashMap<>(), pipeRuntimeMeta.getSchemaRegionId2TaskMetaMap());
    } catch (IOException ignore) {
      // Do not fail due to serialization failure
    }
  }
}
