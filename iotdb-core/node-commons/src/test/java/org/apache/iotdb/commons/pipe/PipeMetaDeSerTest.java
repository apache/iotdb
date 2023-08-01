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
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class PipeMetaDeSerTest {

  @Test
  public void test() throws IOException {
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "pipeName",
            123L,
            new HashMap() {
              {
                put("extractor-key", "extractor-value");
              }
            },
            new HashMap() {
              {
                put("processor-key-1", "processor-value-1");
                put("processor-key-2", "processor-value-2");
              }
            },
            new HashMap() {});
    ByteBuffer staticByteBuffer = pipeStaticMeta.serialize();
    PipeStaticMeta pipeStaticMeta1 = PipeStaticMeta.deserialize(staticByteBuffer);
    Assert.assertEquals(pipeStaticMeta, pipeStaticMeta1);

    PipeRuntimeMeta pipeRuntimeMeta =
        new PipeRuntimeMeta(
            new HashMap() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(new MinimumProgressIndex(), 987));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(new IoTProgressIndex(1, 2L), 789));
              }
            });
    ByteBuffer runtimeByteBuffer = pipeRuntimeMeta.serialize();
    PipeRuntimeMeta pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
    pipeRuntimeMeta.setIsStoppedByRuntimeException(false);
    pipeRuntimeMeta.setExceptionsClearTime(123456789L);
    pipeRuntimeMeta
        .getDataNodeId2PipeRuntimeExceptionMap()
        .put(123, new PipeRuntimeCriticalException("test"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
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
        .getConsensusGroupId2TaskMetaMap()
        .get(new TConsensusGroupId(TConsensusGroupType.DataRegion, 456))
        .trackExceptionMessage(new PipeRuntimeConnectorCriticalException("test456"));

    runtimeByteBuffer = pipeRuntimeMeta.serialize();
    pipeRuntimeMeta1 = PipeRuntimeMeta.deserialize(runtimeByteBuffer);
    Assert.assertEquals(pipeRuntimeMeta, pipeRuntimeMeta1);

    PipeMeta pipeMeta = new PipeMeta(pipeStaticMeta, pipeRuntimeMeta);
    ByteBuffer byteBuffer = pipeMeta.serialize();
    PipeMeta pipeMeta1 = PipeMeta.deserialize(byteBuffer);
    Assert.assertEquals(pipeMeta, pipeMeta1);
  }
}
