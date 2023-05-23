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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PipeHandleMetaChangeProcedureTest {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    List<PipeMeta> pipeMetaList = new ArrayList<>();
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "pipeName",
            123L,
            new HashMap() {
              {
                put("collector-key", "collector-value");
              }
            },
            new HashMap() {
              {
                put("processor-key-1", "processor-value-1");
                put("processor-key-2", "processor-value-2");
              }
            },
            new HashMap() {});
    PipeRuntimeMeta pipeRuntimeMeta =
        new PipeRuntimeMeta(
            new HashMap() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(789, 987));
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(456, 789));
              }
            });
    pipeMetaList.add(new PipeMeta(pipeStaticMeta, pipeRuntimeMeta));
    PipeHandleMetaChangeProcedure proc =
        new PipeHandleMetaChangeProcedure(pipeMetaList, pipeMetaList, true);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      PipeHandleMetaChangeProcedure proc2 =
          (PipeHandleMetaChangeProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }
}
