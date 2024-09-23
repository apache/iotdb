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
package org.apache.iotdb.confignode.consensus.response.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.iotdb.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipePluginTableRespTest {

  @Test
  public void testConvertToThriftResponse() throws IOException {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PipePluginMeta> pipePluginMetaList = new ArrayList<>();
    pipePluginMetaList.add(new PipePluginMeta("iotdb-extractor", IoTDBExtractor.class.getName()));
    pipePluginMetaList.add(
        new PipePluginMeta("do-nothing-processor", DoNothingProcessor.class.getName()));
    PipePluginTableResp pipePluginTableResp = new PipePluginTableResp(status, pipePluginMetaList);

    final List<ByteBuffer> pipePluginByteBuffers = new ArrayList<>();
    for (PipePluginMeta pipePluginMeta : pipePluginMetaList) {
      pipePluginByteBuffers.add(pipePluginMeta.serialize());
    }
    TGetPipePluginTableResp getPipePluginTableResp =
        new TGetPipePluginTableResp(status, pipePluginByteBuffers);

    Assert.assertEquals(pipePluginTableResp.convertToThriftResponse(), getPipePluginTableResp);
  }
}
