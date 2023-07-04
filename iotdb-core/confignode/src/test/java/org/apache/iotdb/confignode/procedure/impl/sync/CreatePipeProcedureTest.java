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

package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.commons.sync.PipeStatus;
import org.apache.iotdb.commons.sync.TsFilePipeInfo;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreatePipeProcedureTest {
  @Test
  public void serializeDeserializeTest() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    TsFilePipeInfo pipeInfo =
        new TsFilePipeInfo("testPipe", "testSink", PipeStatus.STOP, 0, 0, false);
    CreatePipeProcedure proc = new CreatePipeProcedure(pipeInfo);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreatePipeProcedure proc2 =
          (CreatePipeProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
