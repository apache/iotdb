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

package org.apache.iotdb.confignode.procedure.impl.pipe.plugin;

import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreatePipePluginProcedureTest {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    PipePluginMeta pipePluginMeta =
        new PipePluginMeta("test", "test.class", false, "test.jar", "testMD5test");
    CreatePipePluginProcedure proc =
        new CreatePipePluginProcedure(pipePluginMeta, new byte[] {1, 2, 3}, false);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreatePipePluginProcedure proc2 =
          (CreatePipePluginProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(new Binary(proc.getJarFile()), new Binary(proc2.getJarFile()));
    } catch (Exception e) {
      fail();
    }
  }
}
