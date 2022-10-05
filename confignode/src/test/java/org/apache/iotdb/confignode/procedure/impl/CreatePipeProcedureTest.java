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
package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.confignode.procedure.impl.sync.CreatePipeProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TPipeInfo;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreatePipeProcedureTest {

  @Test
  public void serializeDeserializeTest() throws PipeException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("syncdelop", "false");
    TPipeInfo pipeInfo =
        new TPipeInfo()
            .setPipeName("PipeName")
            .setPipeSinkName("PipeSinkName")
            .setStartTime(999)
            .setAttributes(attributes);

    CreatePipeProcedure p1 = new CreatePipeProcedure(pipeInfo);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      CreatePipeProcedure p2 = (CreatePipeProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);
    } catch (Exception e) {
      fail();
    }
  }
}
