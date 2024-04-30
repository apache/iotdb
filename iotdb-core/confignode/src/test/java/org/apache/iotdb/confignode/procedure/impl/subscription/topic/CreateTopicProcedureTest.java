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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic;

import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateTopicProcedureTest {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("path", "root.db.**");

    CreateTopicProcedure proc =
        new CreateTopicProcedure(
            new TCreateTopicReq("test_topic").setTopicAttributes(topicAttributes));

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreateTopicProcedure proc2 =
          (CreateTopicProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }
}
