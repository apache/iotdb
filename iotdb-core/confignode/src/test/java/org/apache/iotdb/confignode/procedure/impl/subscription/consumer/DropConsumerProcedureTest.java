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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DropConsumerProcedureTest {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("k1", "v1");
    consumerAttributes.put("k2", "v2");

    DropConsumerProcedure proc =
        new DropConsumerProcedure(new TCloseConsumerReq("old_consumer1", "test_consumer_group"));

    ConsumerGroupMeta oldMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    oldMeta.addConsumer(new ConsumerMeta("old_consumer1", 1, consumerAttributes));
    ConsumerGroupMeta newMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));

    proc.setExistingConsumerGroupMeta(oldMeta);
    proc.setUpdatedConsumerGroupMeta(newMeta);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropConsumerProcedure proc2 =
          (DropConsumerProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(oldMeta, proc2.getExistingConsumerGroupMeta());
      assertEquals(newMeta, proc2.getUpdatedConsumerGroupMeta());
    } catch (Exception e) {
      fail();
    }
  }
}
