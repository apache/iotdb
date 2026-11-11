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
package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class QueryAttemptProtocolTest {
  @Test
  public void legacyRequestsRemainReadableWithNoAttemptMetadata() throws Exception {
    TSendFragmentInstanceReq original =
        new TSendFragmentInstanceReq(new TFragmentInstance(ByteBuffer.wrap(new byte[] {1, 2, 3})));
    TSendFragmentInstanceReq received = new TSendFragmentInstanceReq();
    new TDeserializer().deserialize(received, new TSerializer().serialize(original));
    assertEquals(original, received);
    assertFalse(received.isSetQueryAttemptId());
    assertFalse(received.isSetQueryDeadline());
    TCancelQueryReq cancel = new TCancelQueryReq("query", Collections.emptyList(), true);
    TCancelQueryReq receivedCancel = new TCancelQueryReq();
    new TDeserializer().deserialize(receivedCancel, new TSerializer().serialize(cancel));
    assertEquals(cancel, receivedCancel);
    assertFalse(receivedCancel.isSetQueryAttemptId());
    assertFalse(receivedCancel.isSetAdmissionDeadline());
  }

  @Test
  public void attemptMetadataRoundTripsAndLegacyReaderCanSkipIt() throws Exception {
    TFragmentInstance body = new TFragmentInstance(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    TSendFragmentInstanceReq request =
        new TSendFragmentInstanceReq(body)
            .setQueryAttemptId(2)
            .setQueryDeadline(123456L)
            .setAdmissionDeadline(123450L);
    byte[] serialized = new TSerializer().serialize(request);
    TSendFragmentInstanceReq received = new TSendFragmentInstanceReq();
    new TDeserializer().deserialize(received, serialized);
    assertEquals(request, received);
    // Simulate a receiver that only knows the original binary-body field.
    TBinaryProtocol protocol = new TBinaryProtocol(new TMemoryInputTransport(serialized));
    TFragmentInstance legacyBody = new TFragmentInstance();
    protocol.readStructBegin();
    for (TField field = protocol.readFieldBegin();
        field.type != TType.STOP;
        field = protocol.readFieldBegin()) {
      if (field.id == 1) {
        legacyBody.read(protocol);
      } else {
        TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
    assertEquals(body, legacyBody);
    TCancelQueryReq cancel =
        new TCancelQueryReq("query", Collections.emptyList(), true)
            .setQueryAttemptId(2)
            .setAdmissionDeadline(123450L);
    TCancelQueryReq receivedCancel = new TCancelQueryReq();
    new TDeserializer().deserialize(receivedCancel, new TSerializer().serialize(cancel));
    assertEquals(cancel, receivedCancel);
  }
}
