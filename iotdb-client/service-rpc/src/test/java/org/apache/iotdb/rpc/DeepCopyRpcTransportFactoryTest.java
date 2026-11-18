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

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DeepCopyRpcTransportFactoryTest {

  // Two client factories use different thresholds in the same JVM. Both plain and Snappy frames
  // must round-trip, while only the configured client releases its large reusable buffers.
  @Test
  public void testIndependentReusableBufferThresholds() throws Exception {
    boolean previousUseSnappy = BaseRpcTransportFactory.USE_SNAPPY;
    try {
      for (boolean snappy : new boolean[] {false, true}) {
        BaseRpcTransportFactory.setUseSnappy(snappy);
        try (TElasticFramedTransport temporary =
                (TElasticFramedTransport)
                    DeepCopyRpcTransportFactory.getInstance(32, 4096, 256)
                        .getTransport(new TMemoryBuffer(0));
            TElasticFramedTransport reusable =
                (TElasticFramedTransport)
                    DeepCopyRpcTransportFactory.getInstance(32, 4096)
                        .getTransport(new TMemoryBuffer(0))) {
          byte[] payload = new byte[1024];
          new Random(0).nextBytes(payload);
          for (TElasticFramedTransport transport :
              new TElasticFramedTransport[] {temporary, reusable}) {
            transport.write(payload);
            transport.flush();
            byte[] actual = new byte[payload.length];
            transport.readAll(actual, 0, actual.length);
            assertArrayEquals(payload, actual);
          }
          assertEquals(32, temporary.writeBuffer.getBuffer().length);
          assertEquals(32, temporary.readBuffer.getBuffer().length);
          assertTrue(reusable.writeBuffer.getBuffer().length >= payload.length);
          assertTrue(reusable.readBuffer.getBuffer().length >= payload.length);
        }
      }
    } finally {
      BaseRpcTransportFactory.setUseSnappy(previousUseSnappy);
    }
  }

  @Test
  public void testGetInstanceReturnsIndependentClientFactories() {
    assertNotSame(
        DeepCopyRpcTransportFactory.getInstance(8, 16),
        DeepCopyRpcTransportFactory.getInstance(8, 16));
  }

  @Test
  public void testIndependentMaxFrameSizeConfigurations() throws TTransportException {
    TTransport smallFrameTransport = createTransport(16, 20);
    TTransportException exception =
        assertThrows(
            TTransportException.class, () -> smallFrameTransport.read(ByteBuffer.allocate(1)));
    assertEquals("Frame size (20) larger than protect max size (16)!", exception.getMessage());

    TTransport largeFrameTransport = createTransport(32, 20);
    assertEquals(1, largeFrameTransport.read(ByteBuffer.allocate(1)));
  }

  private static TTransport createTransport(int maxFrameSize, int frameSize)
      throws TTransportException {
    ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + frameSize);
    frame.putInt(frameSize);
    frame.put(new byte[frameSize]);
    return DeepCopyRpcTransportFactory.getInstance(8, maxFrameSize)
        .getTransport(new TMemoryInputTransport(frame.array()));
  }
}
