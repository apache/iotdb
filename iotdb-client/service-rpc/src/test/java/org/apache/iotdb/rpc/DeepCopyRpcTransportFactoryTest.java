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

import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeepCopyRpcTransportFactoryTest {

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
