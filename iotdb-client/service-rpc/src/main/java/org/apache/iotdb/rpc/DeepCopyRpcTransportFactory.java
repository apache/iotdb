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

import org.apache.thrift.transport.TTransportFactory;

public class DeepCopyRpcTransportFactory extends BaseRpcTransportFactory {

  public static DeepCopyRpcTransportFactory INSTANCE;

  static {
    reInit();
  }

  private DeepCopyRpcTransportFactory(TTransportFactory inner) {
    super(inner);
  }

  public static void reInit() {
    INSTANCE = create(USE_SNAPPY, thriftDefaultBufferSize, thriftMaxFrameSize);
  }

  public static DeepCopyRpcTransportFactory getInstance(
      int thriftDefaultBufferSize, int thriftMaxFrameSize) {
    return create(USE_SNAPPY, thriftDefaultBufferSize, thriftMaxFrameSize);
  }

  /** Creates a client factory whose reusable-buffer threshold is independent of other clients. */
  public static DeepCopyRpcTransportFactory getInstance(
      int thriftDefaultBufferSize, int thriftMaxFrameSize, int maxReusableBufferSize) {
    return create(USE_SNAPPY, thriftDefaultBufferSize, thriftMaxFrameSize, maxReusableBufferSize);
  }

  private static DeepCopyRpcTransportFactory create(
      boolean useSnappy, int thriftDefaultBufferSize, int thriftMaxFrameSize) {
    return create(useSnappy, thriftDefaultBufferSize, thriftMaxFrameSize, Integer.MAX_VALUE);
  }

  private static DeepCopyRpcTransportFactory create(
      boolean useSnappy,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      int maxReusableBufferSize) {
    return useSnappy
        ? new DeepCopyRpcTransportFactory(
            new TimeoutChangeableTSnappyFramedTransport.Factory(
                thriftDefaultBufferSize, thriftMaxFrameSize, true, maxReusableBufferSize))
        : new DeepCopyRpcTransportFactory(
            new TimeoutChangeableTFastFramedTransport.Factory(
                thriftDefaultBufferSize, thriftMaxFrameSize, true, maxReusableBufferSize));
  }
}
