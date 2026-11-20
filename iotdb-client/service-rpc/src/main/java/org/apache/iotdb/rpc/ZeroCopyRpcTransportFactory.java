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

public class ZeroCopyRpcTransportFactory extends BaseRpcTransportFactory {

  public static ZeroCopyRpcTransportFactory INSTANCE;

  static {
    reInit();
  }

  private ZeroCopyRpcTransportFactory(TTransportFactory inner, TTransportFactory serverInner) {
    super(inner, serverInner);
  }

  public static void reInit() {
    int serverMaxReusableBufferSize =
        Math.max(thriftDefaultBufferSize, SERVER_MAX_REUSABLE_BUFFER_SIZE);
    INSTANCE =
        USE_SNAPPY
            ? new ZeroCopyRpcTransportFactory(
                new TimeoutChangeableTSnappyFramedTransport.Factory(
                    thriftDefaultBufferSize, thriftMaxFrameSize, false),
                new TimeoutChangeableTSnappyFramedTransport.Factory(
                    thriftDefaultBufferSize,
                    thriftMaxFrameSize,
                    false,
                    serverMaxReusableBufferSize))
            : new ZeroCopyRpcTransportFactory(
                new TimeoutChangeableTFastFramedTransport.Factory(
                    thriftDefaultBufferSize, thriftMaxFrameSize, false),
                new TimeoutChangeableTFastFramedTransport.Factory(
                    thriftDefaultBufferSize,
                    thriftMaxFrameSize,
                    false,
                    serverMaxReusableBufferSize));
  }
}
