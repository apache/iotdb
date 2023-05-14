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

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.SocketException;

public class TimeoutChangeableTFastFramedTransport extends TElasticFramedTransport
    implements TimeoutChangeableTransport {

  private TSocket underlyingSocket;

  public TimeoutChangeableTFastFramedTransport(
      TSocket underlying, int thriftDefaultBufferSize, int thriftMaxFrameSize) {
    super(underlying, thriftDefaultBufferSize, thriftMaxFrameSize);
    this.underlyingSocket = underlying;
  }

  @Override
  public void setTimeout(int timeout) {
    underlyingSocket.setTimeout(timeout);
  }

  @Override
  public int getTimeOut() throws SocketException {
    return underlyingSocket.getSocket().getSoTimeout();
  }

  public static class Factory extends TTransportFactory {

    private final int thriftDefaultBufferSize;
    protected final int thriftMaxFrameSize;

    public Factory(int thriftDefaultBufferSize, int thriftMaxFrameSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      if (trans instanceof TSocket) {
        return new TimeoutChangeableTFastFramedTransport(
            (TSocket) trans, thriftDefaultBufferSize, thriftMaxFrameSize);
      } else {
        return new TElasticFramedTransport(trans, thriftDefaultBufferSize, thriftMaxFrameSize);
      }
    }
  }
}
