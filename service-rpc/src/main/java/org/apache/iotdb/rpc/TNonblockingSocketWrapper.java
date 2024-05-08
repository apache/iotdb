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

import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * In Thrift 0.14.1, TNonblockingSocket's constructor throws a never-happened exception. So, we
 * screen the exception https://issues.apache.org/jira/browse/THRIFT-5412
 */
public class TNonblockingSocketWrapper {

  public static TNonblockingSocket wrap(String host, int port) throws IOException {
    try {
      return new TNonblockingSocket(host, port);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }

  public static TNonblockingSocket wrap(String host, int port, int timeout) throws IOException {
    try {
      return new TNonblockingSocket(host, port, timeout);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }

  public static TNonblockingSocket wrap(SocketChannel socketChannel) throws IOException {
    try {
      return new TNonblockingSocket(socketChannel);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }
}
