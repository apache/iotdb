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

package org.apache.iotdb.postgresql.session;

import org.apache.iotdb.postgresql.protocol.FrontendMessage;

import io.netty.channel.Channel;

/**
 * Handles the messages of a logged-in connection (simple and extended query protocol). Called on
 * the connection's own ordered executor, never on Netty's event loop, so it may block.
 */
public interface QueryProtocol {

  /** Handles one message of the ready state ('Q', 'P', 'B', 'D', 'E', 'S', 'C', 'H'). */
  void handle(FrontendMessage message, Channel channel);

  /** True while a query runs or a result is being sent; idle timeouts do not apply then. */
  boolean busy();

  /** Cancels the running query, if any (CancelRequest); may be called from any thread. */
  void cancel();

  /** Releases everything when the connection closes. */
  void close();
}
