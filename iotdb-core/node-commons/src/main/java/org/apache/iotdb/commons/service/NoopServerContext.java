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

package org.apache.iotdb.commons.service;

import org.apache.thrift.server.ServerContext;

/**
 * Shared {@link ServerContext} implementation for Thrift handlers that do not need per-connection
 * state.
 *
 * <p>Thrift 0.23 server implementations expect {@code createContext} to return a non-null context,
 * even when the event handler only uses connection lifecycle callbacks. This no-op implementation
 * keeps those handlers explicit without adding a custom context object for each connection.
 */
public final class NoopServerContext implements ServerContext {

  /** Singleton instance reused by handlers that do not attach state to the connection. */
  public static final NoopServerContext INSTANCE = new NoopServerContext();

  private NoopServerContext() {}

  @Override
  public <T> T unwrap(Class<T> iface) {
    return iface.isInstance(this) ? iface.cast(this) : null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
