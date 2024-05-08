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
package org.apache.iotdb.external.api.thrift;

import org.apache.thrift.server.ServerContext;

public interface JudgableServerContext extends ServerContext {

  /**
   * this method will be called when a client connects to the IoTDB server.
   *
   * @return false if we do not allow this connection
   */
  boolean whenConnect();

  /** @return false if we do not allow this connection */
  boolean whenDisconnect();

  @Override
  default <T> T unwrap(Class<T> iface) {
    return null;
  }

  @Override
  default boolean isWrapperFor(Class<?> iface) {
    return false;
  };
}
