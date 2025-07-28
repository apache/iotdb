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

package org.apache.iotdb.isession.pool;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;

/**
 * This interface defines a pool for managing {@link ITableSession} instances. It provides methods
 * to acquire a session from the pool and to close the pool.
 *
 * <p>The implementation should handle the lifecycle of sessions, ensuring efficient reuse and
 * proper cleanup of resources.
 */
public interface ITableSessionPool extends AutoCloseable {

  /**
   * Acquires an {@link ITableSession} instance from the pool.
   *
   * @return an {@link ITableSession} instance for interacting with IoTDB.
   * @throws IoTDBConnectionException if there is an issue obtaining a session from the pool.
   */
  ITableSession getSession() throws IoTDBConnectionException;

  /**
   * Closes the session pool, releasing any held resources.
   *
   * <p>Once the pool is closed, no further sessions can be acquired.
   */
  void close();
}
