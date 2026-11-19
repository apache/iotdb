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

package org.apache.iotdb.isession;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public interface INodeSupplier extends Supplier<List<TEndPoint>> {

  void close();

  Optional<TEndPoint> getQueryEndPoint();

  /** Initial addresses to try after all discovered endpoints have failed. */
  default List<TEndPoint> getSeedNodes() {
    return Collections.emptyList();
  }

  /** Request a discovery refresh after reconnecting through a seed. */
  default void requestRefresh() {
    // Suppliers without automatic discovery have nothing to refresh.
  }

  /**
   * Returns whether this supplier is managed by the caller instead of the IoTDB client.
   *
   * <p>When this returns {@code true}, the session or pool must not call {@link #close()} during
   * its own close or reopen lifecycle. The caller remains responsible for closing or otherwise
   * invalidating the supplier.
   *
   * <p>When this returns {@code false}, the session or pool owns the supplier lifecycle and may
   * call {@link #close()} when the session or pool is closed or reopened. Implementations whose
   * {@link #close()} is a no-op do not need to override this method.
   *
   * <p>Callers sharing one supplier between multiple sessions or pools should implement this method
   * to return {@code true}. This flag is declarative and is not validated by the client.
   */
  default boolean isCallerManaged() {
    return false;
  }
}
