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

package org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader;

import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ISchemaInfo;

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/**
 * ISchemaReader is a non-blocking iterator.
 *
 * <ol>
 *   <li>The isBlock interface is used to determine if it is blocking. If isDone() is false, it is
 *       blocking.
 *   <li>The hasNext interface is responsible for determining whether the next result is available,
 *       and if the current iterator is still in the isBlock state, it will synchronously wait for
 *       the isBlock state to be lifted.
 *   <li>The next interface is responsible for consuming the next result, if the current iterator
 *       hasNext returns false, throw NoSuchElementException.
 * </ol>
 *
 * @param <T>
 */
public interface ISchemaReader<T extends ISchemaInfo> extends AutoCloseable {

  ListenableFuture<Boolean> NOT_BLOCKED_TRUE = immediateFuture(true);
  ListenableFuture<Boolean> NOT_BLOCKED_FALSE = immediateFuture(false);
  ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

  /**
   * Determines if the iteration is successful when it completes.
   *
   * @return False if an exception occurs during the iteration. Otherwise, return true.
   */
  boolean isSuccess();

  /**
   * Get throwable if there is exception occurs during the iteration.
   *
   * @return Throwable, null if no exception.
   */
  Throwable getFailure();

  /**
   * Returns a future that will be completed when the schemaReader becomes unblocked. It may be
   * called several times before next and will return the same value.
   *
   * @return isDone is false if is Blocked.
   */
  ListenableFuture<?> isBlocked();

  boolean hasNext();

  /**
   * The next interface is responsible for consuming the next result.
   *
   * @throws java.util.NoSuchElementException if the current iterator hasNext is false
   */
  T next();
}
