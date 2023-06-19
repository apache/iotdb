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

package org.apache.iotdb.db.metadata.query.reader;

import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public interface ISchemaReader<T extends ISchemaInfo> extends AutoCloseable {

  ListenableFuture<Boolean> NOT_BLOCKED_TRUE = immediateFuture(true);
  ListenableFuture<Boolean> NOT_BLOCKED_FALSE = immediateFuture(false);

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
   * Returns a future that will be completed when the schemaReader becomes
   * unblocked.
   * @return value is true if the schemaReader has more data, false if it has no more data.
   */
  ListenableFuture<Boolean> hasNextFuture();

  T next();
}
