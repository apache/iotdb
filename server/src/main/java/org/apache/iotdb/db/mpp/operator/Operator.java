/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.operator;

import org.apache.iotdb.tsfile.read.common.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface Operator extends AutoCloseable {
  ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

  OperatorContext getOperatorContext();

  /**
   * Returns a future that will be completed when the operator becomes unblocked. If the operator is
   * not blocked, this method should return {@code NOT_BLOCKED}.
   */
  default ListenableFuture<Void> isBlocked() {
    return NOT_BLOCKED;
  }

  /** Gets next tsBlock from this operator. If no data is currently available, return null. */
  TsBlock next() throws IOException;

  /** @return true if the operator has more data, otherwise false */
  boolean hasNext() throws IOException;

  /** This method will always be called before releasing the Operator reference. */
  @Override
  default void close() throws Exception {}
}
