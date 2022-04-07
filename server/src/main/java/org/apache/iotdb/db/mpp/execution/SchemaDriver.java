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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.mpp.buffer.ISinkHandle;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.iotdb.db.mpp.operator.Operator.NOT_BLOCKED;

@NotThreadSafe
public class SchemaDriver implements Driver {

  private static final Logger logger = LoggerFactory.getLogger(DataDriver.class);

  private final Operator root;
  private final ISinkHandle sinkHandle;
  private final SchemaDriverContext driverContext;

  private final AtomicReference<SettableFuture<Void>> driverBlockedFuture = new AtomicReference<>();

  public SchemaDriver(Operator root, ISinkHandle sinkHandle, SchemaDriverContext driverContext) {
    this.root = root;
    this.sinkHandle = sinkHandle;
    this.driverContext = driverContext;
    // initially the driverBlockedFuture is not blocked (it is completed)
    SettableFuture<Void> future = SettableFuture.create();
    future.set(null);
    driverBlockedFuture.set(future);
  }

  @Override
  public boolean isFinished() {
    try {
      boolean isFinished = driverBlockedFuture.get().isDone() && root != null && root.isFinished();
      if (isFinished) {
        driverContext.finish();
      }
      return isFinished;
    } catch (Throwable t) {
      logger.error(
          "Failed to query whether the schema driver {} is finished", driverContext.getId(), t);
      driverContext.failed(t);
      return true;
    }
  }

  @Override
  public ListenableFuture<Void> processFor(Duration duration) {
    // if the driver is blocked we don't need to continue
    SettableFuture<Void> blockedFuture = driverBlockedFuture.get();
    if (!blockedFuture.isDone()) {
      return blockedFuture;
    }

    long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

    long start = System.nanoTime();
    try {
      do {
        ListenableFuture<Void> future = processInternal();
        if (!future.isDone()) {
          return updateDriverBlockedFuture(future);
        }
      } while (System.nanoTime() - start < maxRuntime && !root.isFinished());
    } catch (Throwable t) {
      logger.error("Failed to execute fragment instance {}", driverContext.getId(), t);
      driverContext.failed(t);
      close();
      blockedFuture.setException(t);
      return blockedFuture;
    }
    return NOT_BLOCKED;
  }

  private ListenableFuture<Void> processInternal() throws IOException {
    ListenableFuture<Void> blocked = root.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    blocked = sinkHandle.isFull();
    if (!blocked.isDone()) {
      return blocked;
    }
    if (root.hasNext()) {
      TsBlock tsBlock = root.next();
      if (tsBlock != null && !tsBlock.isEmpty()) {
        sinkHandle.send(Collections.singletonList(tsBlock));
      }
    }
    return NOT_BLOCKED;
  }

  private ListenableFuture<Void> updateDriverBlockedFuture(
      ListenableFuture<Void> sourceBlockedFuture) {
    // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
    // or any of the operators gets a memory revocation request
    SettableFuture<Void> newDriverBlockedFuture = SettableFuture.create();
    driverBlockedFuture.set(newDriverBlockedFuture);
    sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

    // TODO Although we don't have memory management for operator now, we should consider it for
    // future
    // it's possible that memory revoking is requested for some operator
    // before we update driverBlockedFuture above and we don't want to miss that
    // notification, so we check to see whether that's the case before returning.

    return newDriverBlockedFuture;
  }

  @Override
  public FragmentInstanceId getInfo() {
    return driverContext.getId();
  }

  @Override
  public void close() {}
}
