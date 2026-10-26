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

package org.apache.iotdb.commons.concurrent;

/**
 * Lightweight polling utility for production code. Provides a fluent API similar to Awaitility for
 * waiting until a condition becomes true.
 *
 * <pre>{@code
 * // Wait with timeout
 * Await.await()
 *     .atMost(5, TimeUnit.SECONDS)
 *     .pollInterval(100, TimeUnit.MILLISECONDS)
 *     .until(() -> isReady());
 *
 * // Wait forever (use with caution)
 * Await.await()
 *     .forever()
 *     .pollInterval(1, TimeUnit.SECONDS)
 *     .until(() -> isReady());
 *
 * // Ignore exceptions during polling
 * Await.await()
 *     .atMost(30, TimeUnit.SECONDS)
 *     .ignoreExceptions()
 *     .until(() -> tryConnect());
 * }</pre>
 */
public final class Await {

  private Await() {}

  public static ConditionAwaiter await() {
    return new ConditionAwaiter();
  }
}
