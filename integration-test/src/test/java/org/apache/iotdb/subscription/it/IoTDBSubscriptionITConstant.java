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

package org.apache.iotdb.subscription.it;

import org.apache.iotdb.session.Session;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import java.util.concurrent.TimeUnit;

public class IoTDBSubscriptionITConstant {

  public static final long SLEEP_NS = 1_000_000_000L;
  public static final long POLL_TIMEOUT_MS = 10_000L;
  public static final int MAX_RETRY_TIMES = 3;

  //////////////////////////// awaitility ////////////////////////////

  private static final long AWAITILITY_POLL_DELAY_SECOND = 1L;
  private static final long AWAITILITY_POLL_INTERVAL_SECOND = 1L;
  private static final long AWAITILITY_AT_MOST_SECOND = 600L;

  public static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(IoTDBSubscriptionITConstant.AWAITILITY_POLL_DELAY_SECOND, TimeUnit.SECONDS)
          .pollInterval(
              IoTDBSubscriptionITConstant.AWAITILITY_POLL_INTERVAL_SECOND, TimeUnit.SECONDS)
          .atMost(IoTDBSubscriptionITConstant.AWAITILITY_AT_MOST_SECOND, TimeUnit.SECONDS);

  @FunctionalInterface
  public interface WrappedVoidSupplier {
    void get() throws Throwable;
  }

  public static void AWAIT_WITH_FLUSH(final Session session, final WrappedVoidSupplier assertions) {
    AWAIT.untilAsserted(() -> {
      session.executeQueryStatement("flush");
      assertions.get();
    });
  }
}
